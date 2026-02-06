# fastdata-indexer Architecture

## 1. Architecture Diagram

```
NEAR Blockchain
  │  (transactions calling __fastdata_* methods)
  ▼
┌─────────────────────────────────────────────────┐
│  NEAR Lake (S3)                                 │
│  fastnear-neardata-fetcher library              │
└───────────────────────┬─────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────┐
│  main-indexer                                   │
│  • Receives blocks via mpsc channel             │
│  • Scans receipt actions for __fastdata_* calls │
│  • Extracts suffix + raw args bytes             │
│  • Writes to `blobs` table (all suffixes)       │
│  • Checkpoints to `meta` table                  │
└──────────┬────────────────────────┬─────────────┘
           │                        │
     suffix="kv"             suffix="fastfs"
           ▼                        ▼
┌────────────────────┐  ┌─────────────────────────┐
│  kv-sub-indexer    │  │  fastfs-sub-indexer      │
│  (suffix-fetcher)  │  │  (suffix-fetcher)        │
│  • Polls blobs     │  │  • Polls blobs           │
│  • JSON deser      │  │  • Borsh deser           │
│  • Explodes into   │  │  • Handles Simple +      │
│    (key,value)     │  │    Partial uploads        │
│    pairs           │  │                           │
│  ▼                 │  │  ▼                        │
│  s_kv (history)    │  │  s_fastfs_v2             │
│  s_kv_last (latest)│  └─────────────────────────┘
│  mv_kv_key (MV)    │
│  mv_kv_cur_key (MV)│
│  mv_kv_by_contract │
└────────────────────┘

Keyspace: fastdata_{CHAIN_ID}   (e.g. fastdata_mainnet)
Replication: NetworkTopologyStrategy dc1=3, tablets enabled
```

## 2. File Map

| File | Purpose |
|------|---------|
| `Cargo.toml` | Workspace root: members = main-indexer, scylladb, suffix-fetcher, kv-sub-indexer, fastfs-sub-indexer |
| `main-indexer/src/main.rs` | Entry point. Connects to NEAR Lake + ScyllaDB, filters `__fastdata_*` calls, writes raw blobs |
| `scylladb/src/lib.rs` | Shared DB layer: session creation, TLS/mTLS, `blobs`+`meta` table creation, prepared statements |
| `scylladb/src/types.rs` | `FastData` struct (domain) + `FastDataRow` (DB serialization) + `UNIVERSAL_SUFFIX = "*"` |
| `scylladb/src/utils.rs` | `compute_order_id()` — deterministic ordering from shard/receipt/action indices |
| `suffix-fetcher/src/lib.rs` | `SuffixFetcher` — polls `blobs` table by suffix, sends `FastData`/`EndOfRange` events to sub-indexers |
| `kv-sub-indexer/src/main.rs` | KV processing loop: receives FastData, JSON-deserializes, validates, accumulates rows, batch-writes |
| `kv-sub-indexer/src/scylla_types.rs` | `FastDataKv`/`FastDataKvRow` structs, `s_kv`+`s_kv_last` table creation, `add_kv_rows()` batch writer |
| `fastfs-sub-indexer/src/main.rs` | FastFS processing loop: receives FastData, Borsh-deserializes, validates paths/sizes, writes |
| `fastfs-sub-indexer/src/scylla_types.rs` | `FastfsFastData`/`FastfsFastDataRow`, `s_fastfs_v2` table creation |
| `fastfs-sub-indexer/src/fastfs.rs` | `SimpleFastfs`/`PartialFastfs` Borsh enums, path validation, size limits |

## 3. Write Map — Every INSERT Statement

| # | Table | CQL Location | Batching | Consistency |
|---|-------|-------------|----------|-------------|
| 1 | `blobs` | `scylladb/src/lib.rs:120` | Individual, 100 concurrent via `buffer_unordered` | LocalQuorum |
| 2 | `meta` | `scylladb/src/lib.rs:131` | Individual | LocalQuorum |
| 3 | `s_kv` | `kv-sub-indexer/src/scylla_types.rs:172` | Logged batch, chunks of 100 | LocalQuorum |
| 4 | `s_kv_last` | `kv-sub-indexer/src/scylla_types.rs:183` | Logged batch, chunks of 100 | LocalQuorum |
| 5 | `s_fastfs_v2` | `fastfs-sub-indexer/src/scylla_types.rs:139` | Individual | LocalQuorum |

The `add_kv_rows()` function at `kv-sub-indexer/src/scylla_types.rs:191-246` orchestrates writes #3, #4, and a checkpoint (#2) in a single call. Deduplication for `s_kv_last` happens at lines 203-209 via `HashMap` keyed on `(predecessor_id, current_account_id, key)`.

Retry pattern everywhere: exponential backoff `[1s, 2s, 4s]` (sub-indexers) or `[0s, 1s, 2s, 4s]` (main-indexer). Process exits on final failure to prevent data loss.

## 4. Schema Catalog — Every CREATE TABLE/VIEW

| # | Object | Type | Location | Partition Key | Clustering Keys |
|---|--------|------|----------|---------------|-----------------|
| 1 | `blobs` | TABLE | `scylladb/src/lib.rs:176-190` | `(suffix)` | `block_height, shard_id, receipt_index, action_index, receipt_id` |
| 2 | `idx_tx_hash` | INDEX | `scylladb/src/lib.rs:191` | — | on `blobs(tx_hash)` |
| 3 | `idx_receipt_id` | INDEX | `scylladb/src/lib.rs:192` | — | on `blobs(receipt_id)` |
| 4 | `meta` | TABLE | `scylladb/src/lib.rs:193-196` | `(suffix)` | — |
| 5 | `s_kv` | TABLE | `kv-sub-indexer/src/scylla_types.rs:107-123` | `(predecessor_id)` | `current_account_id, key, block_height, order_id` |
| 6 | `s_kv_last` | TABLE | `kv-sub-indexer/src/scylla_types.rs:124-140` | `(predecessor_id)` | `current_account_id, key` |
| 7 | `mv_kv_key` | MAT. VIEW | `kv-sub-indexer/src/scylla_types.rs:141-145` | `(key)` | `block_height, order_id, predecessor_id, current_account_id` |
| 8 | `mv_kv_cur_key` | MAT. VIEW | `kv-sub-indexer/src/scylla_types.rs:146-150` | `(current_account_id)` | `key, block_height, order_id, predecessor_id` |
| 9 | `mv_kv_by_contract` | MAT. VIEW | `kv-sub-indexer/src/scylla_types.rs:151-157` | `(current_account_id)` | `predecessor_id, key` |
| 10 | `s_fastfs_v2` | TABLE | `fastfs-sub-indexer/src/scylla_types.rs:105-123` | `(predecessor_id)` | `current_account_id, relative_path, offset` |
| 11 | `idx_s_fastfs_v2_tx_hash` | INDEX | `fastfs-sub-indexer/src/scylla_types.rs:124` | — | on `s_fastfs_v2(tx_hash)` |
| 12 | `idx_s_fastfs_v2_receipt_id` | INDEX | `fastfs-sub-indexer/src/scylla_types.rs:125` | — | on `s_fastfs_v2(receipt_id)` |

## 5. Where to Add a New `kv_accounts` Table INSERT

The function that writes to `s_kv_last` is **`add_kv_rows()`** at:

```
kv-sub-indexer/src/scylla_types.rs:191-246
```

This function already:
1. Converts `FastDataKv` → `FastDataKvRow` (lines 198-201)
2. Deduplicates for `s_kv_last` (lines 203-209)
3. Batch-inserts into `s_kv` (lines 215-223)
4. Batch-inserts into `s_kv_last` (lines 226-234)
5. Writes checkpoint to `meta` (lines 237-243)

A new `kv_accounts` INSERT would go in this same function, between step 4 and step 5 (after line 234, before line 236). You would:

1. Add the CREATE TABLE to the `create_tables()` function at line 105 (add to the `queries` array)
2. Add a `prepare_kv_accounts_insert_query()` function (pattern: lines 167-176)
3. Add a new batch-insert loop in `add_kv_rows()` after the `s_kv_last` loop
4. Pass the new prepared statement through from `main.rs` (pattern: lines 42-48)

## 6. Environment Variables

| Variable | Required | Default | Used In |
|----------|----------|---------|---------|
| `CHAIN_ID` | Yes | — | main.rs:25, kv/main.rs:27, fastfs/main.rs:30 |
| `SCYLLA_URL` | Yes | — | scylladb/src/lib.rs:73 |
| `SCYLLA_USERNAME` | Yes | — | scylladb/src/lib.rs:74 |
| `SCYLLA_PASSWORD` | Yes | — | scylladb/src/lib.rs:75 |
| `SCYLLA_SSL_CA` | No | (unencrypted) | scylladb/src/lib.rs:37, 77 |
| `SCYLLA_SSL_CERT` | No | — | scylladb/src/lib.rs:38 (mTLS client cert) |
| `SCYLLA_SSL_KEY` | No | — | scylladb/src/lib.rs:39 (mTLS client key) |
| `START_BLOCK_HEIGHT` | No | latest block | main.rs:68-71, 80-83 |
| `NUM_THREADS` | No | 8 | main.rs:51-54 |
| `BLOCK_UPDATE_INTERVAL_MS` | No | 5000 | main.rs:104-109 |
| `FASTNEAR_AUTH_BEARER_TOKEN` | No | — | main.rs:86 |
| `AWS_ACCESS_KEY_ID` | Yes* | — | used by fastnear-neardata-fetcher |
| `AWS_SECRET_ACCESS_KEY` | Yes* | — | used by fastnear-neardata-fetcher |
| `AWS_DEFAULT_REGION` | No | — | used by fastnear-neardata-fetcher |

\* Required by the external `fastnear-neardata-fetcher` crate, not referenced directly in this repo.

## 7. Key Design Patterns

- **Hub-and-spoke**: main-indexer writes raw blobs; sub-indexers poll by suffix and process independently
- **Suffix-based routing**: The `__fastdata_` prefix is stripped to get the suffix (e.g., `"kv"`, `"fastfs"`), which becomes the partition key in the `blobs` table
- **Idempotent inserts**: Primary key design ensures duplicate processing is safe (CQL INSERT = upsert)
- **Checkpoint via `meta` table**: `UNIVERSAL_SUFFIX = "*"` for main indexer, `INDEXER_ID` (e.g., `"kv-1"`) for sub-indexers
- **order_id formula**: `(shard_id * 100000 + receipt_index) * 1000 + action_index` — deterministic within a block
- **KV data format**: JSON object → exploded into one row per key, value re-serialized to string
- **FastFS data format**: Borsh-encoded enum with `SimpleFastfs` (whole file) and `PartialFastfs` (chunked upload)
