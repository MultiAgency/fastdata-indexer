# fastdata-indexer Architecture

## 1. Architecture Diagram

```
NEAR Blockchain
  │  (transactions calling __fastdata_* methods)
  ▼
┌─────────────────────────────────────────────────┐
│  fastnear neardata API (HTTP)                   │
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
│  kv_accounts       │
│  mv_kv_key (MV)    │
│  mv_kv_cur_key (MV)│
└────────────────────┘

Keyspace: fastdata_{CHAIN_ID}   (e.g. fastdata_mainnet)
Replication: NetworkTopologyStrategy dc1=3, tablets enabled
```

## 2. File Map

| File | Purpose |
|------|---------|
| `Cargo.toml` | Workspace root: members = main-indexer, scylladb, suffix-fetcher, fastfs-sub-indexer, kv-sub-indexer |
| `main-indexer/src/main.rs` | Entry point. Connects to fastnear neardata API + ScyllaDB, filters `__fastdata_*` calls, writes raw blobs |
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
| 3 | `s_kv` | `kv-sub-indexer/src/scylla_types.rs:171` | Unlogged batch, chunks of 100 | LocalQuorum |
| 4 | `s_kv_last` | `kv-sub-indexer/src/scylla_types.rs:182` | Individual with `USING TIMESTAMP` | LocalQuorum |
| 5 | `kv_accounts` | `kv-sub-indexer/src/scylla_types.rs:193` | Unlogged batch, chunks of 100 | LocalQuorum |
| 6 | `s_fastfs_v2` | `fastfs-sub-indexer/src/scylla_types.rs:142` | Individual | LocalQuorum |

The `add_kv_rows()` function at `kv-sub-indexer/src/scylla_types.rs:206-296` orchestrates writes #3, #4, #5, and an optional checkpoint (#2) in a single call. Deduplication for `s_kv_last` happens at lines 220-233 via explicit `(block_height, order_id)` comparison, keeping the highest value. Write #4 uses `USING TIMESTAMP` with `block_height * 1B + order_id` to ensure deterministic last-write-wins ordering based on blockchain order rather than wall-clock time.

Retry pattern everywhere: exponential backoff `[1s, 2s, 4s]` (sub-indexers) or `[0s, 1s, 2s, 4s]` (main-indexer). Graceful shutdown via `is_running` flag on final failure to prevent data loss.

## 4. Schema Catalog — Every CREATE TABLE/VIEW

| # | Object | Type | Location | Partition Key | Clustering Keys |
|---|--------|------|----------|---------------|-----------------|
| 1 | `blobs` | TABLE | `scylladb/src/lib.rs:156-171` | `(suffix, block_height_bucket)` | `block_height, shard_id, receipt_index, action_index, receipt_id` |
| 2 | `meta` | TABLE | `scylladb/src/lib.rs:174-177` | `(suffix)` | — |
| 3 | `s_kv` | TABLE | `kv-sub-indexer/src/scylla_types.rs:113-129` | `(predecessor_id)` | `current_account_id, key, block_height, order_id` |
| 4 | `s_kv_last` | TABLE | `kv-sub-indexer/src/scylla_types.rs:130-146` | `(predecessor_id)` | `current_account_id, key` |
| 5 | `mv_kv_key` | MAT. VIEW | `kv-sub-indexer/src/scylla_types.rs:147-151` | `(key)` | `block_height, order_id, predecessor_id, current_account_id` |
| 6 | `mv_kv_cur_key` | MAT. VIEW | `kv-sub-indexer/src/scylla_types.rs:152-156` | `(current_account_id)` | `key, block_height, order_id, predecessor_id` |
| 7 | `kv_accounts` | TABLE | `kv-sub-indexer/src/scylla_types.rs:157-162` | `(current_account_id)` | `key, predecessor_id` |
| 8 | `s_fastfs_v2` | TABLE | `fastfs-sub-indexer/src/scylla_types.rs:113-131` | `(predecessor_id)` | `current_account_id, relative_path, nonce, offset` |

## 5. Environment Variables

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

## 6. Key Design Patterns

- **Hub-and-spoke**: main-indexer writes raw blobs; sub-indexers poll by suffix and process independently
- **Suffix-based routing**: The `__fastdata_` prefix is stripped to get the suffix (e.g., `"kv"`, `"fastfs"`), which becomes part of the partition key in the `blobs` table
- **Block height bucketing**: `blobs` PK is `(suffix, block_height_bucket)` with bucket size 10,000 blocks to prevent hot partitions
- **Idempotent inserts**: Primary key design ensures duplicate processing is safe (CQL INSERT = upsert)
- **Checkpoint via `meta` table**: `UNIVERSAL_SUFFIX = "*"` for main indexer, `INDEXER_ID` (e.g., `"kv-1"`) for sub-indexers
- **order_id formula**: `(shard_id * 100000 + receipt_index) * 1000 + action_index` — deterministic within a block, validated for overflow
- **Deterministic LWW**: `s_kv_last` writes use `USING TIMESTAMP` with `block_height * 1B + order_id` so blockchain ordering always wins over wall-clock time on crash/replay
- **Graceful shutdown**: All binaries use an `is_running` AtomicBool flag instead of `process::exit()`, allowing Tokio to drain connections
- **Block gap detection**: main-indexer tracks expected block height and halts on gaps to prevent silent data loss
- **KV data format**: JSON object → exploded into one row per key, value re-serialized to string
- **FastFS data format**: Borsh-encoded enum with `SimpleFastfs` (whole file) and `PartialFastfs` (chunked upload)
