use crate::*;
use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use scylla::serialize::row::SerializeRow;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::{DeserializeRow, SerializeRow};
use scylladb::{ScyllaDb, SCYLLADB};
use std::collections::{HashMap, HashSet};

pub(crate) const SUFFIX: &str = "kv";
pub(crate) const INDEXER_ID: &str = "kv-1";

#[derive(Debug, Clone)]
pub struct FastDataKv {
    pub receipt_id: CryptoHash,
    pub action_index: u32,
    pub tx_hash: Option<CryptoHash>,
    pub signer_id: AccountId,
    pub predecessor_id: AccountId,
    pub current_account_id: AccountId,
    pub block_height: BlockHeight,
    pub block_timestamp: u64,
    pub shard_id: u32,
    pub receipt_index: u32,
    pub order_id: u64,

    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub(crate) struct FastDataKvRow {
    pub receipt_id: String,
    pub action_index: i32,
    pub tx_hash: Option<String>,
    pub signer_id: String,
    pub predecessor_id: String,
    pub current_account_id: String,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub shard_id: i32,
    pub receipt_index: i32,
    pub order_id: i64,

    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, SerializeRow)]
pub(crate) struct KvEdgeRow {
    pub edge_type: String,
    pub target: String,
    pub source: String,
    pub current_account_id: String,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub order_id: i64,
    pub value: String,
}

impl TryFrom<FastDataKvRow> for FastDataKv {
    type Error = anyhow::Error;

    fn try_from(row: FastDataKvRow) -> anyhow::Result<Self> {
        Ok(Self {
            receipt_id: row.receipt_id.parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse receipt_id '{}': {:?}", row.receipt_id, e))?,
            action_index: u32::try_from(row.action_index)
                .map_err(|_| anyhow::anyhow!("Negative action_index in DB: {}", row.action_index))?,
            tx_hash: row.tx_hash
                .map(|h| h.parse()
                    .map_err(|e| anyhow::anyhow!("Failed to parse tx_hash '{}': {:?}", h, e)))
                .transpose()?,
            signer_id: row.signer_id.parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse signer_id '{}': {:?}", row.signer_id, e))?,
            predecessor_id: row.predecessor_id.parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse predecessor_id '{}': {:?}", row.predecessor_id, e))?,
            current_account_id: row.current_account_id.parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse current_account_id '{}': {:?}", row.current_account_id, e))?,
            block_height: u64::try_from(row.block_height)
                .map_err(|_| anyhow::anyhow!("Negative block_height in DB: {}", row.block_height))?,
            block_timestamp: u64::try_from(row.block_timestamp)
                .map_err(|_| anyhow::anyhow!("Negative block_timestamp in DB: {}", row.block_timestamp))?,
            shard_id: u32::try_from(row.shard_id)
                .map_err(|_| anyhow::anyhow!("Negative shard_id in DB: {}", row.shard_id))?,
            receipt_index: u32::try_from(row.receipt_index)
                .map_err(|_| anyhow::anyhow!("Negative receipt_index in DB: {}", row.receipt_index))?,
            order_id: u64::try_from(row.order_id)
                .map_err(|_| anyhow::anyhow!("Negative order_id in DB: {}", row.order_id))?,

            key: row.key,
            value: row.value,
        })
    }
}

impl From<FastDataKv> for FastDataKvRow {
    fn from(data: FastDataKv) -> Self {
        Self {
            receipt_id: data.receipt_id.to_string(),
            action_index: i32::try_from(data.action_index).expect("action_index exceeds i32"),
            tx_hash: data.tx_hash.map(|h| h.to_string()),
            signer_id: data.signer_id.to_string(),
            predecessor_id: data.predecessor_id.to_string(),
            current_account_id: data.current_account_id.to_string(),
            block_height: i64::try_from(data.block_height).expect("block_height exceeds i64"),
            block_timestamp: i64::try_from(data.block_timestamp).expect("block_timestamp exceeds i64"),
            shard_id: i32::try_from(data.shard_id).expect("shard_id exceeds i32"),
            receipt_index: i32::try_from(data.receipt_index).expect("receipt_index exceeds i32"),
            order_id: i64::try_from(data.order_id).expect("order_id exceeds i64"),
            key: data.key,
            value: data.value,
        }
    }
}

// __fastdata_kv({ "foo/alex.near": "bar", "foo/bob.near": "moo"})
// PRIMARY KEY ((predecessor_id), current_account_id, key)
// "key" >= "foo/" and "key" < "foo/" + '\xff'
// INDEX KEY ((current_account_id), key)

pub(crate) async fn create_tables(scylla_db: &ScyllaDb) -> anyhow::Result<()> {
    let queries = [
        "CREATE TABLE IF NOT EXISTS s_kv (
            receipt_id text,
            action_index int,
            tx_hash text,
            signer_id text,
            predecessor_id text,
            current_account_id text,
            block_height bigint,
            block_timestamp bigint,
            shard_id int,
            receipt_index int,
            order_id bigint,

            key text,
            value text,
            PRIMARY KEY ((predecessor_id), current_account_id, key, block_height, order_id)
        )",
        "CREATE TABLE IF NOT EXISTS s_kv_last (
            receipt_id text,
            action_index int,
            tx_hash text,
            signer_id text,
            predecessor_id text,
            current_account_id text,
            block_height bigint,
            block_timestamp bigint,
            shard_id int,
            receipt_index int,
            order_id bigint,

            key text,
            value text,
            PRIMARY KEY ((predecessor_id), current_account_id, key)
        )",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kv_key AS
            SELECT * FROM s_kv
            WHERE key IS NOT NULL AND block_height IS NOT NULL AND order_id IS NOT NULL AND predecessor_id IS NOT NULL AND current_account_id IS NOT NULL
            PRIMARY KEY((key), block_height, order_id, predecessor_id, current_account_id)
        ",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kv_cur_key AS
            SELECT * FROM s_kv
            WHERE current_account_id IS NOT NULL AND key IS NOT NULL AND block_height IS NOT NULL AND order_id IS NOT NULL AND predecessor_id IS NOT NULL
            PRIMARY KEY((current_account_id), key, block_height, order_id, predecessor_id)
        ",
        "CREATE TABLE IF NOT EXISTS kv_accounts (
            current_account_id text,
            key text,
            predecessor_id text,
            PRIMARY KEY ((current_account_id), key, predecessor_id)
        )",
        "CREATE TABLE IF NOT EXISTS kv_edges (
            edge_type text,
            target text,
            source text,
            current_account_id text,
            block_height bigint,
            block_timestamp bigint,
            order_id bigint,
            value text,
            PRIMARY KEY ((edge_type, target), source)
        )",
    ];
    for query in queries.iter() {
        tracing::debug!(target: SCYLLADB, "Creating table: {}", query);
        scylla_db.scylla_session.query_unpaged(*query, &[]).await?;
    }
    Ok(())
}

pub(crate) async fn prepare_kv_insert_query(
    scylla_db: &ScyllaDb,
) -> anyhow::Result<PreparedStatement> {
    ScyllaDb::prepare_query(
        &scylla_db.scylla_session,
    "INSERT INTO s_kv (receipt_id, action_index, tx_hash, signer_id, predecessor_id, current_account_id, block_height, block_timestamp, shard_id, receipt_index, order_id, key, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        scylla::frame::types::Consistency::LocalQuorum,
    )
    .await
}

pub(crate) async fn prepare_kv_last_insert_query(
    scylla_db: &ScyllaDb,
) -> anyhow::Result<PreparedStatement> {
    ScyllaDb::prepare_query(
        &scylla_db.scylla_session,
        "INSERT INTO s_kv_last (receipt_id, action_index, tx_hash, signer_id, predecessor_id, current_account_id, block_height, block_timestamp, shard_id, receipt_index, order_id, key, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        scylla::frame::types::Consistency::LocalQuorum,
    )
        .await
}

pub(crate) async fn prepare_kv_accounts_insert_query(
    scylla_db: &ScyllaDb,
) -> anyhow::Result<PreparedStatement> {
    ScyllaDb::prepare_query(
        &scylla_db.scylla_session,
        "INSERT INTO kv_accounts (current_account_id, key, predecessor_id) VALUES (?, ?, ?)",
        scylla::frame::types::Consistency::LocalQuorum,
    )
    .await
}

pub(crate) async fn prepare_kv_edges_insert_query(
    scylla_db: &ScyllaDb,
) -> anyhow::Result<PreparedStatement> {
    ScyllaDb::prepare_query(
        &scylla_db.scylla_session,
        "INSERT INTO kv_edges (edge_type, target, source, current_account_id, block_height, block_timestamp, order_id, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        scylla::frame::types::Consistency::LocalQuorum,
    )
    .await
}

/// Auto-detects a graph edge from a KV key.
/// Key format: `{source}/{...edge_type...}/{target}`
/// An edge is detected when:
/// - At least 3 segments separated by `/`
/// - First segment (source) equals `predecessor_id`
/// - Last segment = target, middle segments joined by `/` = edge_type
pub(crate) fn extract_edge(key: &str, predecessor_id: &str) -> Option<(String, String, String)> {
    let key = key.trim_start_matches('/');
    let segments: Vec<&str> = key.split('/').collect();
    if segments.len() < 2 {
        return None;
    }
    if predecessor_id.is_empty() {
        return None;
    }
    let target = segments[segments.len() - 1];
    if target.is_empty() {
        return None;
    }
    let edge_type = segments[..segments.len() - 1].join("/");
    if edge_type.is_empty() {
        return None;
    }
    Some((edge_type, predecessor_id.to_string(), target.to_string()))
}

const BATCH_CHUNK_SIZE: usize = 100;

pub(crate) async fn add_kv_rows(
    scylla_db: &ScyllaDb,
    kv_insert_query: &PreparedStatement,
    kv_last_insert_query: &PreparedStatement,
    kv_accounts_insert_query: &PreparedStatement,
    kv_edges_insert_query: &PreparedStatement,
    rows: &[FastDataKv],
    last_processed_block_height: Option<BlockHeight>,
) -> anyhow::Result<()> {
    let kv_rows: Vec<FastDataKvRow> = rows
        .iter()
        .cloned()
        .map(FastDataKvRow::from)
        .collect();
    // Deduplicate kv_rows by key, keeping the row with the highest order_id
    let mut kv_last_map: HashMap<(String, String, String), FastDataKvRow> = HashMap::new();
    for row in kv_rows.iter().cloned() {
        let dedup_key = (
            row.predecessor_id.clone(),
            row.current_account_id.clone(),
            row.key.clone(),
        );
        match kv_last_map.get(&dedup_key) {
            Some(existing) if (existing.block_height, existing.order_id) >= (row.block_height, row.order_id) => {}
            _ => {
                kv_last_map.insert(dedup_key, row);
            }
        }
    }
    let mut kv_last_rows: Vec<_> = kv_last_map.into_values().collect();
    kv_last_rows.sort_by(|a, b| (b.block_height, b.order_id).cmp(&(a.block_height, a.order_id)));

    // Write kv_rows in chunks
    for chunk in kv_rows.chunks(BATCH_CHUNK_SIZE) {
        let mut batch = Batch::new(BatchType::Unlogged);
        let mut values: Vec<&dyn SerializeRow> = vec![];
        for kv in chunk {
            batch.append_statement(kv_insert_query.clone());
            values.push(kv);
        }
        scylla_db.scylla_session.batch(&batch, values).await?;
    }

    // Write kv_last_rows with USING TIMESTAMP for deterministic last-write-wins ordering.
    // This prevents stale overwrites on crash/replay: blockchain ordering always wins
    // over wall-clock time, regardless of write order or duplicate writes.
    for kv_last in &kv_last_rows {
        // Encode (block_height, order_id) into a single i64 timestamp for deterministic LWW.
        // Max order_id ≈ shard_count * 100_000 * 1_000 ≈ 600M (6 shards). With 1B multiplier,
        // block_height can reach ~9.2B before i64 overflow (current NEAR height: ~150M).
        let ts = (kv_last.block_height) * 1_000_000_000 + kv_last.order_id;
        let mut stmt = kv_last_insert_query.clone();
        stmt.set_timestamp(Some(ts));
        scylla_db.scylla_session.execute_unpaged(&stmt, kv_last).await?;
    }

    // Write kv_accounts (unique tuples from kv_last_rows)
    let account_tuples: HashSet<(&str, &str, &str)> = kv_last_rows
        .iter()
        .map(|row| (
            row.current_account_id.as_str(),
            row.key.as_str(),
            row.predecessor_id.as_str(),
        ))
        .collect();
    let account_tuples_vec: Vec<_> = account_tuples
        .into_iter()
        .map(|(a, k, p)| (a.to_string(), k.to_string(), p.to_string()))
        .collect();
    for chunk in account_tuples_vec.chunks(BATCH_CHUNK_SIZE) {
        let mut batch = Batch::new(BatchType::Unlogged);
        let mut values: Vec<&dyn SerializeRow> = vec![];
        for tuple in chunk {
            batch.append_statement(kv_accounts_insert_query.clone());
            values.push(tuple);
        }
        scylla_db.scylla_session.batch(&batch, values).await?;
    }

    // Write kv_edges for auto-detected graph edges
    {
        let mut edge_rows: Vec<KvEdgeRow> = Vec::new();
        let mut edge_dedup: HashSet<(String, String, String)> = HashSet::new();

        for row in &kv_last_rows {
            if let Some((edge_type, source, target)) = extract_edge(&row.key, &row.predecessor_id) {
                let dedup_key = (edge_type.clone(), target.clone(), source.clone());
                if !edge_dedup.insert(dedup_key) {
                    continue;
                }
                edge_rows.push(KvEdgeRow {
                    edge_type,
                    target,
                    source,
                    current_account_id: row.current_account_id.clone(),
                    block_height: row.block_height,
                    block_timestamp: row.block_timestamp,
                    order_id: row.order_id,
                    value: row.value.clone(),
                });
            }
        }

        for edge in &edge_rows {
            let ts = edge.block_height * 1_000_000_000 + edge.order_id;
            let mut stmt = kv_edges_insert_query.clone();
            stmt.set_timestamp(Some(ts));
            scylla_db.scylla_session.execute_unpaged(&stmt, edge).await?;
        }
    }

    // Write checkpoint only when a block height is provided
    if let Some(height) = last_processed_block_height {
        let checkpoint_row = (INDEXER_ID.to_string(), i64::try_from(height).expect("checkpoint height exceeds i64"));
        scylla_db
            .scylla_session
            .execute_unpaged(
                &scylla_db.insert_last_processed_block_height_query,
                checkpoint_row,
            )
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_edge_follow() {
        assert_eq!(
            extract_edge("legion/follow/bob.near", "alice.near"),
            Some(("legion/follow".into(), "alice.near".into(), "bob.near".into()))
        );
    }

    #[test]
    fn test_extract_edge_graph_follow() {
        assert_eq!(
            extract_edge("graph/follow/bob.near", "alice.near"),
            Some(("graph/follow".into(), "alice.near".into(), "bob.near".into()))
        );
    }

    #[test]
    fn test_extract_edge_single_segment_type() {
        assert_eq!(
            extract_edge("follow/bob.near", "alice.near"),
            Some(("follow".into(), "alice.near".into(), "bob.near".into()))
        );
    }

    #[test]
    fn test_extract_edge_deep_type() {
        assert_eq!(
            extract_edge("a/b/c/bob.near", "alice.near"),
            Some(("a/b/c".into(), "alice.near".into(), "bob.near".into()))
        );
    }

    #[test]
    fn test_extract_edge_empty_predecessor() {
        assert_eq!(extract_edge("graph/follow/bob.near", ""), None);
    }

    #[test]
    fn test_extract_edge_too_few_segments() {
        assert_eq!(extract_edge("bob.near", "alice.near"), None);
    }

    #[test]
    fn test_extract_edge_non_account_target() {
        assert_eq!(
            extract_edge("like/post/12345", "alice.near"),
            Some(("like/post".into(), "alice.near".into(), "12345".into()))
        );
    }

    #[test]
    fn test_extract_edge_profile_name() {
        assert_eq!(
            extract_edge("profile/name", "alice.near"),
            Some(("profile".into(), "alice.near".into(), "name".into()))
        );
    }

    #[test]
    fn test_extract_edge_empty_target() {
        assert_eq!(extract_edge("legion/follow/", "alice.near"), None);
    }

    #[test]
    fn test_extract_edge_empty_source() {
        assert_eq!(extract_edge("graph/follow/bob.near", ""), None);
    }

    #[test]
    fn test_extract_edge_no_slash() {
        assert_eq!(extract_edge("justadata", "alice.near"), None);
    }

    #[test]
    fn test_extract_edge_custom_subscribe() {
        assert_eq!(
            extract_edge("custom/subscribe/channel.near", "alice.near"),
            Some(("custom/subscribe".into(), "alice.near".into(), "channel.near".into()))
        );
    }

    #[test]
    fn test_extract_edge_leading_slash() {
        assert_eq!(
            extract_edge("/graph/alice.near", "bob.near"),
            Some(("graph".into(), "bob.near".into(), "alice.near".into()))
        );
    }
}
