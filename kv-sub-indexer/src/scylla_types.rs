use crate::*;
use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use scylla::serialize::row::SerializeRow;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::{DeserializeRow, SerializeRow};
use scylladb::{ScyllaDb, SCYLLADB};
use std::collections::HashMap;

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

impl TryFrom<FastDataKvRow> for FastDataKv {
    type Error = anyhow::Error;

    fn try_from(row: FastDataKvRow) -> anyhow::Result<Self> {
        Ok(Self {
            receipt_id: row.receipt_id.parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse receipt_id '{}': {:?}", row.receipt_id, e))?,
            action_index: row.action_index as u32,
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
            block_height: row.block_height as u64,
            block_timestamp: row.block_timestamp as u64,
            shard_id: row.shard_id as u32,
            receipt_index: row.receipt_index as u32,
            order_id: row.order_id as u64,

            key: row.key,
            value: row.value,
        })
    }
}

impl From<FastDataKv> for FastDataKvRow {
    fn from(data: FastDataKv) -> Self {
        Self {
            receipt_id: data.receipt_id.to_string(),
            action_index: data.action_index as i32,
            tx_hash: data.tx_hash.map(|h| h.to_string()),
            signer_id: data.signer_id.to_string(),
            predecessor_id: data.predecessor_id.to_string(),
            current_account_id: data.current_account_id.to_string(),
            block_height: data.block_height as i64,
            block_timestamp: data.block_timestamp as i64,
            shard_id: data.shard_id as i32,
            receipt_index: data.receipt_index as i32,
            order_id: data.order_id as i64,
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
        ", //        "CREATE INDEX IF NOT EXISTS idx_s_kv_tx_hash ON s_kv (tx_hash)",
           //        "CREATE INDEX IF NOT EXISTS idx_s_kv_receipt_id ON s_kv (receipt_id)",
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

const BATCH_CHUNK_SIZE: usize = 100;

pub(crate) async fn add_kv_rows(
    scylla_db: &ScyllaDb,
    kv_insert_query: &PreparedStatement,
    kv_last_insert_query: &PreparedStatement,
    rows: Vec<FastDataKv>,
    last_processed_block_height: BlockHeight,
) -> anyhow::Result<()> {
    let kv_rows = rows
        .into_iter()
        .map(FastDataKvRow::from)
        .collect::<Vec<_>>();
    // Deduplicate kv_rows by key
    let kv_last_rows = kv_rows
        .iter()
        .cloned()
        .map(|row| (row.key.clone(), row))
        .collect::<HashMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();

    let last_processed_block_height_row =
        (INDEXER_ID.to_string(), last_processed_block_height as i64);

    // Write kv_rows in chunks
    for chunk in kv_rows.chunks(BATCH_CHUNK_SIZE) {
        let mut batch = Batch::new(BatchType::Logged);
        let mut values: Vec<&dyn SerializeRow> = vec![];
        for kv in chunk {
            batch.append_statement(kv_insert_query.clone());
            values.push(kv);
        }
        scylla_db.scylla_session.batch(&batch, values).await?;
    }

    // Write kv_last_rows in chunks
    for chunk in kv_last_rows.chunks(BATCH_CHUNK_SIZE) {
        let mut batch = Batch::new(BatchType::Logged);
        let mut values: Vec<&dyn SerializeRow> = vec![];
        for kv_last in chunk {
            batch.append_statement(kv_last_insert_query.clone());
            values.push(kv_last);
        }
        scylla_db.scylla_session.batch(&batch, values).await?;
    }

    // Write checkpoint
    {
        let mut batch = Batch::new(BatchType::Logged);
        let mut values: Vec<&dyn SerializeRow> = vec![];
        batch.append_statement(scylla_db.insert_last_processed_block_height_query.clone());
        values.push(&last_processed_block_height_row);
        scylla_db.scylla_session.batch(&batch, values).await?;
    }

    Ok(())
}
