mod scylla_types;

use crate::scylla_types::{add_kv_rows, FastDataKv, INDEXER_ID, SUFFIX};
use dotenv::dotenv;
use fastnear_primitives::near_indexer_primitives::types::BlockHeight;
use fastnear_primitives::types::ChainId;
use scylla::statement::prepared::PreparedStatement;
use scylladb::ScyllaDb;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use suffix_fetcher::{SuffixFetcher, SuffixFetcherConfig, SuffixFetcherUpdate};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "kv-sub-indexer";
const MAX_NUM_KEYS: usize = 256;
const MAX_KEY_LENGTH: usize = 1024;
const MAX_VALUE_LENGTH: usize = 1_048_576;
const MAX_TOTAL_BYTES: usize = 256_000_000;
const MAX_KV_DATA_SIZE: usize = 4_194_304; // 4MB — defense-in-depth (main-indexer also caps at 4MB)
const ENTRY_OVERHEAD: usize = 320; // Approximate FastDataKv struct + String metadata per entry

fn parse_kv_entries(fastdata: &scylladb::FastData) -> Vec<FastDataKv> {
    if fastdata.data.len() > MAX_KV_DATA_SIZE {
        tracing::warn!(
            target: PROJECT_ID,
            "Skipping oversized KV data ({} bytes) for receipt {} action {}",
            fastdata.data.len(), fastdata.receipt_id, fastdata.action_index
        );
        return vec![];
    }
    let json_value = match serde_json::from_slice::<serde_json::Value>(&fastdata.data) {
        Ok(v) => v,
        Err(_) => {
            tracing::debug!(target: PROJECT_ID, "Received invalid Key-Value Fastdata");
            return vec![];
        }
    };
    let json_object = match json_value.as_object() {
        Some(o) => o,
        None => {
            tracing::debug!(target: PROJECT_ID, "Received invalid Key-Value Fastdata");
            return vec![];
        }
    };
    if json_object.len() > MAX_NUM_KEYS {
        tracing::warn!(
            target: PROJECT_ID,
            "Dropping Key-Value Fastdata with {} keys (max {}) for receipt {} action {}",
            json_object.len(), MAX_NUM_KEYS, fastdata.receipt_id, fastdata.action_index
        );
        return vec![];
    }

    let order_id = match scylladb::compute_order_id(fastdata) {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(target: PROJECT_ID, "Skipping KV entry (data permanently lost): {}", e);
            return vec![];
        }
    };
    let mut entries = Vec::new();
    for (key, value) in json_object {
        if key.len() > MAX_KEY_LENGTH {
            tracing::debug!(target: PROJECT_ID, "Received Key-Value Fastdata with invalid key length: {}", key.len());
            continue;
        }
        if key.is_empty() || key.chars().any(|c| c.is_control()) {
            tracing::debug!(target: PROJECT_ID, "Skipping KV key with invalid characters");
            continue;
        }
        let serialized_value = match serde_json::to_string(value) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    target: PROJECT_ID,
                    "Failed to serialize JSON value for key {}: {:?}. Skipping entry.",
                    key, e
                );
                continue;
            }
        };
        if serialized_value.len() > MAX_VALUE_LENGTH {
            tracing::debug!(target: PROJECT_ID, "Received Key-Value Fastdata with value too large: {} bytes", serialized_value.len());
            continue;
        }
        entries.push(FastDataKv {
            receipt_id: fastdata.receipt_id,
            action_index: fastdata.action_index,
            tx_hash: fastdata.tx_hash,
            signer_id: fastdata.signer_id.clone(),
            predecessor_id: fastdata.predecessor_id.clone(),
            current_account_id: fastdata.current_account_id.clone(),
            block_height: fastdata.block_height,
            block_timestamp: fastdata.block_timestamp,
            shard_id: fastdata.shard_id,
            receipt_index: fastdata.receipt_index,
            order_id,
            key: key.clone(),
            value: serialized_value,
        });
    }
    entries
}

async fn flush_rows(
    scylladb: &ScyllaDb,
    kv_insert_query: &PreparedStatement,
    kv_last_insert_query: &PreparedStatement,
    kv_accounts_insert_query: &PreparedStatement,
    rows: &[FastDataKv],
    checkpoint: Option<BlockHeight>,
) -> anyhow::Result<()> {
    let delays = [1, 2, 4];
    let mut last_error = None;

    for (attempt, &delay_secs) in delays.iter().enumerate() {
        if attempt > 0 {
            tracing::warn!(
                target: PROJECT_ID,
                "Retrying DB write (attempt {}/{}) after {}s delay",
                attempt + 1, delays.len(), delay_secs
            );
            tokio::time::sleep(Duration::from_secs(delay_secs)).await;
        }

        match add_kv_rows(scylladb, kv_insert_query, kv_last_insert_query, kv_accounts_insert_query, rows, checkpoint).await
        {
            Ok(_) => return Ok(()),
            Err(e) => {
                tracing::error!(
                    target: PROJECT_ID,
                    "DB write failed (attempt {}): {:?}",
                    attempt + 1, e
                );
                last_error = Some(e);
            }
        }
    }

    Err(last_error.unwrap())
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter("kv-sub-indexer=info,scylladb=info,suffix-fetcher=info")
        .init();

    let chain_id: ChainId = env::var("CHAIN_ID")
        .expect("CHAIN_ID required")
        .try_into()
        .expect("Invalid chain id");

    let fetcher = SuffixFetcher::new(chain_id, None)
        .await
        .expect("Can't create suffix fetcher");

    let scylladb = fetcher.get_scylladb();

    scylla_types::create_tables(&scylladb)
        .await
        .expect("Error creating tables");

    let kv_insert_query = scylla_types::prepare_kv_insert_query(&scylladb)
        .await
        .expect("Error preparing kv insert query");

    let kv_last_insert_query = scylla_types::prepare_kv_last_insert_query(&scylladb)
        .await
        .expect("Error preparing kv insert query");

    let kv_accounts_insert_query = scylla_types::prepare_kv_accounts_insert_query(&scylladb)
        .await
        .expect("Error preparing kv_accounts insert query");

    let last_processed_block_height = scylladb
        .get_last_processed_block_height(INDEXER_ID)
        .await
        .expect("Error getting last processed block height");

    let start_block_height: BlockHeight = last_processed_block_height
        .map(|h| h + 1)
        .unwrap_or_else(|| {
            env::var("START_BLOCK_HEIGHT")
                .ok()
                .map(|start_block_height| start_block_height.parse().expect("Invalid block height"))
                .unwrap_or(0)
        });

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();

    ctrlc::set_handler(move || {
        ctrl_c_running.store(false, Ordering::SeqCst);
        tracing::info!(target: PROJECT_ID, "Received Ctrl+C, starting shutdown...");
    })
    .expect("Error setting Ctrl+C handler");

    tracing::info!(target: PROJECT_ID,
        "Starting {:?} {} fetcher from height {}",
        SUFFIX,
        chain_id,
        start_block_height,
    );

    let (sender, mut receiver) = mpsc::channel(100);
    tokio::spawn(fetcher.start(
        SuffixFetcherConfig {
            suffix: SUFFIX.to_string(),
            start_block_height: Some(start_block_height),
            sleep_duration: Duration::from_millis(500),
        },
        sender,
        is_running.clone(),
    ));

    let mut rows: Vec<FastDataKv> = vec![];
    let mut total_bytes: usize = 0;
    while let Some(update) = receiver.recv().await {
        match update {
            SuffixFetcherUpdate::FastData(fastdata) => {
                tracing::info!(target: PROJECT_ID, "Received fastdata: {} {} {}", fastdata.block_height, fastdata.receipt_id, fastdata.action_index);

                let new_entries = parse_kv_entries(&fastdata);
                total_bytes +=
                    new_entries.iter().map(|e| e.key.len() + e.value.len() + ENTRY_OVERHEAD).sum::<usize>();
                rows.extend(new_entries);

                if rows.len() >= 10_000 || total_bytes >= MAX_TOTAL_BYTES {
                    tracing::info!(target: PROJECT_ID, "Early flush at {} rows ({} bytes)", rows.len(), total_bytes);
                    let current_rows = std::mem::take(&mut rows);
                    total_bytes = 0;

                    if let Err(e) = flush_rows(
                        &scylladb, &kv_insert_query, &kv_last_insert_query, &kv_accounts_insert_query,
                        &current_rows, None,
                    ).await {
                        tracing::error!(target: PROJECT_ID,
                            "Failed to write data after retries. Shutting down to prevent data loss: {:?}", e
                        );
                        is_running.store(false, Ordering::SeqCst);
                        break;
                    }
                }
            }
            SuffixFetcherUpdate::EndOfRange(block_height) => {
                tracing::info!(target: PROJECT_ID, "Saving last processed block height {} with {} rows", block_height, rows.len());
                let current_rows = std::mem::take(&mut rows);
                total_bytes = 0;

                if let Err(e) = flush_rows(
                    &scylladb, &kv_insert_query, &kv_last_insert_query, &kv_accounts_insert_query,
                    &current_rows, Some(block_height),
                ).await {
                    tracing::error!(target: PROJECT_ID,
                        "Failed to write data after retries. Shutting down to prevent data loss: {:?}", e
                    );
                    is_running.store(false, Ordering::SeqCst);
                    break;
                }

                if !is_running.load(Ordering::SeqCst) {
                    tracing::info!(target: PROJECT_ID, "Shutting down...");
                    break;
                }
            }
        };
    }

    tracing::info!(target: PROJECT_ID, "Successfully shut down");
}
