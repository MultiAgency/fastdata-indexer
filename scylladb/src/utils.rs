use crate::FastData;

pub fn compute_order_id(fd: &FastData) -> anyhow::Result<u64> {
    if fd.receipt_index >= 100_000 || fd.action_index >= 1_000 {
        anyhow::bail!(
            "order_id encoding overflow: receipt_index={}, action_index={} for receipt {} (max receipt_index=99999, max action_index=999)",
            fd.receipt_index, fd.action_index, fd.receipt_id
        );
    }
    Ok(((fd.shard_id as u64) * 100_000 + fd.receipt_index as u64) * 1_000 + fd.action_index as u64)
}
