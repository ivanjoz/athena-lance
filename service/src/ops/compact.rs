//! `compact`: size- and count-bounded compaction (ADR-006), under the table lock.
//!
//! Adjacent small fragments are grouped and each group is rewritten as ONE file. A group
//! closes when adding the next fragment would exceed `max_file_bytes` OR `max_files`, so
//! both limits always hold. Fragments already at the size limit, or with an unknown size,
//! are never touched and act as boundaries (groups only join neighbours, which keeps the
//! table's sort order). Groups of one fragment are skipped: there is nothing to merge.
//! At most `max_tasks` groups are planned per run, so one run fits in the Lambda timeout;
//! `more = true` tells the caller to run it again.
//!
//! After compacting, versions older than `service.version_retention_hours` are cleaned up,
//! which is what actually deletes the replaced files.

use std::ops::Range;

use lance::Dataset;
use lance::dataset::optimize::{
    CompactionOptions, CompactionPlan, CompactionPlanner, TaskData, compact_files_with_planner,
};
use lance_table::format::Fragment;
use serde::Serialize;

use crate::app::App;
use crate::error::{Error, Result, code};
use crate::ledger::{self, BatchRecord};
use crate::protocol::Compact;

const MB: u64 = 1 << 20;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct CompactLimits {
    pub max_file_bytes: u64,
    pub max_files: usize,
    pub max_tasks: usize,
}

impl CompactLimits {
    /// Request values, 0 = the config default.
    pub fn resolve(app: &App, max_file_mb: u64, max_files: usize, max_tasks: usize) -> Result<CompactLimits> {
        let d = &app.cfg.compaction;
        let l = CompactLimits {
            max_file_bytes: if max_file_mb == 0 { d.max_file_mb } else { max_file_mb } * MB,
            max_files: if max_files == 0 { d.max_files } else { max_files },
            max_tasks: if max_tasks == 0 { d.max_tasks } else { max_tasks },
        };
        if l.max_files < 2 || l.max_file_bytes == 0 || l.max_tasks == 0 {
            return Err(Error::api(code::BAD_REQUEST, "compaction needs max_files >= 2, max_file_mb >= 1, max_tasks >= 1"));
        }
        Ok(l)
    }
}

/// Plans groups over fragment sizes given in table order. Returns the groups as index
/// ranges, and whether more groups exist beyond `max_tasks`.
pub fn plan_groups(sizes: &[Option<u64>], limits: &CompactLimits) -> (Vec<Range<usize>>, bool) {
    // Keeps a closed group if it merges at least two fragments; false once max_tasks
    // groups are planned and another one is still eligible.
    fn close(groups: &mut Vec<Range<usize>>, group: Range<usize>, max_tasks: usize) -> bool {
        if group.len() < 2 {
            return true;
        }
        if groups.len() == max_tasks {
            return false;
        }
        groups.push(group);
        true
    }

    let mut groups = Vec::new();
    let mut start = 0;
    let mut bytes = 0u64;
    for (i, size) in sizes.iter().enumerate() {
        let Some(s) = size.filter(|s| *s < limits.max_file_bytes) else {
            if !close(&mut groups, start..i, limits.max_tasks) {
                return (groups, true);
            }
            start = i + 1;
            bytes = 0;
            continue;
        };
        if i - start + 1 > limits.max_files || bytes + s > limits.max_file_bytes {
            if !close(&mut groups, start..i, limits.max_tasks) {
                return (groups, true);
            }
            start = i;
            bytes = 0;
        }
        bytes += s;
    }
    let more = !close(&mut groups, start..sizes.len(), limits.max_tasks);
    (groups, more)
}

/// Total size of a fragment's data files, if the manifest records all of them.
pub fn fragment_bytes(f: &Fragment) -> Option<u64> {
    f.files.iter().map(|d| d.file_size_bytes.get().map(|n| n.get())).sum()
}

struct SizeCountPlanner {
    limits: CompactLimits,
    options: CompactionOptions,
}

#[async_trait::async_trait]
impl CompactionPlanner for SizeCountPlanner {
    async fn plan(&self, dataset: &Dataset) -> lance::Result<CompactionPlan> {
        let fragments: Vec<Fragment> = dataset.get_fragments().iter().map(|f| f.metadata().clone()).collect();
        let sizes: Vec<Option<u64>> = fragments.iter().map(fragment_bytes).collect();
        let (groups, _) = plan_groups(&sizes, &self.limits);
        Ok(CompactionPlan {
            tasks: groups.into_iter().map(|r| TaskData { fragments: fragments[r].to_vec() }).collect(),
            read_version: dataset.version().version,
            options: self.options.clone(),
        })
    }
}

#[derive(Debug, Serialize)]
pub struct CompactResult {
    pub operation_id: String,
    pub limits: CompactLimits,
    pub version_before: u64,
    pub version: u64,
    pub fragments_removed: usize,
    pub fragments_added: usize,
    pub files_removed: usize,
    pub files_added: usize,
    /// More groups were eligible than `max_tasks`; call again.
    pub more: bool,
    pub cleanup: Cleanup,
    pub already_applied: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct Cleanup {
    pub old_versions_removed: u64,
    pub bytes_removed: u64,
}

pub async fn compact(app: &App, req: Compact) -> Result<CompactResult> {
    let limits = CompactLimits::resolve(app, req.max_file_mb, req.max_files, req.max_tasks)?;
    if !crate::ops::write::valid_batch_id(&req.operation_id) {
        return Err(Error::api(code::BAD_REQUEST, format!("invalid operation_id {:?}", req.operation_id)));
    }
    let mut ds = app.open(&req.table).await?;
    let lock = app.lock(&req.table, &req.operation_id, "compact").await?;
    let result = compact_locked(app, &req, &mut ds, limits, &lock).await;
    lock.release().await;
    result
}

async fn compact_locked(
    app: &App,
    req: &Compact,
    ds: &mut Dataset,
    limits: CompactLimits,
    lock: &crate::lock::TableLock,
) -> Result<CompactResult> {
    if let Some(prev) = app.ledger.get_batch(&req.table, &req.operation_id).await? {
        if prev.status == ledger::COMMITTED {
            return Ok(CompactResult {
                operation_id: req.operation_id.clone(),
                limits,
                version_before: prev.data_version,
                version: prev.version,
                fragments_removed: 0,
                fragments_added: 0,
                files_removed: 0,
                files_added: 0,
                more: false,
                cleanup: Cleanup::default(),
                already_applied: true,
            });
        }
    }
    let mut record = BatchRecord {
        batch_id: req.operation_id.clone(),
        op: "compact".into(),
        status: ledger::PENDING.into(),
        started_at: chrono::Utc::now().timestamp(),
        ..Default::default()
    };
    app.ledger.put_batch(&req.table, &record).await?;

    let run = async {
        let version_before = ds.version().version;
        let sizes: Vec<Option<u64>> = ds.get_fragments().iter().map(|f| fragment_bytes(f.metadata())).collect();
        let (_, more) = plan_groups(&sizes, &limits);
        let options = CompactionOptions {
            // Each planned group must become exactly one output file: the row target only
            // splits outputs, so set it above any group, and cap bytes as a hard limit.
            target_rows_per_fragment: usize::MAX / 2,
            max_bytes_per_file: Some(limits.max_file_bytes as usize),
            ..Default::default()
        };
        let planner = SizeCountPlanner { limits, options };
        lock.check()?;
        let metrics = compact_files_with_planner(ds, None, &planner).await?;
        let retention = chrono::Duration::hours(app.cfg.service.version_retention_hours as i64);
        let stats = ds.cleanup_old_versions(retention, None, None).await?;
        Ok::<_, Error>(CompactResult {
            operation_id: req.operation_id.clone(),
            limits,
            version_before,
            version: ds.version().version,
            fragments_removed: metrics.fragments_removed,
            fragments_added: metrics.fragments_added,
            files_removed: metrics.files_removed,
            files_added: metrics.files_added,
            more,
            cleanup: Cleanup { old_versions_removed: stats.old_versions, bytes_removed: stats.bytes_removed },
            already_applied: false,
        })
    };
    match run.await {
        Ok(r) => {
            record.status = ledger::COMMITTED.into();
            record.committed_at = chrono::Utc::now().timestamp();
            record.data_version = r.version_before;
            record.version = r.version;
            app.ledger.put_batch(&req.table, &record).await?;
            Ok(r)
        }
        Err(e) => {
            record.status = ledger::FAILED.into();
            record.error = Some((&e.to_api()).into());
            if let Err(le) = app.ledger.put_batch(&req.table, &record).await {
                tracing::error!(error = %le, "could not record the failed compaction");
            }
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(max_mb: u64, max_files: usize, max_tasks: usize) -> CompactLimits {
        CompactLimits { max_file_bytes: max_mb * MB, max_files, max_tasks }
    }

    fn mb(sizes: &[u64]) -> Vec<Option<u64>> {
        sizes.iter().map(|s| Some(s * MB)).collect()
    }

    #[test]
    fn count_limit_binds() {
        // 45 x 1 MB, limits 50 MB / 20 files -> 20 + 20 + 5.
        let (g, more) = plan_groups(&mb(&[1; 45]), &limits(50, 20, 10));
        assert_eq!(g, vec![0..20, 20..40, 40..45]);
        assert!(!more);
    }

    #[test]
    fn size_limit_binds() {
        // 10 MB files: 5 fit in 50 MB (count limit 20 never reached).
        let (g, _) = plan_groups(&mb(&[10; 12]), &limits(50, 20, 10));
        assert_eq!(g, vec![0..5, 5..10, 10..12]);
    }

    #[test]
    fn both_limits_hold_in_every_group() {
        let sizes = mb(&[3, 7, 1, 30, 12, 2, 2, 9, 40, 1, 1, 1, 25, 25, 4]);
        let l = limits(50, 4, 100);
        let (g, _) = plan_groups(&sizes, &l);
        assert!(!g.is_empty());
        for r in &g {
            let total: u64 = sizes[r.clone()].iter().map(|s| s.unwrap()).sum();
            assert!(r.len() >= 2 && r.len() <= 4, "{r:?} has {} files", r.len());
            assert!(total <= 50 * MB, "{r:?} is {} MB", total / MB);
        }
    }

    #[test]
    fn large_and_unknown_fragments_are_boundaries() {
        // 50 MB (at limit) and unknown sizes are never compacted and split groups.
        let sizes = vec![Some(MB), Some(MB), Some(50 * MB), Some(MB), None, Some(MB), Some(MB)];
        let (g, _) = plan_groups(&sizes, &limits(50, 20, 10));
        assert_eq!(g, vec![0..2, 5..7]);
    }

    #[test]
    fn single_fragment_groups_are_skipped() {
        let (g, _) = plan_groups(&mb(&[30, 30, 30]), &limits(50, 20, 10));
        assert!(g.is_empty());
    }

    #[test]
    fn max_tasks_reports_more_work() {
        let (g, more) = plan_groups(&mb(&[1; 100]), &limits(50, 20, 2));
        assert_eq!(g, vec![0..20, 20..40]);
        assert!(more);
        let (_, more) = plan_groups(&mb(&[1; 40]), &limits(50, 20, 2));
        assert!(!more, "exactly max_tasks groups is not 'more'");
    }
}
