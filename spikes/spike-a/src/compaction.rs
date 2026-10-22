//! Size- and count-bounded compaction (the service's `compact` op).
//!
//! Adjacent small fragments are grouped and each group is rewritten as ONE file. A group
//! closes when adding the next fragment would exceed `max_file_bytes` OR `max_files`, so
//! both limits always hold. Fragments already at the size limit, or with an unknown size,
//! are never touched and act as boundaries (groups only join neighbours, which keeps the
//! table's sort order). Groups of one fragment are skipped: there is nothing to merge.
//! At most `max_tasks` groups are planned per run, so one run fits in the Lambda timeout.

use std::ops::Range;

use lance::Dataset;
use lance::dataset::optimize::{
    CompactionMetrics, CompactionOptions, CompactionPlan, CompactionPlanner, TaskData,
    compact_files_with_planner,
};
use lance_table::format::Fragment;

#[derive(Debug, Clone, Copy)]
pub struct CompactLimits {
    pub max_file_bytes: u64,
    pub max_files: usize,
    pub max_tasks: usize,
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
    f.files
        .iter()
        .map(|d| d.file_size_bytes.get().map(|n| n.get()))
        .sum()
}

pub struct SizeCountPlanner {
    pub limits: CompactLimits,
    pub options: CompactionOptions,
}

#[async_trait::async_trait]
impl CompactionPlanner for SizeCountPlanner {
    async fn plan(&self, dataset: &Dataset) -> lance::Result<CompactionPlan> {
        let fragments: Vec<Fragment> = dataset.get_fragments().iter().map(|f| f.metadata().clone()).collect();
        let sizes: Vec<Option<u64>> = fragments.iter().map(fragment_bytes).collect();
        let (groups, _) = plan_groups(&sizes, &self.limits);
        Ok(CompactionPlan {
            tasks: groups
                .into_iter()
                .map(|r| TaskData { fragments: fragments[r].to_vec() })
                .collect(),
            read_version: dataset.version().version,
            options: self.options.clone(),
        })
    }
}

/// Outcome of one compaction run.
#[derive(Debug)]
pub struct CompactReport {
    pub metrics: CompactionMetrics,
    /// More groups were eligible than `max_tasks`; call again.
    pub more: bool,
}

/// Runs one bounded compaction on `dataset` and commits it (indices are remapped in the
/// same commit). Concurrent appends are compatible and are rebased by Lance.
pub async fn compact(dataset: &mut Dataset, limits: CompactLimits) -> lance::Result<CompactReport> {
    let sizes: Vec<Option<u64>> = dataset.get_fragments().iter().map(|f| fragment_bytes(f.metadata())).collect();
    let (_, more) = plan_groups(&sizes, &limits);
    let options = CompactionOptions {
        // Each planned group must become exactly one output file: the row target only
        // splits outputs, so set it above any group, and cap bytes as a hard limit.
        target_rows_per_fragment: usize::MAX / 2,
        max_bytes_per_file: Some(limits.max_file_bytes as usize),
        ..Default::default()
    };
    let planner = SizeCountPlanner { limits, options };
    let metrics = compact_files_with_planner(dataset, None, &planner).await?;
    Ok(CompactReport { metrics, more })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1 << 20;

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
