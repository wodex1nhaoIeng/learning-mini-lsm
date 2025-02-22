// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let mut levels = Vec::new();
        levels.push(_snapshot.l0_sstables.len());

        for level in _snapshot.levels.iter() {
            levels.push(level.1.len());
        }

        for i in 0..self.options.max_levels {
            if i == 0 {
                if levels[i] >= self.options.level0_file_num_compaction_trigger {
                    return Some(SimpleLeveledCompactionTask {
                        upper_level: None,
                        upper_level_sst_ids: Vec::new(),
                        lower_level: i + 1,
                        lower_level_sst_ids: _snapshot.levels[i].1.clone(),
                        is_lower_level_bottom_level: i + 1 == self.options.max_levels,
                    });
                }
                continue;
            }
            let ratio = levels[i + 1] as f64 / levels[i] as f64;
            if ratio < (self.options.size_ratio_percent as f64 / 100.0) {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(i),
                    upper_level_sst_ids: _snapshot.levels[i - 1].1.clone(),
                    lower_level: i + 1,
                    lower_level_sst_ids: _snapshot.levels[i].1.clone(),
                    is_lower_level_bottom_level: i + 1 == self.options.max_levels,
                });
            }
        }
        None
        // unimplemented!()
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut files_to_remove = Vec::new();
        if let Some(upper_level) = _task.upper_level {
            assert_eq!(
                _task.upper_level_sst_ids,
                snapshot.levels[upper_level - 1].1,
                "sst mismatched"
            );
            files_to_remove.extend(&snapshot.levels[upper_level - 1].1);
            snapshot.levels[upper_level - 1].1.clear();
        } else {
            assert_eq!(
                _task.upper_level_sst_ids, snapshot.l0_sstables,
                "sst mismatched"
            );
            files_to_remove.extend(&snapshot.l0_sstables);
            snapshot.l0_sstables.clear();
        }
        assert_eq!(
            _task.lower_level_sst_ids,
            snapshot.levels[_task.lower_level - 1].1,
            "sst mismatched"
        );
        files_to_remove.extend(&snapshot.levels[_task.lower_level - 1].1);
        snapshot.levels[_task.lower_level - 1].1 = _output.to_vec();
        (snapshot, files_to_remove)
    }
}
