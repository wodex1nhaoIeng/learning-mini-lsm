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

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{key::KeyBytes, lsm_storage::LsmStorageState, table};

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let mut min_key = &KeyBytes::from_bytes(Bytes::new());
        let mut max_key = &KeyBytes::from_bytes(Bytes::new());
        for (i, id) in _sst_ids.iter().enumerate() {
            let sst = _snapshot.sstables.get(id).unwrap();
            if i == 0 {
                min_key = sst.first_key();
                max_key = sst.last_key();
            }
            if sst.first_key() < min_key {
                min_key = sst.first_key();
            }
            if sst.last_key() > max_key {
                max_key = sst.last_key();
            }
        }

        let mut overlap_ids = Vec::new();
        for sst_id in _snapshot.levels[_in_level - 1].1.iter() {
            let sst = _snapshot.sstables.get(sst_id).unwrap();
            if sst.last_key() < min_key || sst.first_key() > max_key {
                continue;
            }
            overlap_ids.push(*sst_id);
        }
        overlap_ids
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut level_real_size = Vec::new();
        level_real_size.push(0); // 第0层大小不考虑，
        for i in 0..self.options.max_levels {
            let mut size = 0;
            for table_id in _snapshot.levels[i].1.iter() {
                size += _snapshot.sstables.get(table_id).unwrap().table_size();
            }
            level_real_size.push(size as usize);
        }

        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    _snapshot.l0_sstables.as_slice(),
                    1,
                ),
                is_lower_level_bottom_level: false,
            });
        }
        unimplemented!()
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        unimplemented!()
    }
}
