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

use std::collections::HashSet;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{key::KeyBytes, lsm_storage::LsmStorageState};

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
        let mut target_size = Vec::new();
        let mut base_level = self.options.max_levels;
        target_size.push(0); // 第0层大小不考虑，
        level_real_size.push(0); // 第0层大小不考虑，
        for i in 0..self.options.max_levels {
            let mut size = 0;
            target_size.push(0 as usize);
            for table_id in _snapshot.levels[i].1.iter() {
                size += _snapshot.sstables.get(table_id).unwrap().table_size();
            }
            level_real_size.push(size as usize);
        }

        let base_level_size = self.options.base_level_size_mb * 1024 * 1024;

        // println!("base_level_size: {}", base_level_size);
        target_size[self.options.max_levels] =
            base_level_size.max(level_real_size[self.options.max_levels]);
        for i in (0..self.options.max_levels).rev() {
            let next_level_size = target_size[i + 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;

            // println!("next_level_size: {}, this_level_size: {}", next_level_size, this_level_size);
            if next_level_size > base_level_size {
                target_size[i] = this_level_size;
            }
            if target_size[i] > 0 {
                base_level = i;
            }
        }

        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    _snapshot.l0_sstables.as_slice(),
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut priority_vec = Vec::new();

        for i in 1..self.options.max_levels {
            let real_size = level_real_size[i];
            let target_size = target_size[i];
            if target_size > 0 && real_size > target_size {
                priority_vec.push((real_size as f64 / target_size as f64, i));
            }
        }
        priority_vec.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

        if let Some((_, level)) = priority_vec.first() {
            let selected_sst = _snapshot.levels[level - 1].1.iter().min().copied().unwrap(); // select the oldest sst to compact
            return Some(LeveledCompactionTask {
                upper_level: Some(*level),
                upper_level_sst_ids: vec![selected_sst],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &[selected_sst],
                    level + 1,
                ),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snap_shot = _snapshot.clone();
        let mut files_to_remove: Vec<usize> = Vec::new();

        let mut upper_id_set: HashSet<usize> = std::collections::HashSet::new();
        let mut lower_id_set: HashSet<usize> = std::collections::HashSet::new();
        upper_id_set.extend(&_task.upper_level_sst_ids);
        lower_id_set.extend(&_task.lower_level_sst_ids);

        let mut new_upper_id_vec = Vec::new();
        let mut new_lower_id_vec = Vec::new();

        if let Some(upper_level) = _task.upper_level {
            for id in _snapshot.levels[upper_level - 1].1.iter() {
                if !upper_id_set.contains(id) {
                    new_upper_id_vec.push(*id);
                }
            }
            snap_shot.levels[upper_level - 1].1 = new_upper_id_vec;
        } else {
            for id in _snapshot.l0_sstables.iter() {
                if !upper_id_set.contains(id) {
                    new_upper_id_vec.push(*id);
                }
            }
            snap_shot.l0_sstables = new_upper_id_vec;
        }

        for id in _snapshot.levels[_task.lower_level - 1].1.iter() {
            if !lower_id_set.contains(id) {
                new_lower_id_vec.push(*id);
            }
        }
        // println!("new_lower_id_vec: {:?}", new_lower_id_vec);

        new_lower_id_vec.extend(_output);

        if !_in_recovery {
            println!("new_lower_id_vec: {:?}", new_lower_id_vec);
            // println!("sstables: {:?}", snap_shot.sstables);
            for i in snap_shot.sstables.keys() {
                println!("sstables: {:?}", i);
            }
            new_lower_id_vec.sort_by(|x, y| {
                snap_shot
                    .sstables
                    .get(x)
                    .unwrap()
                    .first_key()
                    .cmp(snap_shot.sstables.get(y).unwrap().first_key())
            });
        }

        snap_shot.levels[_task.lower_level - 1].1 = new_lower_id_vec;

        files_to_remove.extend(&_task.lower_level_sst_ids);
        files_to_remove.extend(&_task.upper_level_sst_ids);
        (snap_shot, files_to_remove)
    }
}
