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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let mut previous_size = 0;
        for i in 0.._snapshot.levels.len() - 1 {
            previous_size += _snapshot.levels[i].1.len();
        }
        println!("{:?}", self.options);
        if previous_size as f64 / _snapshot.levels.last().unwrap().1.len() as f64
            >= self.options.max_size_amplification_percent as f64 * 0.01
        {
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        previous_size = 0;
        let mut max_width_merge = 2147483647;
        if let Some(x) = self.options.max_merge_width {
            max_width_merge = x;
        }

        for i in 0.._snapshot.levels.len() {
            let this_tier_size = _snapshot.levels[i].1.len();

            if (this_tier_size as f64 / previous_size as f64)
                > (100.0 + self.options.size_ratio as f64) / 100.0
            {
                println!("this_tier_size: {:?}, {:?}", this_tier_size, previous_size);
                if i >= self.options.min_merge_width && i < max_width_merge {
                    let mut tiers = Vec::new();
                    for j in 0..i {
                        tiers.push(_snapshot.levels[j].clone());
                    }
                    return Some(TieredCompactionTask {
                        tiers: tiers,
                        bottom_tier_included: i == _snapshot.levels.len() - 1,
                    });
                }
            }

            previous_size += this_tier_size;
        }
        let mut tier = Vec::new();
        for i in 0.._snapshot.levels.len().min(max_width_merge) {
            tier.push(_snapshot.levels[i].clone());
        }

        Some(TieredCompactionTask {
            tiers: tier,
            bottom_tier_included: _snapshot.levels.len() <= max_width_merge,
        })
        // unimplemented!()
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut files_to_remove = Vec::new();
        let mut need_to_remove_tier = HashSet::new();
        let mut levels = Vec::new();
        let mut flag = false;

        for i in _task.tiers.iter() {
            need_to_remove_tier.insert(i.0);
        }
        for i in snapshot.levels {
            if need_to_remove_tier.remove(&i.0) {
                files_to_remove.extend(i.1.clone());
            } else {
                levels.push(i.clone());
            }
            if need_to_remove_tier.is_empty() && flag == false {
                flag = true;
                levels.push((_output[0], _output.to_vec()));
            }
        }
        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
