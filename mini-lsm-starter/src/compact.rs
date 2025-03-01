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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use nom::character::complete::tab;
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::SsTableIterator;
use crate::table::{SsTable, SsTableBuilder};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn generate_new_sstables(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut new_sstables = Vec::new();
        let mut builder = None;
        while iter.is_valid() {
            let key = iter.key();
            let value = iter.value();
            if builder.is_none() {
                // 创建一个新的 SsTable
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            if !value.is_empty() {
                builder.as_mut().unwrap().add(key, value);
            }
            iter.next()?;

            if builder.as_ref().unwrap().estimated_size() >= self.options.target_sst_size {
                let id = self.next_sst_id();
                let builder = builder.take().unwrap();
                new_sstables.push(Arc::new(builder.build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                )?));
            }
        }
        if let Some(builder) = builder {
            let id = self.next_sst_id();
            new_sstables.push(Arc::new(builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?));
        }
        Ok(new_sstables)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snap_shot = {
            let read_guard = self.state.read();
            read_guard.clone()
        };

        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let iter;
                // 在这里处理 ForceFullCompaction 的逻辑
                let mut l0_iters = Vec::new();
                for i in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snap_shot.sstables.get(i).unwrap().clone(),
                    )?));
                }
                let mut l1_tables = Vec::new();
                for i in l1_sstables.iter() {
                    l1_tables.push(snap_shot.sstables.get(i).unwrap().clone());
                }
                iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_tables)?,
                )?;
                return self.generate_new_sstables(iter);
            }
            CompactionTask::Simple(task) => {
                let iter;
                let mut upper_iters = Vec::with_capacity(task.upper_level_sst_ids.len());
                for id in task.upper_level_sst_ids.iter() {
                    upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snap_shot.sstables.get(id).unwrap().clone(),
                    )?));
                }
                let upper_iter = MergeIterator::create(upper_iters);
                let mut lower_ssts = Vec::with_capacity(task.upper_level_sst_ids.len());
                for id in task.lower_level_sst_ids.iter() {
                    lower_ssts.push(snap_shot.sstables.get(id).unwrap().clone());
                }
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                return self.generate_new_sstables(iter);
            }
            CompactionTask::Tiered(task) => {
                let iter;
                let mut tiers = Vec::new();
                for tier in task.tiers.iter() {
                    let mut tables = Vec::new();
                    for id in tier.1.iter() {
                        tables.push(snap_shot.sstables.get(id).unwrap().clone());
                    }
                    tiers.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        tables,
                    )?));
                }
                iter = MergeIterator::create(tiers);
                return self.generate_new_sstables(iter);
            }
            _ => {
                unimplemented!()
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snap_shot;
        {
            let read_guard = self.state.read();
            snap_shot = read_guard.clone();
        }
        let l0_sstables = snap_shot.l0_sstables.clone();
        let l1_sstables = snap_shot.levels[0].1.clone();
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let new_sstables = self.compact(&task)?;
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                let result = state.sstables.remove(sst);
            }
            let mut ids = Vec::with_capacity(new_sstables.len());
            for new_sst in new_sstables {
                ids.push(new_sst.sst_id());
                let result = state.sstables.insert(new_sst.sst_id(), new_sst);
            }
            state.levels[0].1 = ids;
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            *self.state.write() = Arc::new(state);
        }
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let Some(task) = task else {
            return Ok(());
        };
        let sstables = self.compact(&task)?;

        // println!("233 {:?}", snapshot.l0_sstables.len());
        let files_added = sstables.len();
        let output = sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>();
        let ssts_to_remove = {
            let _state_lock = self.state_lock.lock();
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&self.state.read(), &task, &output, true);
            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());
            for file_to_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_to_remove);
                assert!(result.is_some());
                ssts_to_remove.push(result.unwrap());
            }
            let mut new_sst_ids = Vec::new();
            for file_to_add in sstables {
                new_sst_ids.push(file_to_add.sst_id());
                let result = snapshot.sstables.insert(file_to_add.sst_id(), file_to_add);
                assert!(result.is_none());
            }
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            ssts_to_remove
        };
        println!(
            "compaction finished: {} files removed, {} files added",
            ssts_to_remove.len(),
            files_added
        );
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let state = self.state.read();
        // println!("trigger_flush: {:?}", state.imm_memtables.len());
        if state.imm_memtables.len() >= self.options.num_memtable_limit {
            drop(state);
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
