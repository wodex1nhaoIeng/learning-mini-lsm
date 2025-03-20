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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Ok, Result};
use bytes::Bytes;
use farmhash::fingerprint32;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator};
use crate::key::{self, KeyBytes, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::map_bound;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::SsTableBuilder;
use crate::table::SsTableIterator;
use crate::table::{FileObject, SsTable};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
        let mut compaction_thread = self.compaction_thread.lock();

        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            let new_memtable = Arc::new(MemTable::create(self.inner.next_sst_id()));
            let mut state = self.inner.state.write();
            let mut snapshot = state.as_ref().clone();
            let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);
            snapshot.imm_memtables.insert(0, old_memtable);
            *state = Arc::new(snapshot);
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }

        self.inner.sync_dir()?;
        Ok(())
        // unimplemented!()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let mut next_sst_id = 1;
        if !path.exists() {
            std::fs::create_dir_all(path).context("Failed to create storage directory")?;
        }
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,
        let manifest;

        let manifest_path = path.join("MANIFEST");
        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            manifest =
                Manifest::create(&manifest_path).context("Failed to create manifest file")?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let mut memtables = BTreeSet::new();
            let (m, records) = Manifest::recover(manifest_path)?;
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        memtables.remove(&sst_id);
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        // next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::Compaction(task, new_sst_ids) => {
                        let (new_state, _) = compaction_controller.apply_compaction_result(
                            &state,
                            &task,
                            &new_sst_ids,
                            true,
                        );
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(new_sst_ids.iter().max().copied().unwrap_or(0));
                    }
                    ManifestRecord::NewMemtable(memtable_id) => {
                        next_sst_id = next_sst_id.max(memtable_id);
                        memtables.insert(memtable_id);
                    }
                }
            }

            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let table_id = *table_id;
                let sst = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, table_id))
                        .context("failed to open SST")?,
                )?;
                state.sstables.insert(table_id, Arc::new(sst));
            }

            if let CompactionController::Leveled(_) = &compaction_controller {
                for (_id, ssts) in &mut state.levels {
                    ssts.sort_by(|x, y| {
                        state
                            .sstables
                            .get(x)
                            .unwrap()
                            .first_key()
                            .cmp(state.sstables.get(y).unwrap().first_key())
                    })
                }
            }

            next_sst_id += 1;

            if options.enable_wal {
                for memtable_id in memtables {
                    let memtable = MemTable::recover_from_wal(
                        memtable_id,
                        Self::path_of_wal_static(path, memtable_id),
                    )?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }

            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            // next_sst_id += 1;
            manifest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        // let qwq = self.state;
        let read_guard = self.state.read();
        let snap_shot = Arc::clone(&read_guard);
        drop(read_guard);
        if let Some(v) = snap_shot.memtable.get(_key) {
            if v.is_empty() {
                return Ok(None);
            }
            return Ok(Some(v));
        }
        for memtable in snap_shot.imm_memtables.iter() {
            if let Some(v) = memtable.get(_key) {
                if v.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(v));
            }
        }

        for sstable in snap_shot.l0_sstables.iter() {
            let sstable = snap_shot.sstables.get(sstable).unwrap();

            if _key < sstable.first_key().raw_ref() || _key > sstable.last_key().raw_ref() {
                continue;
            }
            if let Some(bloom) = &sstable.bloom {
                if !bloom.may_contain(fingerprint32(_key)) {
                    continue;
                }
            }
            let iter = SsTableIterator::create_and_seek_to_key(
                sstable.clone(),
                key::KeySlice::from_slice(_key),
            )?;
            if iter.is_valid() && iter.key() == key::KeySlice::from_slice(_key) {
                if iter.value().is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        for i in 0..snap_shot.levels.len() {
            let mut tables = Vec::new();
            for sstable_id in snap_shot.levels[i].1.iter() {
                let sstable = snap_shot.sstables.get(sstable_id).unwrap();

                if _key < sstable.first_key().raw_ref() || _key > sstable.last_key().raw_ref() {
                    continue;
                }
                if let Some(bloom) = &sstable.bloom {
                    if !bloom.may_contain(fingerprint32(_key)) {
                        continue;
                    }
                }

                tables.push(sstable.clone());
            }

            let iter =
                SstConcatIterator::create_and_seek_to_key(tables, key::KeySlice::from_slice(_key))?;

            if iter.is_valid() && iter.key() == key::KeySlice::from_slice(_key) {
                if iter.value().is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in _batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    let fake_write_guard = self.state.read();
                    fake_write_guard.memtable.put(key, value)?;
                    let size = fake_write_guard.memtable.approximate_size();
                    drop(fake_write_guard);
                    if size >= self.options.target_sst_size {
                        let state_lock = self.state_lock.lock();
                        if self.state.read().memtable.approximate_size()
                            >= self.options.target_sst_size
                        {
                            self.force_freeze_memtable(&state_lock)?;
                        }
                    }
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    let fake_write_guard = self.state.read();
                    fake_write_guard.memtable.put(key, b"")?;
                    let size = fake_write_guard.memtable.approximate_size();
                    drop(fake_write_guard);
                    if size >= self.options.target_sst_size {
                        let state_lock = self.state_lock.lock();
                        if self.state.read().memtable.approximate_size()
                            >= self.options.target_sst_size
                        {
                            self.force_freeze_memtable(&state_lock)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    // We could use read lock because the crossbeam_skiplist::SkipMap is based on a lock-free skip list
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(_key, _value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(_key)])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(id, self.path_of_wal(id))?)
        } else {
            Arc::new(MemTable::create(id))
        };
        let mut state = self.state.write();
        let mut snapshot = state.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        old_memtable.sync_wal()?;
        snapshot.imm_memtables.insert(0, old_memtable);
        *state = Arc::new(snapshot);

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(_state_lock_observer, ManifestRecord::NewMemtable(id))?;
        self.sync_dir()?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let lock = self.state_lock.lock();
        let flushed_memtable;
        {
            let read_guard = self.state.read();
            let snapshot = read_guard.as_ref().clone();
            flushed_memtable = snapshot.imm_memtables.last().unwrap().clone();
        }

        let mut builder = SsTableBuilder::new(self.options.block_size);
        flushed_memtable.flush(&mut builder)?;

        let sst_id = flushed_memtable.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            snapshot.sstables.insert(sst_id, sst);
            *guard = Arc::new(snapshot);
        };

        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id)).context("Failed to remove WAL file")?;
        }

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&lock, ManifestRecord::Flush(sst_id))?;

        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    fn range_overlap(
        user_begin: Bound<&[u8]>,
        user_end: Bound<&[u8]>,
        table_begin: &KeyBytes,
        table_end: &KeyBytes,
    ) -> bool {
        match user_end {
            Bound::Excluded(key) if key <= table_begin.raw_ref() => {
                return false;
            }
            Bound::Included(key) if key < table_begin.raw_ref() => {
                return false;
            }
            _ => {}
        }
        match user_begin {
            Bound::Excluded(key) if key >= table_end.raw_ref() => {
                return false;
            }
            Bound::Included(key) if key > table_end.raw_ref() => {
                return false;
            }
            _ => {}
        }
        true
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(_lower, _upper)));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(_lower, _upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut l0_table_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if Self::range_overlap(_lower, _upper, table.first_key(), table.last_key()) {
                let iter = match _lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key),
                        )?;
                        if iter.is_valid() && iter.key() == KeySlice::from_slice(key) {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };

                l0_table_iters.push(Box::new(iter));
            }
        }

        let table_iter = MergeIterator::create(l0_table_iters);
        let mem_l0table_iter = TwoMergeIterator::create(memtable_iter, table_iter)?;

        let mut iters = Vec::new();
        for i in 0..snapshot.levels.len() {
            let mut tables = Vec::with_capacity(snapshot.levels[i].1.len());

            for table_id in snapshot.levels[i].1.iter() {
                let table = snapshot.sstables[table_id].clone();
                if Self::range_overlap(_lower, _upper, table.first_key(), table.last_key()) {
                    tables.push(table);
                }
            }

            let iter = match _lower {
                Bound::Included(key) => {
                    SstConcatIterator::create_and_seek_to_key(tables, KeySlice::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        tables,
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key() == KeySlice::from_slice(key) {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(tables)?,
            };
            iters.push(Box::new(iter));
        }

        let concatiter = MergeIterator::create(iters);

        let iter = TwoMergeIterator::create(mem_l0table_iter, concatiter)?;

        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(_upper),
        )?))
    }
}
