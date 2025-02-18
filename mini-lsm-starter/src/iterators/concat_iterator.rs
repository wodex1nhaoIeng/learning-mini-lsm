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

use std::sync::Arc;

use anyhow::{Ok, Result};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let iter;
        if sstables.is_empty() {
            iter = Self {
                current: None,
                next_sst_idx: 0,
                sstables: sstables,
            };
            return Ok(iter);
        }
        let iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 1,
            sstables: sstables,
        };
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let iter;
        if sstables.is_empty() {
            iter = Self {
                current: None,
                next_sst_idx: 0,
                sstables: sstables,
            };
            return Ok(iter);
        }
        let mut left = 0;
        let mut right = sstables.len();
        while left < right {
            let mid = (left + right) >> 1;
            if sstables[mid].first_key().raw_ref() < key.raw_ref() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if left == sstables.len() {
            // last sstable's fisrt key < key
            if sstables[sstables.len() - 1].last_key().raw_ref() < key.raw_ref() {
                iter = Self {
                    current: None,
                    next_sst_idx: 0,
                    sstables: sstables,
                };
                return Ok(iter);
            }
        }
        if left > 0 {
            if sstables[left - 1].last_key().raw_ref() >= key.raw_ref() {
                left -= 1;
            }
        }
        let iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[left].clone(),
                key,
            )?),
            next_sst_idx: left + 1,
            sstables: sstables,
        };
        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if self.current.is_none() {
            return false;
        }
        self.current.as_ref().unwrap().is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        if self.current.as_ref().unwrap().is_valid() {
            return Ok(());
        }
        if self.next_sst_idx < self.sstables.len() {
            self.current = Some(SsTableIterator::create_and_seek_to_first(
                self.sstables[self.next_sst_idx].clone(),
            )?);
            self.next_sst_idx += 1;
            return Ok(());
        }
        self.current = None;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
