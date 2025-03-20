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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

const U16_SIZE: usize = std::mem::size_of::<u16>();

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn compute_overlap(first_key: &[u8], key: &[u8]) -> usize {
    let mut i = 0;
    loop {
        if i >= first_key.len() || i >= key.len() {
            break;
        }
        if first_key[i] != key[i] {
            break;
        }
        i += 1;
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    pub fn real_size(&self) -> usize {
        self.data.len() + self.offsets.len() * U16_SIZE + U16_SIZE
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.is_empty() {
            if self.real_size() + key.len() + value.len() + U16_SIZE * 3 > self.block_size {
                return false;
            }
        } else {
            self.first_key = KeyVec::from_vec(key.into_inner().to_vec());
            self.offsets.push(self.data.len() as u16);
            self.data.put_u16(0_u16);
            self.data.put_u16((key.len()) as u16);
            self.data.put(key.raw_ref());
            self.data.put_u16(value.len() as u16);
            self.data.put(value);
            return true;
        }

        let overlap = compute_overlap(self.first_key.raw_ref(), key.raw_ref());
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(overlap as u16);
        self.data.put_u16((key.len() - overlap) as u16);
        self.data.put(&key.raw_ref()[overlap..]);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
