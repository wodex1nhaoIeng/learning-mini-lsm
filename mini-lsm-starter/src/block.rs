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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

const U16_SIZE: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut res = self.data.clone();
        let num_of_elements = self.offsets.len();
        for i in &self.offsets {
            res.put_u16(*i);
        }
        res.put_u16(num_of_elements as u16);
        res.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let entry_offsets_len: u16 = (&data[data.len() - U16_SIZE..]).get_u16();

        // println!("data.len() = {}", data.len());
        // println!("entry_offsets_len = {}", entry_offsets_len);
        // panic!("Invalid data length");

        let data_slice = &data[..(data.len() - U16_SIZE - entry_offsets_len as usize * U16_SIZE)];
        let offsets_raw = &data
            [data.len() - U16_SIZE - entry_offsets_len as usize * U16_SIZE..data.len() - U16_SIZE];
        let offsets: Vec<u16> = offsets_raw
            .chunks(U16_SIZE)
            .map(|mut x| x.get_u16())
            .collect();
        Self {
            data: data_slice.to_vec(),
            offsets,
        }
    }

    fn get_first_key(&self) -> Vec<u8> {
        let mut buf = &self.data[..];
        buf.get_u16();
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        key.to_vec()
    }
}
