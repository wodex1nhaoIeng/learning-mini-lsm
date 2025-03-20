#![allow(dead_code)]
// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::{Context, Result};
use bytes::Bytes;
use bytes::{Buf, BufMut};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create(true)
                    .write(true)
                    .open(_path.as_ref())
                    .context("Failed to create WAL file")?,
            ))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(_path.as_ref())?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut file_as_u8 = buf.as_slice();
        while !file_as_u8.is_empty() {
            let key_len = file_as_u8.get_u16() as usize;
            let key = Bytes::copy_from_slice(&file_as_u8[..key_len]);
            file_as_u8.advance(key_len);
            let value_len = file_as_u8.get_u16() as usize;
            let value = Bytes::copy_from_slice(&file_as_u8[..value_len]);
            file_as_u8.advance(value_len);
            _skiplist.insert(key, value);
        }
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> = Vec::new();
        buf.put_u16(_key.len() as u16);
        buf.put_slice(_key);
        buf.put_u16(_value.len() as u16);
        buf.put_slice(_value);
        file.write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();

        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
