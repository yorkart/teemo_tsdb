use crate::stream::{BufferedWriter, BufferedReader};
use std::sync::{RwLock, Arc};
use std::collections::HashMap;
use crate::DataPoint;

#[derive(Debug)]
pub struct AppendOnlyBlock {
    time_begin: u32,
    time_end: u32,
    bytes_rw: Arc<RwLock<Vec<u8>>>,
}

impl AppendOnlyBlock {
    pub fn new(time_begin: u32, time_end: u32) -> Self {
        let mut bytes: Vec<u8> = Vec::new();
        let writer = BufferedWriter::new(bytes.as_mut());
        let rw = RwLock::new(bytes);
        let bytes_rw = Arc::new(rw);
        AppendOnlyBlock {
            time_begin,
            time_end,
            bytes_rw,
        }
    }

    pub fn len(&self) -> usize {
        self.bytes_rw.read().unwrap().len()
    }
}

impl Clone for AppendOnlyBlock {
    fn clone(&self) -> Self {
        let clone = self.bytes_rw.clone();
        AppendOnlyBlock {
            time_begin: self.time_begin,
            time_end: self.time_end,
            bytes_rw: clone,
        }
    }
}

#[derive(Debug)]
pub struct ClosedBlock {
    time_begin: u32,
    time_end: u32,
    bytes: Arc<Vec<u8>>,
}

impl ClosedBlock {
    pub fn new(append_only_block: AppendOnlyBlock) -> Self {
        let bytes = append_only_block.bytes_rw.read().unwrap().to_vec();
        ClosedBlock {
            time_begin: append_only_block.time_begin,
            time_end: append_only_block.time_end,
            bytes: Arc::new(bytes),
        }
    }
}

pub struct TS {
    append_only_block: AppendOnlyBlock,
    closed_blocks: Vec<ClosedBlock>,
}

impl TS {
    pub fn new(time_begin: u32, bytes: Vec<u8>) -> Self {
        TS {
            append_only_block: AppendOnlyBlock::new(0, 0),
            closed_blocks: Vec::new(),
        }
    }

    pub fn append(&self, data_point: DataPoint) {
        self.append_only_block.bytes_rw.write().unwrap();
    }
}

pub struct TSMap {
    ts_map : std::collections::HashMap<String, TS>,
}