use crate::stream::{BufferedWriter, BufferedReader};
use std::sync::{RwLock, Arc};
use std::collections::HashMap;
use crate::{DataPoint, StdEncoder, StdDecoder};
use std::cell::RefCell;

#[derive(Debug)]
pub struct AppendOnlyBlock<'a> {
    time_begin: u64,
    time_end: u64,
    bytes_rw: Vec<u8>,

    encoder: StdEncoder<BufferedWriter<'a>>,
    decoder: StdDecoder<BufferedReader<'a>>,
}

impl<'a> AppendOnlyBlock<'a> {
    pub fn new(time_begin: u64, time_end: u64) -> Self {
        let mut bytes: Vec<u8> = Vec::new();
        let writer = BufferedWriter::new(bytes.as_mut());
        let encoder = StdEncoder::new(time_begin, writer);

        let reader = BufferedReader::new(bytes.as_ref());
        let decoder = StdDecoder::new(reader);

        AppendOnlyBlock {
            time_begin,
            time_end,
            bytes_rw: bytes,
            encoder,
            decoder,
        }
    }

    pub fn len(&self) -> usize {
        self.bytes_rw.len()
    }
}

#[derive(Debug)]
pub struct ClosedBlock {
    time_begin: u64,
    time_end: u64,
    bytes: Arc<Vec<u8>>,
}

impl ClosedBlock {
    pub fn new(append_only_block: AppendOnlyBlock) -> Self {
        let bytes = append_only_block.bytes_rw.to_vec();
        ClosedBlock {
            time_begin: append_only_block.time_begin,
            time_end: append_only_block.time_end,
            bytes: Arc::new(bytes),
        }
    }
}

pub struct TS {
    append_only_block: RefCell<AppendOnlyBlock>,
    closed_blocks: Vec<ClosedBlock>,
}

impl TS {
    pub fn new(time_begin: u32, bytes: Vec<u8>) -> Self {
        TS {
            append_only_block: RefCell::new(AppendOnlyBlock::new(0, 0)),
            closed_blocks: Vec::new(),
        }
    }

    pub fn append(&self, data_point: DataPoint) {
//        self.append_only_block.bytes_rw.
    }
}

pub struct TSMap {
    ts_map: std::collections::HashMap<String, TS>,
}