use crate::stream::{BufferedWriter, BufferedReader};
use std::sync::RwLock;
use std::collections::HashMap;
use crate::{DataPoint, StdEncoder, StdDecoder, Encode};
use crate::stream::Buffer;
use std::ops::Deref;

#[derive(Debug)]
pub struct AppendOnlyBlock {
    time_begin: u64,
    time_end: u64,

    encoder: StdEncoder<BufferedWriter>,
}

impl AppendOnlyBlock {
    pub fn new(time_begin: u64, time_end: u64) -> Self {
        let writer = BufferedWriter::new();
        let encoder = StdEncoder::new(time_begin, writer);

        AppendOnlyBlock {
            time_begin,
            time_end,
            encoder,
        }
    }

    pub fn from(&mut self, other: AppendOnlyBlock) {
        self.time_begin = other.time_begin;
        self.time_end = other.time_end;
        self.encoder = other.encoder;
    }

    pub fn get_decoder(&self) -> StdDecoder<BufferedReader> {
        let reader = BufferedReader::new(self.encoder.get_buffer());
        StdDecoder::new(reader)
    }

    pub fn len(&self) -> u64 {
        self.encoder.get_size()
    }

    pub fn get_buffer(&self) -> &Buffer {
        self.encoder.get_buffer()
    }
}

#[derive(Debug)]
pub struct ClosedBlock {
    time_begin: u64,
    time_end: u64,
    bytes: Buffer,
}

impl ClosedBlock {
    pub fn new(append_only_block: &AppendOnlyBlock) -> Self {
        let bytes = append_only_block.get_buffer().to_vec();
        ClosedBlock {
            time_begin: append_only_block.time_begin,
            time_end: append_only_block.time_end,
            bytes,
        }
    }

    pub fn get_decoder(&self) -> StdDecoder<BufferedReader> {
        let reader = BufferedReader::new(self.bytes.as_ref());
        StdDecoder::new(reader)
    }
}

pub struct TS {
    append_only_block: RwLock<AppendOnlyBlock>,
    closed_blocks: Vec<ClosedBlock>,
}

impl TS {
    pub fn new() -> Self {
        TS {
            append_only_block: RwLock::new(AppendOnlyBlock::new(0, 0)),
            closed_blocks: Vec::new(),
        }
    }

    pub fn append(&mut self, dp: DataPoint) {
        let mut n = self.append_only_block.write().unwrap();
        if n.time_begin == 0 {
            n.time_begin = dp.time;
            n.time_end = dp.time + 2 * 60 * 60;
        } else {
            if dp.time > n.time_end {
                self.closed_blocks.push(
                    ClosedBlock::new(n.deref())
                );
                let time_begin = n.time_end;
                let time_end = time_begin + 2 * 60 * 60;
                n.from(AppendOnlyBlock::new(time_begin, time_end));
            }
        }
        n.encoder.encode(dp);
    }

    pub fn get_decoder<F>(&self, begin_time: u64, end_time: u64, f : F)
    where F : FnOnce(StdDecoder<BufferedReader> ) {
        println!("search ts: [{}, {})", begin_time, end_time);

        let r = self.append_only_block.read().unwrap();
        let a = r.get_decoder();
        f(a);
    }
}

pub struct TSMap {
    rw: RwLock<bool>,
    ts_map: HashMap<String, TS>,
}

impl TSMap {
    pub fn new() -> Self {
        TSMap {
            rw: RwLock::new(true),
            ts_map: HashMap::new(),
        }
    }

    pub fn append(&mut self, ts_name: &String, dp: DataPoint) {
        let mut b = false;
        {
            let _ = self.rw.read().unwrap();
            if self.ts_map.contains_key(ts_name) {
                let ts = self.ts_map.get_mut(ts_name).unwrap();
                ts.append(dp);
            } else {
                b = true;
            }
        }

        if b {
            let _ = self.rw.write().unwrap();

            if !self.ts_map.contains_key(ts_name) {
                self.ts_map.insert(ts_name.to_string(), TS::new());
            }

            let ts = self.ts_map.get_mut(ts_name).unwrap();
            ts.append(dp);
        }
    }

    pub fn get(&self, ts_name: &String) -> Option<&TS> {
        let _ = self.rw.read().unwrap();
        self.ts_map.get(ts_name)
    }
}