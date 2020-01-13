use crate::stream::{BufferedWriter, BufferedReader};
use std::sync::{RwLock, Arc};
use std::collections::BTreeMap;
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

#[derive(Clone)]
pub struct TS {
    append_only_block: Arc<RwLock<AppendOnlyBlock>>,
    closed_blocks: Arc<RwLock<Vec<ClosedBlock>>>,
}

impl TS {
    pub fn new() -> Self {
        TS {
            append_only_block: Arc::new(RwLock::new(AppendOnlyBlock::new(0, 0))),
            closed_blocks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn append(&self, dp: DataPoint) {
        let mut aob = self.append_only_block.write().unwrap();
        if aob.time_begin == 0 {
            aob.time_begin = dp.time;
            aob.time_end = dp.time + 2 * 60 * 60;
        } else {
            if dp.time > aob.time_end {
                {
                    self.closed_blocks.write().unwrap().push(
                        ClosedBlock::new(aob.deref())
                    );
                }
                let time_begin = aob.time_end;
                let time_end = time_begin + 2 * 60 * 60;
                aob.from(AppendOnlyBlock::new(time_begin, time_end));
            }
        }
        aob.encoder.encode(dp);
    }

    pub fn get_decoder<F>(&self, begin_time: u64, end_time: u64, f: F)
        where F: FnOnce(StdDecoder<BufferedReader>) {
        println!("search ts: [{}, {})", begin_time, end_time);

        let r = self.append_only_block.read().unwrap();
        let a = r.get_decoder();
        f(a);
    }
}

pub type TSTreeMap = RwLock<BTreeMap<String, TS>>;

#[derive(Clone)]
pub struct BTreeEngine {
    ts_store: Arc<TSTreeMap>
}

impl BTreeEngine {
    pub fn new() -> Self {
        BTreeEngine {
            ts_store: Arc::new(TSTreeMap::new(BTreeMap::new()))
        }
    }

//    pub fn get_ts(&self, ts_name: &String) -> Option<&TS> {
//        let store = self.ts_store.read().unwrap();
//        store.get(ts_name)
//    }

    pub fn append(&self, ts_name: &String, dp: DataPoint) {
        // try read
        {
            let store = self.ts_store.read().unwrap();
            match store.get(ts_name) {
                Some(ts) => {
                    ts.append(dp);
                }
                None => {}
            }
        };

        // read check and write
        {
            let mut store = self.ts_store.write().unwrap();
            match store.get(ts_name) {
                Some(ts) => {
                    ts.append(dp);
                }
                None => {
                    let ts = TS::new();
                    ts.append(dp);
                    store.insert(ts_name.to_string(), ts);
                }
            }
        }
    }

    pub fn get(&self, ts_name: &String) -> Option<TS> {
        let store = self.ts_store.read().unwrap();
        match store.get(ts_name) {
            Some(ts) => {
                Some(ts.clone())
            },
            None =>{
                None
            }
        }
    }
}

impl Default for BTreeEngine {
    fn default() -> Self {
        Self::new()
    }
}
