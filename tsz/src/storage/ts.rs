use crate::stream::BufferedReader;
use std::sync::{RwLock, Arc};
use crate::{DataPoint, Encode, StdDecoder};
use std::ops::Deref;
use crate::storage::block::{AppendOnlyBlock, ClosedBlock};

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