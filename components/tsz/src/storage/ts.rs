use std::ops::DerefMut;
use crate::{DataPoint, Encode, StdDecoder};
use crate::stream::BufferedReader;
use crate::storage::block::{AppendOnlyBlock, ClosedBlock, Block};

pub type SharedRwLockVec<T> = common::SharedRwLock<Vec<T>>;

fn new_shared_rw_lock_vec<T>() -> SharedRwLockVec<T> where T: Block {
    common::new_shared_rw_lock(Vec::new())
}

#[derive(Clone)]
pub struct TS {
    append_only_blocks: SharedRwLockVec<AppendOnlyBlock>,
    closed_blocks: SharedRwLockVec<ClosedBlock>,
    period: u64,
}

impl TS {
    pub fn new() -> Self {
        TS {
            append_only_blocks: new_shared_rw_lock_vec(), // new_shared_rw_lock(AppendOnlyBlock::new(0, 0)),
            closed_blocks: new_shared_rw_lock_vec(),
            period: 2 * 60 * 60,
        }
    }

    /// check and roll down append_only_block to closed_block
    pub fn roll_down(&self, timeout: u64) {
        // read check
        let closed_block = {
            let append_only_blocks = self.append_only_blocks.read().unwrap();
            match append_only_blocks.get(0) {
                Some(block) => {
                    let now = common::now_timestamp_mills();
                    if (now - block.time_begin as u128) < timeout as u128 {
                        None
                    } else {
                        Some(ClosedBlock::new(block))
                    }
                }
                None => None,
            }
        };

        // write check and roll down
        match closed_block {
            Some(block) => {
                self.closed_blocks.write().unwrap().push(block);
            }
            None => {}
        }
    }

    pub fn append(&self, dp: DataPoint) {
        let mut append_only_blocks = self.append_only_blocks.write().unwrap();
        let append_only_blocks = append_only_blocks.deref_mut();

        if append_only_blocks.len() == 0 {
            let (begin_ts, end_ts) = self.time_align(dp.time, self.period);
            append_only_blocks.push(AppendOnlyBlock::new(begin_ts, end_ts));
            return;
        }

        let star_time = append_only_blocks.get(0).unwrap().time_begin;
//        let end_time = append_only_blocks.get(append_only_blocks.len() - 1).unwrap().time_end;

        if dp.time < star_time {
            // skip old data point.
            return;
        }

        for i in 0..append_only_blocks.len() {
            let aob = append_only_blocks.get_mut(i).unwrap();
            if dp.time >= aob.time_begin && dp.time < aob.time_end {
                aob.encoder.encode(dp);
                return;
            }
        }

        let (begin_ts, end_ts) = self.time_align(dp.time, self.period);
        let aob = AppendOnlyBlock::new(begin_ts, end_ts);
        for i in 0..append_only_blocks.len() {
            if begin_ts < append_only_blocks.get(i).unwrap().time_begin {
                append_only_blocks.insert(i, aob);
                return;
            }
        }
    }

    pub fn get_decoder<F>(&self, begin_time: u64, end_time: u64, f: F)
        where F: Fn(StdDecoder<BufferedReader>) {
        println!("search ts: [{}, {})", begin_time, end_time);

        let r = self.append_only_blocks.read().unwrap();
        for block in r.iter() {
            let a = block.get_decoder();
            f(a);
        }
    }

    /// timestamp : sec
    /// period: sec
    fn time_align(&self, timestamp: u64, period: u64) -> (u64, u64) {
        let ts = timestamp - timestamp % period;
        (ts as u64, (ts + period) as u64)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::ts::TS;
    use std::time::{Duration, UNIX_EPOCH};
    use chrono::{DateTime, Utc};

    #[test]
    fn time_align_test() {
        let ts = TS::new();
        {
// Tue Jan 14 2020 08:02:26 GMT+0800
            let timestamp = 1578960146;
            let (b, e) = ts.time_align(timestamp, 1 * 60 * 60 * 3);
            let origin_dt: DateTime<Utc> = {
                let b_ts = UNIX_EPOCH + Duration::from_secs(timestamp);
                b_ts.into()
            };
            let begin_dt: DateTime<Utc> = {
                let b_ts = UNIX_EPOCH + Duration::from_secs(b);
                b_ts.into()
            };
            let end_dt: DateTime<Utc> = {
                let b_ts = UNIX_EPOCH + Duration::from_secs(e);
                b_ts.into()
            };
            println!("{}={}=>\n{}={}\n{}={}",
                     timestamp,
                     origin_dt.format("%Y-%m-%d %T%z"),
                     b,
                     begin_dt.format("%Y-%m-%d %H:%M:%S%z"),
                     e,
                     end_dt.format("%Y-%m-%d %T%z"));
        }

        ts.get_decoder(0, 0, |_| {})
    }
}