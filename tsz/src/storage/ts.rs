use std::sync::{RwLock, Arc};
use std::ops::Deref;
use crate::{DataPoint, Encode, StdDecoder};
use crate::stream::BufferedReader;
use crate::storage::block::{AppendOnlyBlock, ClosedBlock};

#[derive(Clone)]
pub struct TS {
    append_only_block: Arc<RwLock<AppendOnlyBlock>>,
    closed_blocks: Arc<RwLock<Vec<ClosedBlock>>>,
    period: u64,
}

impl TS {
    pub fn new() -> Self {
        TS {
            append_only_block: Arc::new(RwLock::new(AppendOnlyBlock::new(0, 0))),
            closed_blocks: Arc::new(RwLock::new(Vec::new())),
            period: 2 * 60 * 60,
        }
    }

    pub fn append(&self, dp: DataPoint) {
        let mut aob = self.append_only_block.write().unwrap();
        if aob.time_begin == 0 {
            let (begin_ts, end_ts) = self.time_align(dp.time, self.period);
            aob.from(AppendOnlyBlock::new(begin_ts, end_ts));
        } else {
            if dp.time > aob.time_end {
                {
                    self.closed_blocks.write().unwrap().push(
                        ClosedBlock::new(aob.deref())
                    );
                }

                let (begin_ts, end_ts) = self.time_align(dp.time, self.period);
                aob.from(AppendOnlyBlock::new(begin_ts, end_ts));
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

        ts.get_decoder(0,0 , || {})
    }
}