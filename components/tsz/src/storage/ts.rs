use std::ops::DerefMut;
use crate::{DataPoint, Encode, StdDecoder};
use crate::stream::BufferedReader;
use crate::storage::block::{AppendOnlyBlock, ClosedBlock, Block};
use std::time::Duration;
use std::sync::mpsc::{SyncSender, Receiver};

#[derive(Clone)]
pub struct TS {
    append_only_blocks: common::SharedRwLockVec<AppendOnlyBlock>,
    closed_blocks: common::SharedRwLockVec<ClosedBlock>,
    period: u64,
    timer_guard: Option<timer::Guard>,
    data_tx: SyncSender<DataPoint>,
    close: bool,
}

impl TS {
    pub fn new() -> Self {
        let (data_tx, data_rx) = std::sync::mpsc::sync_channel(100000);

        let ts = TS {
            append_only_blocks: common::new_shared_rw_lock_vec(), // new_shared_rw_lock(AppendOnlyBlock::new(0, 0)),
            closed_blocks: common::new_shared_rw_lock_vec(),
            period: 2 * 60 * 60,
            timer_guard: None,
            data_tx,
            close: false,
        };

        ts.table_consumer(data_rx);
        ts
    }

    pub fn set_close(&mut self) {
        self.close = true;
    }

    fn table_consumer(&self, data_rx: Receiver<DataPoint>) {
        let clone = self.clone();
        std::thread::spawn(move || {
            loop {
                if clone.close {
                    break;
                }

                match data_rx.try_recv() {
                    Ok(raw) => {
                        clone.append(raw);
                    }
                    Err(_) => {
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        });
    }

    pub fn set_timer_guard(&mut self, guard: timer::Guard) {
        self.timer_guard = Some(guard);
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

    // todo error logic
    pub fn append_async(&self, dp: DataPoint) {
        self.data_tx.send(dp).unwrap();
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