use crate::block::{AppendOnlyBlock, ClosedBlock};
use std::ops::DerefMut;
use std::sync::mpsc::{Receiver, SyncSender};
use std::time::Duration;
use tszv1::stream::BufferedReader;
use tszv1::{DataPoint, Encode, StdDecoder};

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
    pub fn new(buffer_size: usize) -> Self {
        let (data_tx, data_rx) = std::sync::mpsc::sync_channel(buffer_size);

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
        std::thread::spawn(move || loop {
            match data_rx.try_recv() {
                Ok(raw) => {
                    clone.append(raw);
                }
                Err(_) => {
                    if clone.close {
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(1));
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
                    let now = common::now_timestamp_secs();
                    if (now - block.time_begin) < timeout {
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

        // no active block
        if append_only_blocks.len() == 0 {
            let (begin_ts, end_ts) = self.time_align(dp.time, self.period);
            let mut aob = AppendOnlyBlock::new(begin_ts, end_ts);
            aob.encoder.encode(dp);
            append_only_blocks.push(aob);
            info!(
                "create AppendOnlyBlock {},{} [{}/{}]",
                begin_ts,
                end_ts,
                common::timestamp_secs_to_string(begin_ts),
                common::timestamp_secs_to_string(end_ts)
            );
            return;
        }

        let start_time = append_only_blocks.get(0).unwrap().time_begin;
        //        let end_time = append_only_blocks.get(append_only_blocks.len() - 1).unwrap().time_end;

        if dp.time < start_time {
            info!("skip old DataPoint: {}", dp.time);
            // skip old data point.
            return;
        }

        // find block with time range and encode DataPoint
        for i in 0..append_only_blocks.len() {
            let aob = append_only_blocks.get_mut(i).unwrap();
            if dp.time >= aob.time_begin && dp.time < aob.time_end {
                aob.encoder.encode(dp);
                return;
            }
        }

        // if not find, create new block and encode DataPoint
        let (begin_ts, end_ts) = self.time_align(dp.time, self.period);
        let mut aob = AppendOnlyBlock::new(begin_ts, end_ts);
        aob.encoder.encode(dp);

        // find the index by time and insert block into append_only_blocks
        for i in 0..append_only_blocks.len() {
            if begin_ts < append_only_blocks.get(i).unwrap().time_begin {
                append_only_blocks.insert(i, aob);
                return;
            }
        }
    }

    pub fn get_decoder<F>(&self, begin_time: u64, end_time: u64, f: F)
    where
        F: Fn(StdDecoder<BufferedReader>),
    {
        info!(
            "search ts: {}",
            common::timestamp_to_interval_str(begin_time, end_time)
        );

        let r = self.append_only_blocks.read().unwrap();
        for block in r.iter() {
            info!(
                "--> block: {}",
                common::timestamp_to_interval_str(block.time_begin, block.time_end)
            );
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
    use crate::ts::TS;
    use chrono::{DateTime, Utc};
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn time_align_test() {
        let ts = TS::new(1000);
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
            info!(
                "{}={}=>\n{}={}\n{}={}",
                timestamp,
                origin_dt.format("%Y-%m-%d %T%z"),
                b,
                begin_dt.format("%Y-%m-%d %H:%M:%S%z"),
                e,
                end_dt.format("%Y-%m-%d %T%z")
            );
        }

        ts.get_decoder(0, 0, |_| {})
    }
}
