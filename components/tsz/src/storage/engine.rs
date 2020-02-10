use std::collections::BTreeMap;
use crate::DataPoint;
use crate::storage::ts::TS;
use std::sync::mpsc::SyncSender;

pub type TSTreeMap = BTreeMap<String, TS>;

#[derive(Clone)]
pub struct BTreeEngine {
    name: String,
    ts_store: common::SharedRwLock<TSTreeMap>,
    //    timer: SharedRwLock<timer::Timer>,
    data_channel_tx: SyncSender<DataPoint>,
    background_task_tx: SyncSender<TS>,
}

impl BTreeEngine {
    pub fn new(name: String, data_channel_tx: SyncSender<DataPoint>, background_task_tx: SyncSender<TS>) -> Self {
        BTreeEngine {
            name,
            ts_store: common::new_shared_rw_lock(BTreeMap::new()),
//            timer: new_shared_rw_lock(timer::Timer::new()),
            data_channel_tx,
            background_task_tx,
        }
    }

    // todo by ts_name write
    pub fn append_async(&self, ts_name: &String, dp: DataPoint) {
        self.data_channel_tx.send(dp).unwrap();
    }

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
                    self.background_task_tx.send(ts.clone()).unwrap();
//                    let guard = {
//                        let ts_clone = ts.clone();
//                        self.timer.read().unwrap().schedule_repeating(chrono::Duration::minutes(1), move || {
//                            ts_clone.roll_down(1000 * 60 * 60);
//                        })
//                    };
//
//                    ts.set_timer_guard(guard);

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
            }
            None => {
                None
            }
        }
    }
}

//impl Default for BTreeEngine {
//    fn default() -> Self {
//        Self::new()
//    }
//}
