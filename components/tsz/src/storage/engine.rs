use std::collections::BTreeMap;
use crate::storage::ts::TS;
use std::sync::mpsc::{SyncSender, Receiver};
use std::time::Duration;
use crate::DataPoint;

pub struct Raw {
    pub table_name: String,
    pub dp: DataPoint,
}

pub type TSTreeMap = BTreeMap<String, TS>;

#[derive(Clone)]
pub struct BTreeEngine {
    ts_store: common::SharedRwLock<TSTreeMap>,
    background_task_tx: SyncSender<TS>,
}

impl BTreeEngine {
    pub fn new() -> Self {
        let (bg_tx, bg_rx) = std::sync::mpsc::sync_channel(10);
        let engine = BTreeEngine {
            ts_store: common::new_shared_rw_lock(BTreeMap::new()),
            background_task_tx: bg_tx,
        };
        engine.background_task(bg_rx);
        engine
    }

    fn background_task(&self, bg_rx: Receiver<TS>) {
        std::thread::spawn(move || {
            let mut sources = Vec::new();
            loop {
                std::thread::sleep(Duration::from_secs(60));

                match bg_rx.try_recv() {
                    Ok(ts) => {
                        &sources.push(ts);
                    }
                    Err(_) => {}
                }

                for ts in &sources {
                    ts.roll_down(100u64);
                }
            }
        });
    }

    pub fn create_table(&self, ts_name: String) {
        let mut store = self.ts_store.write().unwrap();
        match store.get(ts_name.as_str()) {
            Some(_) => {}
            None => {
                let ts = TS::new();
                self.background_task_tx.send(ts.clone()).unwrap();
                store.insert(ts_name.to_string(), ts);
            }
        }
    }

    pub fn append(&self, raw: Raw) {
        let store = self.ts_store.read().unwrap();
        match store.get(&raw.table_name) {
            Some(ts) => {
                ts.append_async(raw.dp);
            }
            None => {}
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
