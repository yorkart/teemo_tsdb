use crate::ts::TS;
use crate::{Engine, Raw};
use std::collections::BTreeMap;
use std::sync::mpsc::{Receiver, SyncSender};
use std::time::Duration;

pub type TSTreeMap = BTreeMap<String, TS>;

#[derive(Clone)]
pub(crate) struct BTreeEngine {
    ts_store: common::SharedRwLock<TSTreeMap>,
    background_task_tx: SyncSender<TS>,
}

impl BTreeEngine {
    pub(crate) fn new() -> Self {
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

    fn append_ts(&self, ts: &TS, raw: Raw) {
        ts.append_async(raw.data_point);
        //        info!("append raw: {}", raw.to_string());
    }
}

impl Engine for BTreeEngine {
    fn create_key(&self, raw: Raw) {
        let mut store = self.ts_store.write().unwrap();
        match store.get(&raw.key) {
            Some(ts) => {
                self.append_ts(ts, raw);
            }
            None => {
                let ts = TS::new(100000);
                self.background_task_tx.send(ts.clone()).unwrap();

                let key = raw.key.to_string();
                self.append_ts(&ts, raw);

                store.insert(key.to_string(), ts);
                info!("new key: {}", key);
            }
        }
    }

    fn append(&self, raw: Raw) {
        {
            let store = self.ts_store.read().unwrap();
            match store.get(&raw.key) {
                Some(ts) => {
                    self.append_ts(ts, raw);
                    return;
                }
                None => {}
            };
        }
        self.create_key(raw);
    }

    fn get(&self, _table_name: &String, key: &String) -> Option<TS> {
        let store = self.ts_store.read().unwrap();
        match store.get(key) {
            Some(ts) => Some(ts.clone()),
            None => None,
        }
    }
}
