use std::sync::{RwLock, Arc};
use std::collections::BTreeMap;
use crate::DataPoint;
use crate::storage::ts::TS;

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
