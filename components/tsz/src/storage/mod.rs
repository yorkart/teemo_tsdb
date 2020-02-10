mod block;
mod ts;
mod engine;

pub use engine::BTreeEngine;
use std::time::Duration;
use crate::DataPoint;

pub struct Raw {
    pub table_name: String,
    pub dp: DataPoint,
}

pub fn new_btree_engine() -> BTreeEngine {
    let (data_tx, data_rx) = std::sync::mpsc::sync_channel(100000);
    let (bg_tx, bg_rx) = std::sync::mpsc::sync_channel(10);

    let engine = BTreeEngine::new(data_tx, bg_tx);

    // background thread
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

    // sequence write thread
    let clone = engine.clone();
    std::thread::spawn(move || {
        loop {
            match data_rx.try_recv() {
                Ok(raw) => {
                    clone.append(raw);
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_secs(100));
                }
            }
        }
    });


    engine
}