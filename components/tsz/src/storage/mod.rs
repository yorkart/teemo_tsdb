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
    let (bg_tx, bg_rx) = std::sync::mpsc::sync_channel(10);

    let engine = BTreeEngine::new(bg_tx);

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

    engine
}