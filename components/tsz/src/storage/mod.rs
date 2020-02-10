mod block;
mod ts;
mod engine;

pub use engine::BTreeEngine;
use std::time::Duration;
use std::borrow::Borrow;

pub fn new_btree_engine(name: String) -> BTreeEngine {
    let (data_tx, data_rx) = std::sync::mpsc::sync_channel(100000);
    let (bg_tx, bg_rx) = std::sync::mpsc::sync_channel(10);

    let engine = BTreeEngine::new(name, data_tx, bg_tx);

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
                Ok(dp) => {
                    clone.append(String::from("table_name").borrow(), dp);
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_secs(100));
                }
            }
        }
    });


    engine
}