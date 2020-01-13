extern crate tsz;

use std::sync::mpsc;
use tsz::storage::mut_mem::BTreeEngine;
use tsz::{DataPoint, Decode};
use std::borrow::{Borrow};
use std::time::Duration;
use std::sync::mpsc::{SyncSender, Receiver};

fn main() {
    let engine = BTreeEngine::new();
    let (tx, rx):(SyncSender<DataPoint>, Receiver<DataPoint>)= mpsc::sync_channel(1000);

    net::serve(engine.clone(), tx.clone());

    // writer
    {
        let clone = engine.clone();
        std::thread::spawn(move || {
            loop {
                match rx.try_recv() {
                    Ok(dp) => {
                        clone.append(String::from("abc").borrow(), dp);
                    }
                    Err(_) => {
                        std::thread::sleep(Duration::from_secs(100));
                    }
                }
            }

//            let d1 = DataPoint::new(1482268055 + 10, 1.24);
//            let mut clone = clone.write().unwrap();
//            clone.append(String::from("abc").borrow(), d1);
        });
    };

    // reader
    let mut threads = Vec::new();
    for num in 0..10 {
        let clone = engine.clone();
        threads.push(std::thread::spawn(move || {
            match clone.get(String::from("abc").borrow()) {
                Some(ts) => {
                    ts.get_decoder(0, 0, |mut decoder| {
                        loop {
                            match decoder.next() {
                                Ok(dp) => {
                                    println!("reader{} => {}, {}", num, dp.time, dp.value);
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                    });
                }
                None => {}
            }
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
