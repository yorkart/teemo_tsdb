extern crate tsz;

use std::sync::{Arc, RwLock};
use std::ops::DerefMut;
use tsz::storage::mut_mem::TSMap;
use tsz::{DataPoint, Decode};
use std::borrow::Borrow;

fn main() {
    let ts_map = Arc::new(RwLock::new(TSMap::new()));

    // writer
    {
        let clone = ts_map.clone();
        std::thread::spawn(move || {
            let mut map = clone.write().unwrap();
            let map = map.deref_mut();

            let d1 = DataPoint::new(1482268055 + 10, 1.24);
            map.append(String::from("abc").borrow(), d1);
        });
    };

    // reader
    let mut threads = Vec::new();
    for num in 0..10 {
        let clone = ts_map.clone();
        threads.push(std::thread::spawn(move || {
            let map = clone.read().unwrap();
            match map.get(String::from("abc").borrow()) {
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