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

//                let writer = BufferedWriter::new(bytes.as_mut());
//                let start_time = 1482268055; // 2016-12-20T21:07:35+00:00
//                let mut encoder = StdEncoder::new(start_time, writer);
//
//                let d1 = DataPoint::new(1482268055 + 10, 1.24);
//                let d2 = DataPoint::new(1482268055 + 20, 1.98);
//                let d3 = DataPoint::new(1482268055 + 32, 2.37);
//                let d4 = DataPoint::new(1482268055 + 44, -7.41);
//                let d5 = DataPoint::new(1482268055 + 52, 103.50);
//
//                encoder.encode(d1);
//                encoder.encode(d2);
//                encoder.encode(d3);
//                encoder.encode(d4);
//                encoder.encode(d5);
//
//                encoder.close();
        });
    };

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
//            if a.len() > 0 {
//                let n = a.get(0).expect("abc");
//                println!("{} => {}", num, n);
//            } else {
//                println!("{} => empty", num);
//            }

//                let reader = BufferedReader::new(bytes_clone.as_ref());
//                let mut decoder = StdDecoder::new(reader);
//                let dp = decoder.next().unwrap();
//                println!("{}: {},{}", num, dp.time, dp.value);
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }
}