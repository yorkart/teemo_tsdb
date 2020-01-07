
extern crate tsz;

use tsz::stream::{BufferedWriter, BufferedReader};
use tsz::{StdDecoder, StdEncoder, DataPoint, Encode, Decode};
use std::sync::{Arc, RwLock};
use std::ops::{DerefMut, Deref};

fn main() {
    let bytes: Vec<u8> = Vec::new();
    let rw = RwLock::new(bytes);
    let bytes_rw = Arc::new(rw);

    // writer
    {
        let clone = bytes_rw.clone();
        std::thread::spawn(move || {
            let mut a = clone.write().unwrap();
            let a = a.deref_mut();
            a.push(1);

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
        let rw = bytes_rw.clone();
        threads.push(std::thread::spawn(move || {
            let a = rw.read().unwrap();
            let a = a.deref();
            if a.len() > 0 {
                let n = a.get(0).expect("abc");
                println!("{} => {}", num, n);
            } else {
                println!("{} => empty", num);
            }

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