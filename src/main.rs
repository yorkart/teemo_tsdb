extern crate tsz;

use tsz::storage::new_btree_engine;
use tsz::Decode;
use std::borrow::Borrow;

fn main() {
    let engine = new_btree_engine();
    engine.create_table(String::from("table_name"));

    net::serve(engine.clone());

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
