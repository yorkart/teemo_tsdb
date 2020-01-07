//! A crate for time series compression based upon Facebook's white paper
//! [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
//! `tsz` provides functionality for compressing a stream of `DataPoint`s, which are composed of a
//! time and value, into bytes, and decompressing a stream of bytes into `DataPoint`s.

/// Bit
///
/// An enum used to represent a single bit, can be either `Zero` or `One`.
#[derive(Debug, PartialEq)]
pub enum Bit {
    Zero,
    One,
}

impl Bit {
    /// Convert a bit to u64, so `Zero` becomes 0 and `One` becomes 1.
    pub fn to_u64(&self) -> u64 {
        match self {
            Bit::Zero => 0,
            Bit::One => 1,
        }
    }
}

/// DataPoint
///
/// Struct used to represent a single data point. Consists of a time and value.
#[derive(Debug, PartialEq, Copy)]
pub struct DataPoint {
    time: u64,
    value: f64,
}

impl Clone for DataPoint {
    fn clone(&self) -> DataPoint {
        *self
    }
}

impl DataPoint {
    // Create a new DataPoint from a time and value.
    pub fn new(time: u64, value: f64) -> Self {
        DataPoint { time, value }
    }
}

pub mod storage;

pub mod stream;

pub mod encode;

pub use self::encode::std_encoder::StdEncoder;
pub use self::encode::Encode;

pub mod decode;

pub use self::decode::std_decoder::StdDecoder;
pub use self::decode::Decode;

#[cfg(test)]
mod tests {
    use crate::stream::{BufferedWriter, BufferedReader};
    use crate::{StdDecoder, StdEncoder, DataPoint, Encode, Decode};
    use std::ops::Deref;

    #[test]
    fn serde_test() {
        let mut bytes = Vec::new();
        let bytes_arc = std::sync::Arc::new(bytes);

        // writer
        let bytes_writer_clone = bytes_arc.clone();
        std::thread::spawn(move || {
            let start_time = 1482268055; // 2016-12-20T21:07:35+00:00
            let a = bytes_writer_clone.deref();
            let writer = BufferedWriter::new(a);
            let mut encoder = StdEncoder::new(start_time, writer);

            let d1 = DataPoint::new(1482268055 + 10, 1.24);
            let d2 = DataPoint::new(1482268055 + 20, 1.98);
            let d3 = DataPoint::new(1482268055 + 32, 2.37);
            let d4 = DataPoint::new(1482268055 + 44, -7.41);
            let d5 = DataPoint::new(1482268055 + 52, 103.50);

            encoder.encode(d1);
            encoder.encode(d2);
            encoder.encode(d3);
            encoder.encode(d4);
            encoder.encode(d5);

            encoder.close();
        });

        let mut threads = Vec::new();
        for num in 0..10 {
            let bytes_clone = bytes_arc.clone();
            threads.push(std::thread::spawn(move || {
                let reader = BufferedReader::new(bytes_clone.as_ref());
                let mut decoder = StdDecoder::new(reader);
                let dp = decoder.next().unwrap();
                println!("{}: {},{}", num, dp.time, dp.value);
            }));
        }


        for thread in threads {
            thread.join().unwrap();
        }
    }
}
