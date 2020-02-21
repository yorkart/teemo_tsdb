//! A crate for time series compression based upon Facebook's white paper
//! [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
//! `tsz` provides functionality for compressing a stream of `DataPoint`s, which are composed of a
//! time and value, into bytes, and decompressing a stream of bytes into `DataPoint`s.
//!
//! ## Example
//!
//! Below is a simple example of how to interact with `tsz` to encode and decode `DataPoint`s.
//!
//! ```rust,no_run
//! extern crate tszv1;
//!
//! use std::vec::Vec;
//! use tszv1::{DataPoint, Encode, Decode, StdEncoder, StdDecoder};
//! use tszv1::stream::{BufferedReader, BufferedWriter};
//! use tszv1::decode::Error;
//!
//! const DATA: &'static str = "1482892270,1.76
//! 1482892280,7.78
//! 1482892288,7.95
//! 1482892292,5.53
//! 1482892310,4.41
//! 1482892323,5.30
//! 1482892334,5.30
//! 1482892341,2.92
//! 1482892350,0.73
//! 1482892360,-1.33
//! 1482892370,-1.78
//! 1482892390,-12.45
//! 1482892401,-34.76
//! 1482892490,78.9
//! 1482892500,335.67
//! 1482892800,12908.12
//! ";
//!
//! fn main() {
//!     let w = BufferedWriter::new();
//!
//!     // 1482892260 is the Unix timestamp of the start of the stream
//!     let mut encoder = StdEncoder::new(1482892260, w);
//!
//!     let mut actual_datapoints = Vec::new();
//!
//!     for line in DATA.lines() {
//!         let substrings: Vec<&str> = line.split(",").collect();
//!         let t = substrings[0].parse::<u64>().unwrap();
//!         let v = substrings[1].parse::<f64>().unwrap();
//!         let dp = DataPoint::new(t, v);
//!         actual_datapoints.push(dp);
//!     }
//!
//!     for dp in &actual_datapoints {
//!         encoder.encode(*dp);
//!     }
//!
//!     let bytes = encoder.close();
//!     let r = BufferedReader::new(bytes);
//!     let mut decoder = StdDecoder::new(r);
//!
//!     let mut expected_datapoints = Vec::new();
//!
//!     let mut done = false;
//!     loop {
//!         if done {
//!             break;
//!         }
//!
//!         match decoder.next() {
//!             Ok(dp) => expected_datapoints.push(dp),
//!             Err(err) => {
//!                 if err == Error::EndOfStream {
//!                     done = true;
//!                 } else {
//!                     panic!("Received an error from decoder: {:?}", err);
//!                 }
//!             }
//!         };
//!     }
//!
//!     println!("actual datapoints: {:?}", actual_datapoints);
//!     println!("expected datapoints: {:?}", expected_datapoints);
//! }
//! ```

#[macro_use]
extern crate serde_derive;

//pub type Buffer = Vec<u8>;
pub type Buffer = buffer::Buffer;

pub fn buffer_new() -> Buffer {
    //    Buffer::new()
    Buffer::new(1024, 0.2)
}

pub fn buffer_with_capacity(capacity: usize) -> Buffer {
    //    Buffer::with_capacity(capacity)
    Buffer::new(capacity, 0.2)
}

pub fn buffer_into_vec(bytes: Box<[u8]>) -> Buffer {
    // bytes.into_vec()
    Buffer::with_array(bytes, 0.2)
}

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
/// Struct used to represent a single datapoint. Consists of a time and value.
#[derive(Debug, PartialEq, Copy, Serialize, Deserialize)]
pub struct DataPoint {
    pub time: u64,
    pub value: f64,
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

pub mod buffer;

pub mod stream;

pub mod encode;
pub use self::encode::std_encoder::StdEncoder;
pub use self::encode::Encode;

pub mod decode;
pub use self::decode::std_decoder::StdDecoder;
pub use self::decode::Decode;

#[cfg(test)]
mod tests {
    use std::vec::Vec;

    use super::decode::Error;
    use super::stream::{BufferedReader, BufferedWriter};
    use super::{DataPoint, Decode, Encode, StdDecoder, StdEncoder};

    const DATA: &'static str = "1482892270,1.76
1482892280,7.78
1482892288,7.95
1482892292,5.53
1482892310,4.41
1482892323,5.30
1482892334,5.30
1482892341,2.92
1482892350,0.73
1482892360,-1.33
1482892370,-1.78
1482892390,-12.45
1482892401,-34.76
1482892490,78.9
1482892500,335.67
1482892800,12908.12
";

    #[test]
    fn integration_test() {
        let w = BufferedWriter::new();
        let mut encoder = StdEncoder::new(1482892260, w);

        let mut original_datapoints = Vec::new();

        for line in DATA.lines() {
            let substrings: Vec<&str> = line.split(",").collect();
            let t = substrings[0].parse::<u64>().unwrap();
            let v = substrings[1].parse::<f64>().unwrap();
            let dp = DataPoint::new(t, v);
            original_datapoints.push(dp);
        }

        for dp in &original_datapoints {
            encoder.encode(*dp);
        }

        let bytes = encoder.close();
        let r = BufferedReader::new(bytes);
        let mut decoder = StdDecoder::new(r);

        let mut new_datapoints = Vec::new();

        let mut done = false;
        loop {
            if done {
                break;
            }

            match decoder.next() {
                Ok(dp) => new_datapoints.push(dp),
                Err(err) => {
                    if err == Error::EndOfStream {
                        done = true;
                    } else {
                        panic!("Received an error from decoder: {:?}", err);
                    }
                }
            };
        }

        assert_eq!(original_datapoints, new_datapoints);
    }
}
