use tszv1::stream::{BufferedReader, BufferedWriter};
use tszv1::Buffer;
use tszv1::{buffer_into_vec, DataPoint, Decode, Encode, StdDecoder, StdEncoder};

pub trait Block {
    //    fn get_decoder(&self) -> StdDecoder<BufferedReader>;
    fn read<F>(&self, f: F)
    where
        F: Fn(DataPoint);
}

#[derive(Debug)]
pub struct AppendOnlyBlock {
    pub time_begin: u64,
    pub time_end: u64,

    pub encoder: StdEncoder<BufferedWriter>,
}

impl AppendOnlyBlock {
    pub fn new(time_begin: u64, time_end: u64) -> Self {
        let writer = BufferedWriter::new();
        let encoder = StdEncoder::new(time_begin, writer);

        AppendOnlyBlock {
            time_begin,
            time_end,
            encoder,
        }
    }

    pub fn get_buffer(&self) -> Box<[u8]> {
        self.encoder.clone().close()
    }

    pub fn get_decoder(&self) -> StdDecoder<BufferedReader> {
        let reader = BufferedReader::new(self.encoder.clone().close());
        StdDecoder::new(reader)
    }
}

impl Block for AppendOnlyBlock {
    // TODO clone always
    //    fn get_decoder(&self) -> StdDecoder<BufferedReader> {
    //        let reader = BufferedReader::new(self.encoder.clone().close());
    //        StdDecoder::new(reader)
    //    }

    fn read<F>(&self, f: F)
    where
        F: Fn(DataPoint),
    {
        let mut decoder = self.get_decoder();
        loop {
            match decoder.next() {
                Ok(dp) => {
                    f(dp);
                }
                Err(_) => {
                    break;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ClosedBlock {
    time_begin: u64,
    time_end: u64,
    bytes: Buffer, // Arc<Buffer>,
}

impl ClosedBlock {
    pub fn new(append_only_block: &AppendOnlyBlock) -> Self {
        let bytes = buffer_into_vec(append_only_block.get_buffer());
        //        let bytes = Arc::new(bytes);
        ClosedBlock {
            time_begin: append_only_block.time_begin,
            time_end: append_only_block.time_end,
            bytes,
        }
    }

    // TODO clone always
    pub fn get_decoder(&self) -> StdDecoder<BufferedReader> {
        let reader = BufferedReader::new_buffer(self.bytes.clone());
        StdDecoder::new(reader)
    }
}

impl Block for ClosedBlock {
    fn read<F>(&self, f: F)
    where
        F: Fn(DataPoint),
    {
        let mut decoder = self.get_decoder();
        loop {
            match decoder.next() {
                Ok(dp) => {
                    f(dp);
                }
                Err(_) => {
                    break;
                }
            }
        }
    }
}
