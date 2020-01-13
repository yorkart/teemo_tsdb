use crate::stream::{BufferedWriter, BufferedReader};
use crate::{StdEncoder, StdDecoder};
use crate::stream::Buffer;

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

    pub fn from(&mut self, other: AppendOnlyBlock) {
        self.time_begin = other.time_begin;
        self.time_end = other.time_end;
        self.encoder = other.encoder;
    }

    pub fn get_decoder(&self) -> StdDecoder<BufferedReader> {
        let reader = BufferedReader::new(self.encoder.get_buffer());
        StdDecoder::new(reader)
    }

    pub fn len(&self) -> u64 {
        self.encoder.get_size()
    }

    pub fn get_buffer(&self) -> &Buffer {
        self.encoder.get_buffer()
    }
}

#[derive(Debug)]
pub struct ClosedBlock {
    time_begin: u64,
    time_end: u64,
    bytes: Buffer,
}

impl ClosedBlock {
    pub fn new(append_only_block: &AppendOnlyBlock) -> Self {
        let bytes = append_only_block.get_buffer().to_vec();
        ClosedBlock {
            time_begin: append_only_block.time_begin,
            time_end: append_only_block.time_end,
            bytes,
        }
    }

    pub fn get_decoder(&self) -> StdDecoder<BufferedReader> {
        let reader = BufferedReader::new(self.bytes.as_ref());
        StdDecoder::new(reader)
    }
}