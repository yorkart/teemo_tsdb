#[derive(Debug, Default)]
pub struct MutMemory {
    buffer: Vec<u8>,
}

impl MutMemory {
    //    pub fn new(capacity: usize) -> Self {
    pub fn new() -> Self {
        MutMemory {
            buffer: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

pub struct TsStorage {

}
