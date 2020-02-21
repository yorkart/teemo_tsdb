use serde::export::fmt::{Debug, Error};
use serde::export::Formatter;
use std::borrow::BorrowMut;
use std::collections::linked_list::Iter;
use std::collections::LinkedList;
use std::ops::{Index, IndexMut};

pub struct Block {
    pub data: Vec<u8>,
    pub start_index: usize,
    pub end_index: usize,
}

impl Block {
    fn new(capacity: usize, start_index: usize) -> Self {
        Block {
            data: Vec::with_capacity(capacity),
            start_index,
            end_index: start_index + capacity,
        }
    }

    fn with_array(array: Box<[u8]>, start_index: usize) -> Self {
        let len = array.len();
        let data = array.into_vec();
        Block {
            data,
            start_index,
            end_index: start_index + len,
        }
    }
}

impl Clone for Block {
    fn clone(&self) -> Self {
        Block {
            data: self.data.clone(),
            start_index: self.start_index,
            end_index: self.end_index,
        }
    }
}

pub struct Buffer {
    blocks: LinkedList<Block>,
    init_capacity: usize,
    incr_factor: f32,
    len: usize,
}

impl Buffer {
    pub fn new(init_capacity: usize, incr_factor: f32) -> Self {
        Buffer {
            blocks: LinkedList::new(),
            init_capacity,
            incr_factor,
            len: 0,
        }
    }

    pub fn with_array(array: Box<[u8]>, incr_factor: f32) -> Self {
        let len = array.len();
        let mut blocks = LinkedList::new();
        blocks.push_back(Block::with_array(array, 0));

        Buffer {
            blocks,
            init_capacity: len,
            incr_factor,
            len,
        }
    }

    //    #[inline]
    //    fn push_value(&mut self, block: &mut Block, value: u8) {
    //        block.data.push(value);
    //        self.len = self.len + 1;
    //    }

    fn push_with_incr_block(&mut self, first_value: u8) {
        let latest_capacity = if self.len == 0 {
            self.init_capacity
        } else {
            self.len
        };

        let mut incr_capacity = (latest_capacity as f32 * self.incr_factor) as usize;
        if incr_capacity < 1024 {
            incr_capacity = 1024;
        } else if incr_capacity > 10 * 1024 * 1024 {
            incr_capacity = 10 * 1024 * 1024;
        }

        let mut block = Block::new(incr_capacity, self.len);
        //        println!("new block start:{}, end:{}", block.start_index, block.end_index);
        block.data.push(first_value);

        self.blocks.push_back(block);
    }

    pub fn push(&mut self, value: u8) {
        match self.blocks.back_mut() {
            Some(block) => {
                if block.data.len() == block.data.capacity() {
                    self.push_with_incr_block(value);
                } else {
                    block.data.push(value);
                }
            }
            None => {
                self.push_with_incr_block(value);
            }
        }
        self.len = self.len + 1;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, index: usize) -> Option<&u8> {
        if index >= self.len {
            return None;
        }

        // try tail cell search prior
        match self.blocks.back() {
            Some(block) => {
                if index >= block.start_index {
                    return if index < block.end_index {
                        block.data.get(index - block.start_index)
                    } else {
                        None
                    };
                }
            }
            None => {}
        }

        // walk linked list
        let mut iter = self.blocks.iter();
        loop {
            match iter.next() {
                Some(block) => {
                    if index >= block.start_index && index < block.end_index {
                        return block.data.get(index - block.start_index);
                    }
                }
                None => {
                    return None;
                }
            }
        }
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut u8> {
        if index >= self.len {
            return None;
        }

        // walk linked list
        let mut iter = self.blocks.iter_mut();
        loop {
            match iter.next_back() {
                Some(block) => {
                    if index >= block.start_index && index < block.end_index {
                        return block.data.get_mut(index - block.start_index);
                    }
                }
                None => {
                    return None;
                }
            }
        }
    }

    pub fn into_boxed_slice(mut self) -> Box<[u8]> {
        let mut vec = Vec::with_capacity(self.len);
        loop {
            match self.blocks.pop_front() {
                Some(mut block) => vec.append(block.data.borrow_mut()),
                None => break,
            }
        }

        vec.into_boxed_slice()
    }

    pub fn iter(&self) -> BufferIter {
        BufferIter::new(self.blocks.iter())
    }
}

impl Index<usize> for Buffer {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).unwrap()
    }
}

impl IndexMut<usize> for Buffer {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index).unwrap()
        //        let n = self.get(index).as_mut();
        //        IndexMut::index_mut(&mut **self, index)
    }
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.debug_tuple("Iter").field(&self.len).finish()
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Buffer::new(10, 0.2)
    }
}

impl Clone for Buffer {
    fn clone(&self) -> Self {
        let mut blocks = LinkedList::new();
        let mut iter = self.blocks.iter();
        loop {
            match iter.next() {
                Some(block) => blocks.push_back(block.clone()),
                None => break,
            }
        }

        Buffer {
            blocks,
            init_capacity: self.init_capacity,
            incr_factor: self.incr_factor,
            len: self.len,
        }
    }
}

pub struct BufferIter<'a> {
    inner: Iter<'a, Block>,
    block: Option<&'a Block>,
    walked: usize,
}

impl<'a> BufferIter<'a> {
    fn new(inner: Iter<'a, Block>) -> Self {
        BufferIter {
            inner,
            block: None,
            walked: 0,
        }
    }
}

impl<'a> Iterator for BufferIter<'a> {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        match self.block {
            Some(block) => {
                if self.walked < block.end_index {
                    let n = block.data.get(self.walked - block.start_index);
                    self.walked = self.walked + 1;
                    return n.map(|value| *value);
                }
            }
            None => {}
        }
        return match self.inner.next() {
            Some(block) => {
                self.block = Some(block);
                let n = block.data.get(0);
                self.walked = self.walked + 1;
                n.map(|value| *value)
            }
            None => None,
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::Buffer;
    use rand::prelude::*;

    #[test]
    fn buffer_test() {
        let size = 10240;
        let mut vec = Vec::with_capacity(size);
        for _i in 0..size {
            let mut n = random::<u8>();
            if n == 255 {
                n = 254;
            }
            vec.push(n);
        }

        let mut buffer = Buffer::new(5, 0.2);
        for i in 0..size {
            buffer.push(vec[i]);
        }

        println!("validate by Index");
        for i in 0..size {
            assert_eq!(buffer[i], vec[i]);
        }

        println!("validate by Iterator");
        let mut i = 0;
        let mut iter = buffer.iter();
        loop {
            match iter.next() {
                Some(value) => {
                    assert_eq!(value, vec[i]);
                    //                    println!("{} => {}", i, value);
                    i = i + 1;
                }
                None => {
                    break;
                }
            }
        }

        println!("validate by get_mut");
        for i in 0..size {
            buffer[i] += 1;
        }
        for i in 0..size {
            assert_eq!(buffer[i] - 1, vec[i]);
            buffer[i] -= 1
        }

        println!("validate by into_boxed_slice");
        let slice = buffer.into_boxed_slice();
        for i in 0..size {
            assert_eq!(slice[i], vec[i]);
        }

        println!("end");
    }
}
