//use crate::stream::{Error, Read, Write};
//use crate::Bit;

pub type Buffer = Vec<u8>;
//
//impl Buffer {
//    pub fn get_byte(&mut self, index: usize) -> Result<u8, Error> {
//        self.get(self.index).cloned().ok_or(Error::EOF)
//    }
//
//    pub fn grow(&mut self) {
//        self.push(0);
//    }
//
//    pub fn last_index(&self) -> usize {
//        self.len() - 1
//    }
//}
//
//impl Read for Buffer {
//    fn read_bit(&mut self) -> Result<Bit, Error> {
//        if self.r_pos == 8 {
//            self.r_index += 1;
//            self.r_pos = 0;
//        }
//
//        let byte = self.get_byte()?;
//
//        let bit = if byte & 1u8.wrapping_shl(7 - self.pos) == 0 {
//            Bit::Zero
//        } else {
//            Bit::One
//        };
//
//        self.r_pos += 1;
//
//        Ok(bit)
//    }
//
//    fn read_byte(&mut self) -> Result<u8, Error> {
//        if self.r_pos == 0 {
//            self.r_pos += 8;
//            return self.get_byte();
//        }
//
//        if self.pos == 8 {
//            self.r_index += 1;
//            return self.get_byte();
//        }
//
//        let mut byte = 0;
//        let mut b = self.get_byte()?;
//
//        byte |= b.wrapping_shl(self.pos);
//
//        self.r_index += 1;
//        b = self.get_byte()?;
//
//        byte |= b.wrapping_shr(8 - self.pos);
//
//        Ok(byte)
//    }
//
//    fn read_bits(&mut self, mut num: u32) -> Result<u64, Error> {
//        // can't read more than 64 bits into a u64
//        if num > 64 {
//            num = 64;
//        }
//
//        let mut bits: u64 = 0;
//        while num >= 8 {
//            let byte = self.read_byte().map(u64::from)?;
//            bits = bits.wrapping_shl(8) | byte;
//            num -= 8;
//        }
//
//        while num > 0 {
//            self.read_bit()
//                .map(|bit| bits = bits.wrapping_shl(1) | bit.to_u64())?;
//
//            num -= 1;
//        }
//
//        Ok(bits)
//    }
//
//    fn peak_bits(&mut self, num: u32) -> Result<u64, Error> {
//        // save the current index and pos so we can reset them after calling `read_bits`
//        let index = self.r_index;
//        let pos = self.r_pos;
//
//        let bits = self.read_bits(num)?;
//
//        self.r_index = index;
//        self.r_pos = pos;
//
//        Ok(bits)
//    }
//}
//
//impl Write for Buffer {
//    fn write_bit(&mut self, bit: Bit) {
//        if self.w_pos == 8 {
//            self.grow();
//            self.w_pos = 0;
//        }
//
//        let i = self.last_index();
//
//        match bit {
//            Bit::Zero => (),
//            Bit::One => self.buf[i] |= 1u8.wrapping_shl(7 - self.w_pos),
//        };
//
//        self.w_pos += 1;
//    }
//
//    fn write_byte(&mut self, byte: u8) {
//        if self.w_pos == 8 {
//            self.grow();
//
//            let i = self.last_index();
//            self.buf[i] = byte;
//            return;
//        }
//
//        let i = self.last_index();
//        let mut b = byte.wrapping_shr(self.w_pos) as u8;
//        self.buf[i] |= b;
//
//        self.grow();
//
//        b = byte.wrapping_shl(8 - self.w_pos) as u8;
//        self.buf[i + 1] |= b;
//    }
//
//    fn write_bits(&mut self, mut bits: u64, mut num: u32) {
//        // we should never write more than 64 bits for a u64
//        if num > 64 {
//            num = 64;
//        }
//
//        bits = bits.wrapping_shl(64 - num);
//        while num >= 8 {
//            let byte = bits.wrapping_shr(56);
//            self.write_byte(byte as u8);
//
//            bits = bits.wrapping_shl(8);
//            num -= 8;
//        }
//
//        while num > 0 {
//            let byte = bits.wrapping_shr(63);
//            if byte == 1 {
//                self.write_bit(Bit::One);
//            } else {
//                self.write_bit(Bit::Zero);
//            }
//
//            bits = bits.wrapping_shl(1);
//            num -= 1;
//        }
//    }
//
////    fn close(self) -> Box<[u8]> {
////        self.buf.into_boxed_slice()
////    }
//}
