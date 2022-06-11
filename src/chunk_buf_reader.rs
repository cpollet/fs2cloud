use std::io;
use std::io::{BufReader, Read};

pub struct ChunkBufReader<'t, R> {
    reader: &'t mut BufReader<R>,
    bytes_left: usize,
}
impl<'t, R: Read> ChunkBufReader<'t, R> {
    pub fn new(reader: &'t mut BufReader<R>, bytes_left: usize) -> ChunkBufReader<R> {
        ChunkBufReader { reader, bytes_left }
    }
}
impl<R: Read> Read for ChunkBufReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_to_read = buf.len().min(self.bytes_left);
        let mut inner_buf = vec![0; bytes_to_read];
        match self.reader.read(&mut inner_buf) {
            Ok(size) => {
                self.bytes_left -= size;
                buf[..bytes_to_read].copy_from_slice(&inner_buf);
                Ok(size)
            }
            e => e,
        }
    }
}
