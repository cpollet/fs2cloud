use std::io;
use std::io::Read;

pub struct ChunkReader<'t, R: Read> {
    reader: &'t mut R,
    bytes_left: usize,
}
impl<'t, R: Read> ChunkReader<'t, R> {
    pub fn new(reader: &'t mut R, bytes_left: usize) -> ChunkReader<R> {
        ChunkReader { reader, bytes_left }
    }

    pub fn read_full_chunk(&self) -> bool {
        self.bytes_left == 0
    }
}

impl<R: Read> Read for ChunkReader<'_, R> {
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
