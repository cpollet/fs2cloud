use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub struct ChunkedSha256 {
    next_block: u64,
    hasher: Sha256,
    pending_blocks: HashMap<u64, Vec<u8>>,
}

impl ChunkedSha256 {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, data: &[u8], block_index: u64) {
        if block_index > self.next_block {
            self.pending_blocks.insert(block_index, Vec::from(data));
        } else {
            self.hasher.update(data);
            self.next_block += 1;
            while let Some(block) = self.pending_blocks.remove(&self.next_block) {
                self.hasher.update(block.as_slice());
                self.next_block += 1;
            }
        }
    }

    pub fn finalize(&mut self) -> Option<String> {
        if self.pending_blocks.is_empty() {
            Some(format!("{:x}", self.hasher.finalize_reset()))
        } else {
            None
        }
    }
}

impl Default for ChunkedSha256 {
    fn default() -> Self {
        Self {
            next_block: 0,
            hasher: Sha256::new(),
            pending_blocks: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_order() {
        let mut hasher = ChunkedSha256::new();
        let blocks = vec![(0, "hello".as_bytes()), (1, "world".as_bytes())];
        for block in blocks {
            hasher.update(block.1, block.0);
        }
        let hash = hasher.finalize().unwrap();
        assert_eq!(
            hash,
            "936a185caaa266bb9cbe981e9e05cb78cd732b0b3280eb944412bb6f8f8f07af"
        );
    }

    #[test]
    fn out_of_order() {
        let mut hasher = ChunkedSha256::new();
        let blocks = vec![(1, "world".as_bytes()), (0u64, "hello".as_bytes())];
        for block in blocks {
            hasher.update(block.1, block.0);
        }
        let hash = hasher.finalize().unwrap();
        assert_eq!(
            hash,
            "936a185caaa266bb9cbe981e9e05cb78cd732b0b3280eb944412bb6f8f8f07af"
        );
    }

    #[test]
    fn not_finished() {
        let mut hasher = ChunkedSha256::new();
        let blocks = vec![(1, "world".as_bytes())];
        for block in blocks {
            hasher.update(block.1, block.0);
        }
        let hash = hasher.finalize();
        assert!(hash.is_none());
    }

    #[test]
    fn reset_after_finalize() {
        let mut hasher = ChunkedSha256::new();

        let blocks = vec![(0, "random string".as_bytes())];
        for block in blocks {
            hasher.update(block.1, block.0);
        }
        let hash = hasher.finalize();
        assert!(hash.is_some());

        let blocks = vec![(0, "hello".as_bytes()), (1, "world".as_bytes())];
        for block in blocks {
            hasher.update(block.1, block.0);
        }
        let hash = hasher.finalize().unwrap();
        assert_eq!(
            hash,
            "936a185caaa266bb9cbe981e9e05cb78cd732b0b3280eb944412bb6f8f8f07af"
        );
    }
}
