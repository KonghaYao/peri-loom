//! WAL image fixtures shared by the correctness tests and the paired scan benchmark.

use super::*;

pub(super) const PAGE_SIZE: usize = 512;
pub(super) const FRAME_SIZE: usize = PAGE_SIZE + 24;

/// Builds a little-endian WAL image frame by frame with the checksum chain
/// `committed_position` verifies, so a caller can produce both the image and the writes that
/// create it.
pub(super) struct WalBuilder {
    bytes: Vec<u8>,
    rolling: [u32; 2],
    salts: [u32; 2],
}

impl WalBuilder {
    pub(super) fn new(salts: [u32; 2]) -> Self {
        let mut bytes = vec![0u8; WAL_HEADER];
        bytes[0..4].copy_from_slice(&WAL_MAGIC_LITTLE_ENDIAN.to_be_bytes());
        bytes[4..8].copy_from_slice(&WAL_FORMAT.to_be_bytes());
        bytes[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_be_bytes());
        bytes[16..20].copy_from_slice(&salts[0].to_be_bytes());
        bytes[20..24].copy_from_slice(&salts[1].to_be_bytes());
        let rolling = checksum(&bytes[..24], [0, 0], false);
        bytes[24..28].copy_from_slice(&rolling[0].to_be_bytes());
        bytes[28..32].copy_from_slice(&rolling[1].to_be_bytes());
        Self {
            bytes,
            rolling,
            salts,
        }
    }

    /// The 32 header bytes the frames above extend.
    pub(super) fn header(&self) -> Vec<u8> {
        self.bytes.clone()
    }

    /// One frame as the engine writes it: page number, database size, salts and the rolling
    /// checksum of its payload. A zero database size is a frame that does not commit.
    pub(super) fn frame(&mut self, page: u32, db_size: u32) -> Vec<u8> {
        let mut frame = vec![0u8; FRAME_SIZE];
        frame[0..4].copy_from_slice(&page.to_be_bytes());
        frame[4..8].copy_from_slice(&db_size.to_be_bytes());
        frame[8..12].copy_from_slice(&self.salts[0].to_be_bytes());
        frame[12..16].copy_from_slice(&self.salts[1].to_be_bytes());
        for (index, byte) in frame[24..].iter_mut().enumerate() {
            *byte = (page as u8).wrapping_add(index as u8);
        }
        let rolling = checksum(
            &frame[24..],
            checksum(&frame[..8], self.rolling, false),
            false,
        );
        frame[16..20].copy_from_slice(&rolling[0].to_be_bytes());
        frame[20..24].copy_from_slice(&rolling[1].to_be_bytes());
        self.rolling = rolling;
        frame
    }

    /// A complete image of `frames` frames, the last one committing.
    pub(super) fn image(&mut self, frames: usize) -> Vec<u8> {
        let mut bytes = self.header();
        for page in 1..=frames {
            let commit = if page == frames { PAGE_SIZE as u32 } else { 0 };
            bytes.extend_from_slice(&self.frame(page as u32, commit));
        }
        bytes
    }
}

/// An image holding only the builder's header, which is the state the engine starts from
/// before it writes frames.
pub(super) fn started(builder: &WalBuilder) -> Result<WalImage> {
    let mut image = WalImage::default();
    let header = builder.header();
    image.apply(&write_at(0, &header))?;
    Ok(image)
}

pub(super) fn write_at(offset: usize, bytes: &[u8]) -> WalBatch {
    WalBatch {
        operations: vec![WalOperation::Write {
            offset: offset as u64,
            bytes: bytes.to_vec(),
        }],
    }
}

pub(super) fn truncate(size: usize) -> WalBatch {
    WalBatch {
        operations: vec![WalOperation::Truncate { size: size as u64 }],
    }
}
