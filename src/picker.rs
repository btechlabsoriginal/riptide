use crate::torrent::Torrent;
use anyhow::{bail, ensure, Result};
use std::time::Instant;

pub const BLOCK_SIZE: u32 = 16 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BlockRequest {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

#[derive(Debug)]
pub struct FinishedPiece {
    pub index: u32,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
enum Block {
    Missing,
    Requested { peer: usize, since: Instant },
    Received(Vec<u8>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Collecting,
    Writing,
    Verified,
}

#[derive(Debug)]
struct Piece {
    length: u32,
    availability: u32,
    blocks: Vec<Block>,
    phase: Phase,
}

#[derive(Debug)]
pub struct PiecePicker {
    pieces: Vec<Piece>,
    verified_bytes: u64,
    verified_count: usize,
}

impl PiecePicker {
    pub fn new(torrent: &Torrent) -> Self {
        let pieces = (0..torrent.piece_hashes.len())
            .map(|index| {
                let length = torrent.piece_size(index).expect("valid torrent") as u32;
                let blocks = (0..length.div_ceil(BLOCK_SIZE))
                    .map(|_| Block::Missing)
                    .collect();
                Piece {
                    length,
                    availability: 0,
                    blocks,
                    phase: Phase::Collecting,
                }
            })
            .collect();
        Self {
            pieces,
            verified_bytes: 0,
            verified_count: 0,
        }
    }

    pub fn piece_count(&self) -> usize {
        self.pieces.len()
    }

    pub fn verified_count(&self) -> usize {
        self.verified_count
    }

    pub fn verified_bytes(&self) -> u64 {
        self.verified_bytes
    }

    pub fn complete(&self) -> bool {
        self.verified_count == self.pieces.len()
    }

    pub fn is_verified(&self, index: usize) -> bool {
        self.pieces
            .get(index)
            .is_some_and(|piece| piece.phase == Phase::Verified)
    }

    pub fn note_available(&mut self, index: usize) {
        if let Some(piece) = self.pieces.get_mut(index) {
            piece.availability = piece.availability.saturating_add(1);
        }
    }

    pub fn note_unavailable(&mut self, index: usize) {
        if let Some(piece) = self.pieces.get_mut(index) {
            piece.availability = piece.availability.saturating_sub(1);
        }
    }

    pub fn claim(&mut self, peer: usize, has: &[bool]) -> Option<BlockRequest> {
        let (index, _) = self
            .pieces
            .iter()
            .enumerate()
            .filter(|(index, piece)| {
                has.get(*index).copied().unwrap_or(false)
                    && piece.phase == Phase::Collecting
                    && piece.blocks.iter().any(|block| matches!(block, Block::Missing))
            })
            .min_by_key(|(index, piece)| {
                let begun = piece
                    .blocks
                    .iter()
                    .any(|block| !matches!(block, Block::Missing));
                (piece.availability, !begun, *index)
            })?;
        let piece = &mut self.pieces[index];
        let block = piece
            .blocks
            .iter()
            .position(|block| matches!(block, Block::Missing))?;
        piece.blocks[block] = Block::Requested {
            peer,
            since: Instant::now(),
        };
        let begin = block as u32 * BLOCK_SIZE;
        Some(BlockRequest {
            index: index as u32,
            begin,
            length: (piece.length - begin).min(BLOCK_SIZE),
        })
    }

    pub fn accept(
        &mut self,
        peer: usize,
        request: BlockRequest,
        bytes: Vec<u8>,
    ) -> Result<Option<FinishedPiece>> {
        let piece = self
            .pieces
            .get_mut(request.index as usize)
            .ok_or_else(|| anyhow::anyhow!("piece index out of range"))?;
        ensure!(piece.phase == Phase::Collecting, "piece is not collecting blocks");
        ensure!(
            request.begin % BLOCK_SIZE == 0 && request.begin < piece.length,
            "invalid block offset"
        );
        let expected_length = (piece.length - request.begin).min(BLOCK_SIZE);
        ensure!(
            request.length == expected_length && bytes.len() == expected_length as usize,
            "incorrect block length"
        );
        let block_index = (request.begin / BLOCK_SIZE) as usize;
        if !matches!(&piece.blocks[block_index], Block::Requested { peer: owner, .. } if *owner == peer)
        {
            bail!("block was not requested from this peer");
        }
        piece.blocks[block_index] = Block::Received(bytes);
        if piece.blocks.iter().any(|block| !matches!(block, Block::Received(_))) {
            return Ok(None);
        }
        piece.phase = Phase::Writing;
        let mut contents = Vec::with_capacity(piece.length as usize);
        for block in &mut piece.blocks {
            if let Block::Received(data) = std::mem::replace(block, Block::Missing) {
                contents.extend_from_slice(&data);
            }
        }
        Ok(Some(FinishedPiece {
            index: request.index,
            bytes: contents,
        }))
    }

    pub fn commit(&mut self, index: usize) -> Result<()> {
        let piece = self
            .pieces
            .get_mut(index)
            .ok_or_else(|| anyhow::anyhow!("piece index out of range"))?;
        ensure!(piece.phase == Phase::Writing, "piece is not awaiting verification");
        piece.phase = Phase::Verified;
        self.verified_bytes += u64::from(piece.length);
        self.verified_count += 1;
        Ok(())
    }

    pub fn reject(&mut self, index: usize) {
        if let Some(piece) = self.pieces.get_mut(index) {
            if piece.phase != Phase::Verified {
                piece.phase = Phase::Collecting;
                for block in &mut piece.blocks {
                    *block = Block::Missing;
                }
            }
        }
    }

    pub fn mark_resumed(&mut self, index: usize) -> Result<()> {
        let piece = self
            .pieces
            .get_mut(index)
            .ok_or_else(|| anyhow::anyhow!("piece index out of range"))?;
        if piece.phase != Phase::Verified {
            piece.phase = Phase::Verified;
            piece.blocks.clear();
            self.verified_bytes += u64::from(piece.length);
            self.verified_count += 1;
        }
        Ok(())
    }

    pub fn release_peer(&mut self, peer: usize) {
        for piece in &mut self.pieces {
            if piece.phase != Phase::Collecting {
                continue;
            }
            for block in &mut piece.blocks {
                if matches!(block, Block::Requested { peer: owner, .. } if *owner == peer) {
                    *block = Block::Missing;
                }
            }
        }
    }

    pub fn oldest_request_age(&self, peer: usize) -> Option<std::time::Duration> {
        let now = Instant::now();
        self.pieces
            .iter()
            .flat_map(|piece| piece.blocks.iter())
            .filter_map(|block| match block {
                Block::Requested { peer: owner, since } if *owner == peer => {
                    Some(now.duration_since(*since))
                }
                _ => None,
            })
            .max()
    }

    pub fn bitfield(&self) -> Vec<u8> {
        let mut bits = vec![0; self.pieces.len().div_ceil(8)];
        for (index, piece) in self.pieces.iter().enumerate() {
            if piece.phase == Phase::Verified {
                bits[index / 8] |= 1 << (7 - index % 8);
            }
        }
        bits
    }

    pub fn verified_indices(&self) -> Vec<u32> {
        self.pieces
            .iter()
            .enumerate()
            .filter_map(|(index, piece)| {
                (piece.phase == Phase::Verified).then_some(index as u32)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::TorrentFile;
    use std::path::PathBuf;

    fn torrent(pieces: usize, piece_length: u32, length: u64) -> Torrent {
        Torrent {
            name: "test.bin".into(),
            announce: "http://localhost/announce".into(),
            trackers: vec!["http://localhost/announce".into()],
            info_hash: [0; 20],
            piece_length,
            piece_hashes: vec![[0; 20]; pieces],
            files: vec![TorrentFile { path: PathBuf::from("test.bin"), length, offset: 0 }],
            total_length: length,
        }
    }

    #[test]
    fn chooses_rarest_available_piece_before_common_piece() {
        let mut picker = PiecePicker::new(&torrent(3, BLOCK_SIZE, 3 * BLOCK_SIZE as u64));
        picker.note_available(0);
        picker.note_available(0);
        picker.note_available(1);
        picker.note_available(2);
        picker.note_available(2);
        picker.note_available(2);
        assert_eq!(picker.claim(0, &[true, true, true]).unwrap().index, 1);
    }

    #[test]
    fn requeues_disconnected_requests_and_preserves_completed_blocks() {
        let mut picker = PiecePicker::new(&torrent(1, BLOCK_SIZE * 2, (BLOCK_SIZE * 2) as u64));
        let first = picker.claim(4, &[true]).unwrap();
        let second = picker.claim(4, &[true]).unwrap();
        assert!(picker.accept(4, first, vec![5; BLOCK_SIZE as usize]).unwrap().is_none());
        picker.release_peer(4);
        let retried = picker.claim(8, &[true]).unwrap();
        assert_eq!(retried, second);
        let finished = picker.accept(8, retried, vec![6; BLOCK_SIZE as usize]).unwrap().unwrap();
        assert_eq!(finished.bytes.len(), (BLOCK_SIZE * 2) as usize);
        assert!(finished.bytes[..BLOCK_SIZE as usize].iter().all(|byte| *byte == 5));
        picker.commit(0).unwrap();
        assert!(picker.complete());
    }

    #[test]
    fn calculates_last_partial_block_and_bitfield() {
        let mut picker = PiecePicker::new(&torrent(2, 20_000, 23_000));
        let first = picker.claim(0, &[true, false]).unwrap();
        let second = picker.claim(0, &[true, false]).unwrap();
        assert_eq!(first.length, BLOCK_SIZE);
        assert_eq!(second.length, 20_000 - BLOCK_SIZE);
        assert!(picker.accept(0, first, vec![0; BLOCK_SIZE as usize]).unwrap().is_none());
        let piece = picker.accept(0, second, vec![1; (20_000 - BLOCK_SIZE) as usize]).unwrap();
        assert_eq!(piece.unwrap().bytes.len(), 20_000);
        picker.commit(0).unwrap();
        assert_eq!(picker.bitfield(), vec![0b1000_0000]);
        assert_eq!(picker.verified_bytes(), 20_000);
        assert_eq!(picker.claim(0, &[false, true]).unwrap().length, 3000);
    }
}
