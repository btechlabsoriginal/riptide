use crate::torrent::Torrent;
use anyhow::{bail, ensure, Context, Result};
use sha1::{Digest, Sha1};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

struct FileSpan {
    start: u64,
    end: u64,
    path: PathBuf,
}

pub struct Storage {
    files: Mutex<Vec<File>>,
    spans: Vec<FileSpan>,
    piece_length: u64,
    piece_hashes: Vec<[u8; 20]>,
    total_length: u64,
}

impl Storage {
    pub fn open(root: &Path, torrent: &Torrent) -> Result<Self> {
        fs::create_dir_all(root).with_context(|| format!("creating {}", root.display()))?;
        let root = root.canonicalize()?;
        let mut files = Vec::with_capacity(torrent.files.len());
        let mut spans = Vec::with_capacity(torrent.files.len());

        for entry in &torrent.files {
            let path = root.join(&entry.path);
            let parent = path.parent().context("torrent file has no parent directory")?;
            fs::create_dir_all(parent)?;
            ensure!(
                parent.canonicalize()?.starts_with(&root),
                "torrent path crosses a symlink outside the output directory"
            );
            if let Ok(metadata) = fs::symlink_metadata(&path) {
                ensure!(!metadata.file_type().is_symlink(), "torrent target is a symlink");
            }
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)
                .with_context(|| format!("opening {}", path.display()))?;
            if file.metadata()?.len() != entry.length {
                file.set_len(entry.length)?;
            }
            spans.push(FileSpan {
                start: entry.offset,
                end: entry.offset + entry.length,
                path,
            });
            files.push(file);
        }

        Ok(Self {
            files: Mutex::new(files),
            spans,
            piece_length: u64::from(torrent.piece_length),
            piece_hashes: torrent.piece_hashes.clone(),
            total_length: torrent.total_length,
        })
    }

    pub fn write_verified_piece(&self, index: usize, bytes: &[u8]) -> Result<()> {
        let expected = self.piece_hashes.get(index).context("piece index out of range")?;
        let (offset, length) = self.piece_range(index)?;
        ensure!(bytes.len() == length, "piece has an unexpected length");
        ensure!(Sha1::digest(bytes).as_slice() == expected.as_slice(), "SHA-1 piece mismatch");
        self.write_range(offset, bytes)
    }

    pub fn verify_existing_piece(&self, index: usize) -> Result<bool> {
        let expected = self.piece_hashes.get(index).context("piece index out of range")?;
        let (offset, length) = self.piece_range(index)?;
        let data = self.read_range(offset, length)?;
        Ok(Sha1::digest(&data).as_slice() == expected.as_slice())
    }

    pub fn read_verified_block(&self, index: usize, begin: u32, length: u32) -> Result<Vec<u8>> {
        let (offset, piece_length) = self.piece_range(index)?;
        ensure!(length > 0 && length <= 16 * 1024, "invalid upload block length");
        ensure!(
            (begin as usize).checked_add(length as usize).is_some_and(|end| end <= piece_length),
            "upload block exceeds piece boundaries"
        );
        self.read_range(offset + u64::from(begin), length as usize)
    }

    pub fn sync(&self) -> Result<()> {
        let files = self.files.lock().map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
        for file in files.iter() {
            file.sync_data()?;
        }
        Ok(())
    }

    pub fn paths(&self) -> Vec<PathBuf> {
        self.spans.iter().map(|span| span.path.clone()).collect()
    }

    fn piece_range(&self, index: usize) -> Result<(u64, usize)> {
        ensure!(index < self.piece_hashes.len(), "piece index out of range");
        let offset = (index as u64)
            .checked_mul(self.piece_length)
            .context("piece offset overflow")?;
        let length = self.total_length.saturating_sub(offset).min(self.piece_length) as usize;
        Ok((offset, length))
    }

    fn read_range(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        ensure!(
            offset.checked_add(length as u64).is_some_and(|end| end <= self.total_length),
            "read outside torrent boundaries"
        );
        let mut output = vec![0_u8; length];
        let mut files = self.files.lock().map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
        let mut current = offset;
        let mut filled = 0_usize;
        for (span, file) in self.spans.iter().zip(files.iter_mut()) {
            if filled == length {
                break;
            }
            if current < span.start || current >= span.end {
                continue;
            }
            let count = ((span.end - current) as usize).min(length - filled);
            file.seek(SeekFrom::Start(current - span.start))?;
            file.read_exact(&mut output[filled..filled + count])?;
            current += count as u64;
            filled += count;
        }
        if filled != length {
            bail!("torrent file layout contains a gap");
        }
        Ok(output)
    }

    fn write_range(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        ensure!(
            offset.checked_add(bytes.len() as u64).is_some_and(|end| end <= self.total_length),
            "write outside torrent boundaries"
        );
        let mut files = self.files.lock().map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
        let mut current = offset;
        let mut written = 0_usize;
        for (span, file) in self.spans.iter().zip(files.iter_mut()) {
            if written == bytes.len() {
                break;
            }
            if current < span.start || current >= span.end {
                continue;
            }
            let count = ((span.end - current) as usize).min(bytes.len() - written);
            file.seek(SeekFrom::Start(current - span.start))?;
            file.write_all(&bytes[written..written + count])?;
            current += count as u64;
            written += count;
        }
        if written != bytes.len() {
            bail!("torrent file layout contains a gap");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::TorrentFile;
    use tempfile::tempdir;

    fn fixture() -> (Torrent, Vec<u8>) {
        let content = b"abcdefghijklmnopqr".to_vec();
        let hashes = content
            .chunks(8)
            .map(|chunk| {
                let mut result = [0; 20];
                result.copy_from_slice(&Sha1::digest(chunk));
                result
            })
            .collect();
        let torrent = Torrent {
            name: "batch".into(),
            announce: "http://localhost".into(),
            trackers: vec!["http://localhost".into()],
            info_hash: [1; 20],
            piece_length: 8,
            piece_hashes: hashes,
            total_length: content.len() as u64,
            files: vec![
                TorrentFile { path: PathBuf::from("batch/first"), offset: 0, length: 5 },
                TorrentFile { path: PathBuf::from("batch/second"), offset: 5, length: 13 },
            ],
        };
        (torrent, content)
    }

    #[test]
    fn writes_pieces_across_file_boundaries_and_recovers_them() {
        let dir = tempdir().unwrap();
        let (torrent, content) = fixture();
        let storage = Storage::open(dir.path(), &torrent).unwrap();
        for (index, chunk) in content.chunks(8).enumerate() {
            storage.write_verified_piece(index, chunk).unwrap();
            assert!(storage.verify_existing_piece(index).unwrap());
        }
        assert_eq!(fs::read(dir.path().join("batch/first")).unwrap(), b"abcde");
        assert_eq!(fs::read(dir.path().join("batch/second")).unwrap(), b"fghijklmnopqr");
        assert_eq!(storage.read_verified_block(1, 2, 3).unwrap(), b"klm".to_vec());
    }

    #[test]
    fn refuses_corrupted_piece_without_writing_it() {
        let dir = tempdir().unwrap();
        let (torrent, _) = fixture();
        let storage = Storage::open(dir.path(), &torrent).unwrap();
        assert!(storage.write_verified_piece(0, b"xxxxxxxx").is_err());
        assert!(!storage.verify_existing_piece(0).unwrap());
    }
}
