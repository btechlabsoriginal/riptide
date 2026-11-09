use crate::bencode::{self, Value};
use anyhow::{bail, ensure, Context, Result};
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct TorrentFile {
    pub path: PathBuf,
    pub length: u64,
    pub offset: u64,
}

#[derive(Clone, Debug)]
pub struct Torrent {
    pub name: String,
    pub announce: String,
    pub trackers: Vec<String>,
    pub info_hash: [u8; 20],
    pub piece_length: u32,
    pub piece_hashes: Vec<[u8; 20]>,
    pub files: Vec<TorrentFile>,
    pub total_length: u64,
}

impl Torrent {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let data = std::fs::read(path.as_ref())
            .with_context(|| format!("reading {}", path.as_ref().display()))?;
        Self::from_bytes(&data)
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let (root, spans) = bencode::decode_root_dict_with_spans(data)?;
        let raw_info = spans.get(&b"info"[..]).context("missing info dictionary")?;
        let info = dictionary(field(&root, b"info")?, "info")?;
        if matches!(info.get(&b"meta version"[..]), Some(Value::Integer(2))) {
            bail!("BitTorrent v2 torrents are not supported");
        }

        let mut info_hash = [0_u8; 20];
        info_hash.copy_from_slice(&Sha1::digest(&data[raw_info.clone()]));

        let name = text(info.get(&b"name.utf-8"[..]).or_else(|| info.get(&b"name"[..])), "name")?;
        validate_component(&name)?;
        let piece_length = positive(field(info, b"piece length")?, "piece length")?;
        ensure!(piece_length <= 64 * 1024 * 1024, "piece length is too large");
        let piece_length = u32::try_from(piece_length).context("piece length overflow")?;

        let hashes = bytes(field(info, b"pieces")?, "pieces")?;
        ensure!(hashes.len() % 20 == 0, "pieces is not a multiple of 20 bytes");
        let piece_hashes = hashes
            .chunks_exact(20)
            .map(|chunk| {
                let mut hash = [0_u8; 20];
                hash.copy_from_slice(chunk);
                hash
            })
            .collect::<Vec<_>>();
        ensure!(piece_hashes.len() <= u32::MAX as usize, "too many pieces for the peer protocol");

        let mut files = Vec::new();
        let mut total_length = 0_u64;
        let mut seen = HashSet::new();

        if let Some(value) = info.get(&b"files"[..]) {
            let Value::List(entries) = value else {
                bail!("files must be a list");
            };
            ensure!(!entries.is_empty(), "multi-file torrent has no files");
            for entry in entries {
                let entry = dictionary(entry, "file entry")?;
                let length = nonnegative(field(entry, b"length")?, "file length")?;
                let path_value = entry
                    .get(&b"path.utf-8"[..])
                    .or_else(|| entry.get(&b"path"[..]))
                    .context("file entry has no path")?;
                let Value::List(components) = path_value else {
                    bail!("file path must be a list");
                };
                ensure!(!components.is_empty(), "empty file path");
                let mut path = PathBuf::from(&name);
                for component in components {
                    let component = text(Some(component), "path component")?;
                    validate_component(&component)?;
                    path.push(component);
                }
                ensure!(seen.insert(path.clone()), "duplicate file path: {}", path.display());
                files.push(TorrentFile {
                    path,
                    length,
                    offset: total_length,
                });
                total_length = total_length.checked_add(length).context("torrent size overflow")?;
            }
        } else {
            let length = nonnegative(field(info, b"length")?, "length")?;
            files.push(TorrentFile {
                path: PathBuf::from(&name),
                length,
                offset: 0,
            });
            total_length = length;
        }

        ensure!(total_length > 0, "empty torrents are not supported");
        let expected_pieces = 1 + (total_length - 1) / u64::from(piece_length);
        ensure!(
            piece_hashes.len() as u64 == expected_pieces,
            "expected {expected_pieces} piece hashes, found {}",
            piece_hashes.len()
        );

        let announce = text(root.get(&b"announce"[..]), "announce")?;
        let mut trackers = Vec::new();
        if let Some(Value::List(tiers)) = root.get(&b"announce-list"[..]) {
            for tier in tiers {
                if let Value::List(urls) = tier {
                    for url in urls {
                        if let Ok(url) = text(Some(url), "tracker URL") {
                            if !trackers.contains(&url) {
                                trackers.push(url);
                            }
                        }
                    }
                }
            }
        }
        if !trackers.contains(&announce) {
            trackers.push(announce.clone());
        }
        ensure!(
            trackers.iter().any(|url| url.starts_with("http://") || url.starts_with("https://")),
            "no HTTP(S) tracker found; UDP and WebSocket trackers are not supported"
        );

        Ok(Self {
            name,
            announce,
            trackers,
            info_hash,
            piece_length,
            piece_hashes,
            files,
            total_length,
        })
    }

    pub fn piece_size(&self, index: usize) -> Option<usize> {
        if index >= self.piece_hashes.len() {
            return None;
        }
        let start = (index as u64).checked_mul(u64::from(self.piece_length))?;
        Some((self.total_length - start).min(u64::from(self.piece_length)) as usize)
    }

    pub fn info_hash_hex(&self) -> String {
        hex::encode(self.info_hash)
    }
}

fn field<'a>(dict: &'a BTreeMap<Vec<u8>, Value>, name: &[u8]) -> Result<&'a Value> {
    dict.get(name)
        .with_context(|| format!("missing {}", String::from_utf8_lossy(name)))
}

fn dictionary<'a>(value: &'a Value, name: &str) -> Result<&'a BTreeMap<Vec<u8>, Value>> {
    match value {
        Value::Dict(dict) => Ok(dict),
        _ => bail!("{name} must be a dictionary"),
    }
}

fn bytes<'a>(value: &'a Value, name: &str) -> Result<&'a [u8]> {
    match value {
        Value::Bytes(data) => Ok(data),
        _ => bail!("{name} must be a byte string"),
    }
}

fn text(value: Option<&Value>, name: &str) -> Result<String> {
    let bytes = bytes(value.with_context(|| format!("missing {name}"))?, name)?;
    Ok(std::str::from_utf8(bytes)
        .with_context(|| format!("{name} is not UTF-8"))?
        .to_owned())
}

fn nonnegative(value: &Value, name: &str) -> Result<u64> {
    match value {
        Value::Integer(n) if *n >= 0 => Ok(*n as u64),
        _ => bail!("{name} must be a non-negative integer"),
    }
}

fn positive(value: &Value, name: &str) -> Result<u64> {
    let number = nonnegative(value, name)?;
    ensure!(number > 0, "{name} must be positive");
    Ok(number)
}

fn validate_component(component: &str) -> Result<()> {
    ensure!(
        !component.is_empty()
            && component != "."
            && component != ".."
            && !component.chars().any(|c| matches!(c, '/' | '\\' | '\0' | ':')),
        "unsafe torrent path component: {component:?}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn torrent_bytes() -> Vec<u8> {
        let piece = Sha1::digest(b"hello world");
        let info = Value::Dict(BTreeMap::from([
            (b"length".to_vec(), Value::Integer(11)),
            (b"name".to_vec(), Value::Bytes(b"hello.txt".to_vec())),
            (b"piece length".to_vec(), Value::Integer(16_384)),
            (b"pieces".to_vec(), Value::Bytes(piece.to_vec())),
        ]));
        bencode::encode(&Value::Dict(BTreeMap::from([
            (
                b"announce".to_vec(),
                Value::Bytes(b"https://tracker.example/announce".to_vec()),
            ),
            (b"info".to_vec(), info),
        ])))
    }

    #[test]
    fn parses_v1_metainfo_and_hashes_original_info_bytes() {
        let data = torrent_bytes();
        let torrent = Torrent::from_bytes(&data).unwrap();
        let (_, spans) = bencode::decode_root_dict_with_spans(&data).unwrap();
        assert_eq!(
            torrent.info_hash.as_slice(),
            Sha1::digest(&data[spans[&b"info"[..]].clone()]).as_slice()
        );
        assert_eq!(torrent.files[0].path, PathBuf::from("hello.txt"));
        assert_eq!(torrent.files[0].length, 11);
        assert_eq!(torrent.piece_size(0), Some(11));
    }

    #[test]
    fn rejects_paths_that_escape_download_root() {
        for bad in ["../escape", "..", "x/y", "x\\y", "C:"] {
            assert!(validate_component(bad).is_err());
        }
    }

    #[test]
    fn rejects_inconsistent_piece_hash_count() {
        let mut root = bencode::decode(&torrent_bytes()).unwrap();
        if let Value::Dict(ref mut fields) = root {
            if let Value::Dict(ref mut info) = fields.get_mut(&b"info"[..]).unwrap() {
                info.insert(b"pieces".to_vec(), Value::Bytes(vec![0; 40]));
            }
        }
        assert!(Torrent::from_bytes(&bencode::encode(&root)).is_err());
    }
}
