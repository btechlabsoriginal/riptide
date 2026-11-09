use crate::peer::{self, Message};
use crate::picker::{BlockRequest, PiecePicker, BLOCK_SIZE};
use crate::storage::Storage;
use crate::torrent::Torrent;
use crate::tracker;
use anyhow::{bail, ensure, Context, Result};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(25);
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);
const PIPELINE: usize = 6;

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub name: String,
    pub total: u64,
    pub verified: u64,
    pub pieces: usize,
    pub complete_pieces: usize,
    pub connections: usize,
    pub received: u64,
    pub uploaded: u64,
    pub elapsed: Duration,
    pub status: String,
    pub events: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Resume {
    version: u8,
    info_hash: String,
    verified: Vec<u32>,
}

pub struct Shared {
    torrent: Arc<Torrent>,
    pub storage: Storage,
    picker: Mutex<PiecePicker>,
    resume_path: PathBuf,
    checkpoint_lock: Mutex<()>,
    received: AtomicU64,
    uploaded: AtomicU64,
    connections: AtomicUsize,
    stopping: AtomicBool,
    started: Instant,
    status: Mutex<String>,
    events: Mutex<VecDeque<String>>,
    have: broadcast::Sender<u32>,
}

impl Shared {
    pub fn open(torrent: Arc<Torrent>, output: &Path, resume_path: Option<PathBuf>) -> Result<Arc<Self>> {
        let storage = Storage::open(output, &torrent)?;
        let resume_path = resume_path.unwrap_or_else(|| {
            output.join(".riptide").join(format!("{}.json", torrent.info_hash_hex()))
        });
        let (have, _) = broadcast::channel(256);
        let shared = Arc::new(Self {
            picker: Mutex::new(PiecePicker::new(&torrent)),
            torrent,
            storage,
            resume_path,
            checkpoint_lock: Mutex::new(()),
            received: AtomicU64::new(0),
            uploaded: AtomicU64::new(0),
            connections: AtomicUsize::new(0),
            stopping: AtomicBool::new(false),
            started: Instant::now(),
            status: Mutex::new(String::from("Preparing download")),
            events: Mutex::new(VecDeque::new()),
            have,
        });
        shared.restore()?;
        Ok(shared)
    }

    pub fn snapshot(&self) -> Snapshot {
        let picker = self.picker.lock().expect("picker lock poisoned");
        Snapshot {
            name: self.torrent.name.clone(),
            total: self.torrent.total_length,
            verified: picker.verified_bytes(),
            pieces: picker.piece_count(),
            complete_pieces: picker.verified_count(),
            connections: self.connections.load(Ordering::Relaxed),
            received: self.received.load(Ordering::Relaxed),
            uploaded: self.uploaded.load(Ordering::Relaxed),
            elapsed: self.started.elapsed(),
            status: self.status.lock().expect("status lock poisoned").clone(),
            events: self.events.lock().expect("events lock poisoned").iter().cloned().collect(),
        }
    }

    pub fn is_complete(&self) -> bool {
        self.picker.lock().expect("picker lock poisoned").complete()
    }

    pub fn stop(&self) {
        self.stopping.store(true, Ordering::Relaxed);
    }

    fn stopped(&self) -> bool {
        self.stopping.load(Ordering::Relaxed)
    }

    pub fn status(&self, status: impl Into<String>) {
        *self.status.lock().expect("status lock poisoned") = status.into();
    }

    pub fn log(&self, message: impl Into<String>) {
        let mut events = self.events.lock().expect("events lock poisoned");
        if events.len() == 40 {
            events.pop_front();
        }
        events.push_back(message.into());
    }

    fn restore(&self) -> Result<()> {
        if !self.resume_path.exists() {
            return Ok(());
        }
        let state: Resume = match fs::read(&self.resume_path)
            .ok()
            .and_then(|data| serde_json::from_slice(&data).ok())
        {
            Some(state) => state,
            None => {
                self.log("Resume file could not be read; starting without cached pieces");
                return Ok(());
            }
        };
        if state.version != 1 || state.info_hash != self.torrent.info_hash_hex() {
            self.log("Resume file belongs to a different torrent; ignoring it");
            return Ok(());
        }
        let mut recovered = 0_usize;
        let mut seen = HashSet::new();
        for index in state.verified {
            if !seen.insert(index) || (index as usize) >= self.torrent.piece_hashes.len() {
                continue;
            }
            if self.storage.verify_existing_piece(index as usize).unwrap_or(false) {
                self.picker.lock().expect("picker lock poisoned").mark_resumed(index as usize)?;
                recovered += 1;
            }
        }
        self.log(format!("Recovered {recovered} SHA-1-verified pieces from resume state"));
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<()> {
        let _checkpoint = self.checkpoint_lock.lock().expect("checkpoint lock poisoned");
        self.storage.sync()?;
        let state = Resume {
            version: 1,
            info_hash: self.torrent.info_hash_hex(),
            verified: self.picker.lock().expect("picker lock poisoned").verified_indices(),
        };
        let parent = self.resume_path.parent().context("resume state has no parent directory")?;
        fs::create_dir_all(parent)?;
        let temp_path = self.resume_path.with_extension("tmp");
        fs::write(&temp_path, serde_json::to_vec_pretty(&state)?)?;
        fs::File::open(&temp_path)?.sync_all()?;
        fs::rename(&temp_path, &self.resume_path)?;
        Ok(())
    }
}

struct Lease {
    shared: Arc<Shared>,
    id: usize,
    has: Vec<bool>,
}

impl Lease {
    fn new(shared: Arc<Shared>, id: usize) -> Self {
        shared.connections.fetch_add(1, Ordering::Relaxed);
        let has = vec![false; shared.torrent.piece_hashes.len()];
        Self { shared, id, has }
    }

    fn add(&mut self, index: usize) -> Result<()> {
        ensure!(index < self.has.len(), "peer advertised a nonexistent piece");
        if !self.has[index] {
            self.has[index] = true;
            self.shared.picker.lock().expect("picker lock poisoned").note_available(index);
        }
        Ok(())
    }

    fn update_bitfield(&mut self, bits: &[u8]) -> Result<()> {
        ensure!(bits.len() == self.has.len().div_ceil(8), "peer bitfield has invalid length");
        for index in 0..self.has.len() {
            if bits[index / 8] & (1 << (7 - index % 8)) != 0 {
                self.add(index)?;
            }
        }
        Ok(())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut picker = self.shared.picker.lock().expect("picker lock poisoned");
        picker.release_peer(self.id);
        for (index, present) in self.has.iter().enumerate() {
            if *present {
                picker.note_unavailable(index);
            }
        }
        self.shared.connections.fetch_sub(1, Ordering::Relaxed);
    }
}

pub async fn download(shared: Arc<Shared>, connections: usize, peer_id: [u8; 20]) -> Result<()> {
    if shared.is_complete() {
        shared.status("All pieces are already verified");
        return Ok(());
    }

    let listener = TcpListener::bind(("0.0.0.0", 0)).await?;
    let port = listener.local_addr()?.port();
    shared.log(format!("Listening for incoming peers on port {port}"));
    let listener_shared = shared.clone();
    let _acceptor = AbortOnDrop(tokio::spawn(async move {
        accept_inbound(listener, listener_shared, connections, peer_id).await;
    }));

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent("riptide/0.1")
        .build()?;
    shared.status("Announcing to trackers");
    let mut peers = HashSet::new();
    let mut tracker_used = None;
    for url in shared
        .torrent
        .trackers
        .iter()
        .filter(|url| url.starts_with("http://") || url.starts_with("https://"))
        .take(8)
    {
        if shared.stopped() {
            bail!("stopped by user");
        }
        match tracker::announce(
            &http,
            url,
            &shared.torrent,
            &peer_id,
            port,
            shared.snapshot().verified,
            Some("started"),
        )
        .await
        {
            Ok(response) => {
                shared.log(format!(
                    "Tracker returned {} peers, next announce in {}s",
                    response.peers.len(),
                    response.interval.as_secs()
                ));
                tracker_used.get_or_insert_with(|| url.clone());
                peers.extend(response.peers);
                if peers.len() >= connections * 8 {
                    break;
                }
            }
            Err(error) => shared.log(format!("Tracker {}: {error:#}", url)),
        }
    }
    ensure!(!peers.is_empty(), "no peers were returned by the available HTTP trackers");
    shared.log(format!("Discovered {} unique peers", peers.len()));

    let mut next_peer_id = 0_usize;
    for pass in 0..2 {
        let mut addresses: Vec<_> = peers.iter().copied().collect();
        addresses.shuffle(&mut rand::thread_rng());
        shared.status(format!("Downloading from peers, pass {}", pass + 1));
        for batch in addresses.chunks(connections) {
            if shared.stopped() {
                bail!("stopped by user");
            }
            let mut workers = JoinSet::new();
            for &address in batch {
                let current = next_peer_id;
                next_peer_id += 1;
                let session_shared = shared.clone();
                workers.spawn(async move { session(session_shared, current, address, peer_id).await });
            }
            while !workers.is_empty() {
                if shared.stopped() || shared.is_complete() {
                    workers.abort_all();
                    while workers.join_next().await.is_some() {}
                    break;
                }
                tokio::select! {
                    finished = workers.join_next() => {
                        if let Some(Ok(Err(error))) = finished {
                            let summary = format!("{error:#}");
                            if !summary.contains("peer idle") {
                                shared.log(summary);
                            }
                        }
                    }
                    _ = sleep(Duration::from_millis(250)) => {}
                }
            }
            if shared.is_complete() || shared.stopped() {
                break;
            }
        }
        if shared.is_complete() || shared.stopped() {
            break;
        }
    }

    if shared.stopped() {
        bail!("stopped by user");
    }
    if !shared.is_complete() {
        bail!("download incomplete; peers were exhausted or unavailable; run again to resume");
    }
    shared.checkpoint()?;
    shared.status("Download complete; every piece verified");
    if let Some(url) = tracker_used {
        let _ = tracker::announce(
            &http,
            &url,
            &shared.torrent,
            &peer_id,
            port,
            shared.torrent.total_length,
            Some("completed"),
        )
        .await;
    }
    Ok(())
}

async fn session(shared: Arc<Shared>, id: usize, address: SocketAddr, peer_id: [u8; 20]) -> Result<()> {
    let mut socket = timeout(CONNECT_TIMEOUT, TcpStream::connect(address))
        .await
        .with_context(|| format!("connection to {address} timed out"))??;
    socket.set_nodelay(true)?;
    timeout(HANDSHAKE_TIMEOUT, async {
        peer::send_handshake(&mut socket, &shared.torrent.info_hash, &peer_id).await?;
        peer::read_handshake(&mut socket, &shared.torrent.info_hash).await
    })
    .await
    .with_context(|| format!("handshake with {address} timed out"))??;

    run_peer_session(shared, id, address, socket).await
}

async fn incoming_session(
    shared: Arc<Shared>,
    id: usize,
    address: SocketAddr,
    mut socket: TcpStream,
    peer_id: [u8; 20],
) -> Result<()> {
    timeout(HANDSHAKE_TIMEOUT, async {
        peer::read_handshake(&mut socket, &shared.torrent.info_hash).await?;
        peer::send_handshake(&mut socket, &shared.torrent.info_hash, &peer_id).await
    })
    .await
    .with_context(|| format!("incoming handshake from {address} timed out"))??;
    socket.set_nodelay(true)?;
    run_peer_session(shared, id, address, socket).await
}

async fn accept_inbound(
    listener: TcpListener,
    shared: Arc<Shared>,
    limit: usize,
    peer_id: [u8; 20],
) {
    let mut workers = JoinSet::new();
    let mut next_id = usize::MAX;
    while !shared.stopped() && !shared.is_complete() {
        tokio::select! {
            accepted = listener.accept() => {
                if let Ok((socket, address)) = accepted {
                    if shared.connections.load(Ordering::Relaxed) < limit {
                        let id = next_id;
                        next_id = next_id.wrapping_sub(1);
                        let copy = shared.clone();
                        workers.spawn(async move {
                            let _ = incoming_session(copy, id, address, socket, peer_id).await;
                        });
                    }
                }
            }
            _ = workers.join_next(), if !workers.is_empty() => {}
            _ = sleep(Duration::from_millis(250)) => {}
        }
    }
    workers.abort_all();
    while workers.join_next().await.is_some() {}
}

async fn run_peer_session(
    shared: Arc<Shared>,
    id: usize,
    address: SocketAddr,
    mut socket: TcpStream,
) -> Result<()> {
    let mut lease = Lease::new(shared.clone(), id);
    let mut notifications = shared.have.subscribe();
    let bitfield = shared.picker.lock().expect("picker lock poisoned").bitfield();
    if bitfield.iter().any(|byte| *byte != 0) {
        peer::send_message(&mut socket, &Message::Bitfield(bitfield)).await?;
    }
    peer::send_message(&mut socket, &Message::Unchoke).await?;
    peer::send_message(&mut socket, &Message::Interested).await?;
    let mut choked = true;
    let mut pending: HashMap<(u32, u32), (BlockRequest, Instant)> = HashMap::new();
    let started = Instant::now();
    let mut last_block = started;
    let mut last_keepalive = started;

    loop {
        if shared.stopped() || shared.is_complete() {
            return Ok(());
        }
        if !choked {
            while pending.len() < PIPELINE {
                let request = {
                    shared.picker.lock().expect("picker lock poisoned").claim(id, &lease.has)
                };
                let Some(request) = request else {
                    break;
                };
                peer::send_message(
                    &mut socket,
                    &Message::Request {
                        index: request.index,
                        begin: request.begin,
                        length: request.length,
                    },
                )
                .await?;
                pending.insert((request.index, request.begin), (request, Instant::now()));
            }
        }

        tokio::select! {
            message = peer::read_message(&mut socket) => {
                match message? {
                    Message::KeepAlive => {}
                    Message::Choke => choked = true,
                    Message::Unchoke => choked = false,
                    Message::Interested | Message::NotInterested => {}
                    Message::Have(index) => lease.add(index as usize)?,
                    Message::Bitfield(bits) => lease.update_bitfield(&bits)?,
                    Message::Request { index, begin, length } => {
                        let allowed = shared.picker.lock().expect("picker lock poisoned")
                            .is_verified(index as usize);
                        if allowed && length > 0 && length <= BLOCK_SIZE {
                            let block = shared.storage.read_verified_block(
                                index as usize,
                                begin,
                                length,
                            )?;
                            peer::send_message(&mut socket, &Message::Piece { index, begin, block }).await?;
                            shared.uploaded.fetch_add(u64::from(length), Ordering::Relaxed);
                        }
                    }
                    Message::Piece { index, begin, block } => {
                        let Some((request, _)) = pending.remove(&(index, begin)) else {
                            continue;
                        };
                        let block_size = block.len() as u64;
                        let finished = shared.picker.lock().expect("picker lock poisoned")
                            .accept(id, request, block)?;
                        shared.received.fetch_add(block_size, Ordering::Relaxed);
                        last_block = Instant::now();
                        if let Some(piece) = finished {
                            let expected = &shared.torrent.piece_hashes[piece.index as usize];
                            if Sha1::digest(&piece.bytes).as_slice() != expected.as_slice() {
                                shared.picker.lock().expect("picker lock poisoned")
                                    .reject(piece.index as usize);
                                bail!("peer {address} contributed data to a piece that failed SHA-1");
                            }
                            shared.storage.write_verified_piece(piece.index as usize, &piece.bytes)?;
                            shared.picker.lock().expect("picker lock poisoned")
                                .commit(piece.index as usize)?;
                            shared.checkpoint()?;
                            let _ = shared.have.send(piece.index);
                            let snapshot = shared.snapshot();
                            shared.log(format!(
                                "Verified piece {}/{} · {:.1}%",
                                piece.index + 1,
                                snapshot.pieces,
                                snapshot.verified as f64 * 100.0 / snapshot.total as f64
                            ));
                        }
                    }
                    Message::Cancel { .. } | Message::Port(_) | Message::Unknown { .. } => {}
                }
            }
            notice = notifications.recv() => {
                if let Ok(index) = notice {
                    peer::send_message(&mut socket, &Message::Have(index)).await?;
                }
            }
            _ = sleep(Duration::from_millis(250)) => {}
        }

        if pending.values().any(|(_, sent)| sent.elapsed() > REQUEST_TIMEOUT) {
            bail!("peer {address} timed out on a block request");
        }
        if last_block.elapsed() > IDLE_TIMEOUT {
            let wanted = {
                let picker = shared.picker.lock().expect("picker lock poisoned");
                lease.has.iter().enumerate().any(|(index, has)| *has && !picker.is_verified(index))
            };
            if choked || !wanted || pending.is_empty() {
                bail!("peer idle: {address}");
            }
        }
        if started.elapsed() > Duration::from_secs(18) && !lease.has.iter().any(|has| *has) {
            bail!("peer idle: {address} did not advertise any pieces");
        }
        if last_keepalive.elapsed() > Duration::from_secs(60) {
            peer::send_message(&mut socket, &Message::KeepAlive).await?;
            last_keepalive = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::TorrentFile;
    use tempfile::tempdir;

    #[test]
    fn resume_state_revalidates_existing_data_before_accepting_it() {
        let root = tempdir().unwrap();
        let contents = b"abcdefgh";
        let mut hash = [0; 20];
        hash.copy_from_slice(&Sha1::digest(contents));
        let torrent = Arc::new(Torrent {
            name: "test.bin".into(),
            announce: "http://localhost".into(),
            trackers: vec!["http://localhost".into()],
            info_hash: [2; 20],
            piece_length: 8,
            piece_hashes: vec![hash],
            files: vec![TorrentFile { path: PathBuf::from("test.bin"), length: 8, offset: 0 }],
            total_length: 8,
        });
        let first = Shared::open(torrent.clone(), root.path(), None).unwrap();
        first.storage.write_verified_piece(0, contents).unwrap();
        first.picker.lock().unwrap().mark_resumed(0).unwrap();
        first.checkpoint().unwrap();
        let second = Shared::open(torrent.clone(), root.path(), None).unwrap();
        assert_eq!(second.snapshot().verified, 8);
        fs::write(root.path().join("test.bin"), b"bad data").unwrap();
        let third = Shared::open(torrent, root.path(), None).unwrap();
        assert_eq!(third.snapshot().verified, 0);
    }
    #[tokio::test]
    async fn downloads_and_verifies_data_from_a_local_tracker_and_peer() {
        use crate::bencode::{self, Value};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let root = tempdir().unwrap();
        let payload = b"a small legal test data file".to_vec();
        let piece_length = 12_usize;
        let hashes = payload
            .chunks(piece_length)
            .map(|chunk| {
                let mut hash = [0; 20];
                hash.copy_from_slice(&Sha1::digest(chunk));
                hash
            })
            .collect::<Vec<_>>();

        let peer_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let peer_port = peer_listener.local_addr().unwrap().port();
        let info_hash = [3; 20];
        let peer_payload = payload.clone();
        let peer_task = tokio::spawn(async move {
            let (mut socket, _) = peer_listener.accept().await.unwrap();
            peer::read_handshake(&mut socket, &info_hash).await.unwrap();
            peer::send_handshake(&mut socket, &info_hash, &[4; 20]).await.unwrap();
            peer::send_message(&mut socket, &Message::Bitfield(vec![0b1110_0000])).await.unwrap();
            peer::send_message(&mut socket, &Message::Unchoke).await.unwrap();
            while let Ok(Ok(message)) = timeout(Duration::from_secs(10), peer::read_message(&mut socket)).await {
                if let Message::Request { index, begin, length } = message {
                    let start = index as usize * piece_length + begin as usize;
                    let end = start + length as usize;
                    peer::send_message(&mut socket, &Message::Piece {
                        index,
                        begin,
                        block: peer_payload[start..end].to_vec(),
                    }).await.unwrap();
                }
            }
        });

        let tracker_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let tracker_port = tracker_listener.local_addr().unwrap().port();
        let tracker_task = tokio::spawn(async move {
            for _ in 0..2 {
                let Ok(Ok((mut socket, _))) = timeout(Duration::from_secs(12), tracker_listener.accept()).await else {
                    return;
                };
                let mut request = [0_u8; 4096];
                let _ = socket.read(&mut request).await.unwrap();
                let mut compact = vec![127, 0, 0, 1];
                compact.extend_from_slice(&peer_port.to_be_bytes());
                let response = bencode::encode(&Value::Dict(std::collections::BTreeMap::from([
                    (b"interval".to_vec(), Value::Integer(1800)),
                    (b"peers".to_vec(), Value::Bytes(compact)),
                ])));
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(&response).await.unwrap();
            }
        });
        let url = format!("http://127.0.0.1:{tracker_port}/announce");
        let torrent = Arc::new(Torrent {
            name: "sample".into(),
            announce: url.clone(),
            trackers: vec![url],
            info_hash,
            piece_length: piece_length as u32,
            piece_hashes: hashes,
            files: vec![
                TorrentFile { path: PathBuf::from("sample/one"), length: 7, offset: 0 },
                TorrentFile {
                    path: PathBuf::from("sample/two"),
                    length: payload.len() as u64 - 7,
                    offset: 7,
                },
            ],
            total_length: payload.len() as u64,
        });
        let shared = Shared::open(torrent, root.path(), None).unwrap();
        timeout(Duration::from_secs(20), download(shared.clone(), 2, [5; 20]))
            .await
            .unwrap()
            .unwrap();
        assert!(shared.is_complete());
        assert_eq!(fs::read(root.path().join("sample/one")).unwrap().as_slice(), &payload[..7]);
        assert_eq!(fs::read(root.path().join("sample/two")).unwrap().as_slice(), &payload[7..]);
        peer_task.abort();
        tracker_task.abort();
    }

}
