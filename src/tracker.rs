use crate::bencode::{self, Value};
use crate::torrent::Torrent;
use anyhow::{bail, ensure, Context, Result};
use reqwest::Client;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

#[derive(Debug)]
pub struct Announce {
    pub interval: Duration,
    pub peers: Vec<SocketAddr>,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
}

pub async fn announce(
    http: &Client,
    url: &str,
    torrent: &Torrent,
    peer_id: &[u8; 20],
    port: u16,
    downloaded: u64,
    event: Option<&str>,
) -> Result<Announce> {
    ensure!(
        url.starts_with("http://") || url.starts_with("https://"),
        "only HTTP(S) trackers are supported"
    );
    let separator = if url.contains('?') { '&' } else { '?' };
    let mut request = format!(
        "{url}{separator}info_hash={}&peer_id={}&port={port}&uploaded=0&downloaded={downloaded}&left={}&compact=1&numwant=100",
        escape_bytes(&torrent.info_hash),
        escape_bytes(peer_id),
        torrent.total_length.saturating_sub(downloaded)
    );
    if let Some(event) = event {
        ensure!(matches!(event, "started" | "completed" | "stopped"), "unknown tracker event");
        write!(&mut request, "&event={event}")?;
    }
    let response = http
        .get(request)
        .send()
        .await
        .with_context(|| format!("request to {url} failed"))?
        .error_for_status()?
        .bytes()
        .await?;
    ensure!(response.len() <= 2 * 1024 * 1024, "tracker response is too large");
    decode_announce(&response)
}

pub fn decode_announce(input: &[u8]) -> Result<Announce> {
    let decoded = bencode::decode(input)?;
    let Value::Dict(fields) = decoded else {
        bail!("tracker response must be a dictionary");
    };
    if let Some(Value::Bytes(message)) = fields.get(&b"failure reason"[..]) {
        bail!("tracker rejected announce: {}", String::from_utf8_lossy(message));
    }

    let interval = number(&fields, b"interval").unwrap_or(1800).clamp(30, 86_400);
    let mut peers = Vec::new();
    if let Some(value) = fields.get(&b"peers"[..]) {
        match value {
            Value::Bytes(raw) => peers.extend(decode_compact_v4(raw)?),
            Value::List(entries) => {
                for entry in entries {
                    let Value::Dict(peer) = entry else {
                        continue;
                    };
                    let Some(Value::Bytes(ip)) = peer.get(&b"ip"[..]) else {
                        continue;
                    };
                    let Some(port) = number(peer, b"port") else {
                        continue;
                    };
                    let Ok(port) = u16::try_from(port) else {
                        continue;
                    };
                    let Ok(ip) = std::str::from_utf8(ip) else {
                        continue;
                    };
                    if let Ok(ip) = ip.parse::<IpAddr>() {
                        if port != 0 {
                            peers.push(SocketAddr::new(ip, port));
                        }
                    }
                }
            }
            _ => bail!("unsupported peers encoding"),
        }
    }
    if let Some(Value::Bytes(raw)) = fields.get(&b"peers6"[..]) {
        peers.extend(decode_compact_v6(raw)?);
    }
    let mut seen = HashSet::new();
    peers.retain(|address| seen.insert(*address));

    Ok(Announce {
        interval: Duration::from_secs(interval),
        peers,
        seeders: number(&fields, b"complete"),
        leechers: number(&fields, b"incomplete"),
    })
}

pub fn decode_compact_v4(input: &[u8]) -> Result<Vec<SocketAddr>> {
    ensure!(input.len() % 6 == 0, "invalid IPv4 compact peer list length");
    Ok(input
        .chunks_exact(6)
        .filter_map(|chunk| {
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            (port != 0).then(|| {
                SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3])),
                    port,
                )
            })
        })
        .collect())
}

pub fn decode_compact_v6(input: &[u8]) -> Result<Vec<SocketAddr>> {
    ensure!(input.len() % 18 == 0, "invalid IPv6 compact peer list length");
    Ok(input
        .chunks_exact(18)
        .filter_map(|chunk| {
            let port = u16::from_be_bytes([chunk[16], chunk[17]]);
            let mut ip = [0_u8; 16];
            ip.copy_from_slice(&chunk[..16]);
            (port != 0)
                .then(|| SocketAddr::new(IpAddr::V6(Ipv6Addr::from(ip)), port))
        })
        .collect())
}

fn number(fields: &BTreeMap<Vec<u8>, Value>, key: &[u8]) -> Option<u64> {
    match fields.get(key) {
        Some(Value::Integer(n)) if *n >= 0 => Some(*n as u64),
        _ => None,
    }
}

fn escape_bytes(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 3);
    for byte in bytes {
        write!(&mut encoded, "%{byte:02X}").expect("writing to String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_compact_ipv4_and_ipv6() {
        let peers = decode_compact_v4(&[127, 0, 0, 1, 0x1a, 0xe1, 8, 8, 8, 8, 0, 80]).unwrap();
        assert_eq!(peers[0].to_string(), "127.0.0.1:6881");
        assert_eq!(peers[1].to_string(), "8.8.8.8:80");
        let mut v6 = [0_u8; 18];
        v6[15] = 1;
        v6[16..].copy_from_slice(&6881_u16.to_be_bytes());
        assert_eq!(decode_compact_v6(&v6).unwrap()[0].to_string(), "[::1]:6881");
        assert!(decode_compact_v4(&[1, 2, 3]).is_err());
        assert!(decode_compact_v6(&[0; 17]).is_err());
    }

    #[test]
    fn understands_tracker_errors_and_compact_response() {
        let value = Value::Dict(BTreeMap::from([
            (b"interval".to_vec(), Value::Integer(900)),
            (b"peers".to_vec(), Value::Bytes(vec![1, 2, 3, 4, 0x1a, 0xe1])),
        ]));
        let response = decode_announce(&bencode::encode(&value)).unwrap();
        assert_eq!(response.interval, Duration::from_secs(900));
        assert_eq!(response.peers[0].to_string(), "1.2.3.4:6881");
        let error = Value::Dict(BTreeMap::from([(
            b"failure reason".to_vec(),
            Value::Bytes(b"not allowed".to_vec()),
        )]));
        assert!(decode_announce(&bencode::encode(&error)).is_err());
    }

    #[test]
    fn percent_encodes_every_byte_of_a_binary_identifier() {
        assert_eq!(escape_bytes(&[0, 31, b'A', 255]), "%00%1F%41%FF");
    }
}
