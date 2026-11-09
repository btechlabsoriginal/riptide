use std::io::{self, ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
const MAX_MESSAGE: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub peer_id: [u8; 20],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, block: Vec<u8> },
    Cancel { index: u32, begin: u32, length: u32 },
    Port(u16),
    Unknown { id: u8, payload: Vec<u8> },
}

pub async fn send_handshake<W: AsyncWrite + Unpin>(
    writer: &mut W,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
) -> io::Result<()> {
    let mut packet = [0_u8; 68];
    packet[0] = PROTOCOL.len() as u8;
    packet[1..20].copy_from_slice(PROTOCOL);
    packet[28..48].copy_from_slice(info_hash);
    packet[48..68].copy_from_slice(peer_id);
    writer.write_all(&packet).await
}

pub async fn read_handshake<R: AsyncRead + Unpin>(
    reader: &mut R,
    expected_info_hash: &[u8; 20],
) -> io::Result<Handshake> {
    let mut packet = [0_u8; 68];
    reader.read_exact(&mut packet).await?;
    if packet[0] != 19 || &packet[1..20] != &PROTOCOL[..] {
        return Err(invalid("invalid BitTorrent handshake"));
    }
    if &packet[28..48] != &expected_info_hash[..] {
        return Err(invalid("peer has a different info hash"));
    }
    let mut reserved = [0_u8; 8];
    reserved.copy_from_slice(&packet[20..28]);
    let mut peer_id = [0_u8; 20];
    peer_id.copy_from_slice(&packet[48..68]);
    Ok(Handshake { reserved, peer_id })
}

pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Message> {
    let length = reader.read_u32().await? as usize;
    if length == 0 {
        return Ok(Message::KeepAlive);
    }
    if length > MAX_MESSAGE {
        return Err(invalid("peer message exceeds frame limit"));
    }
    let mut frame = vec![0_u8; length];
    reader.read_exact(&mut frame).await?;
    let id = frame[0];
    let payload = &frame[1..];
    let message = match id {
        0 if payload.is_empty() => Message::Choke,
        1 if payload.is_empty() => Message::Unchoke,
        2 if payload.is_empty() => Message::Interested,
        3 if payload.is_empty() => Message::NotInterested,
        4 if payload.len() == 4 => Message::Have(be_u32(&payload[..4])),
        5 => Message::Bitfield(payload.to_vec()),
        6 | 8 if payload.len() == 12 => {
            let index = be_u32(&payload[0..4]);
            let begin = be_u32(&payload[4..8]);
            let length = be_u32(&payload[8..12]);
            if id == 6 {
                Message::Request { index, begin, length }
            } else {
                Message::Cancel { index, begin, length }
            }
        }
        7 if payload.len() >= 8 && payload.len() <= 8 + 16 * 1024 => Message::Piece {
            index: be_u32(&payload[0..4]),
            begin: be_u32(&payload[4..8]),
            block: payload[8..].to_vec(),
        },
        9 if payload.len() == 2 => Message::Port(u16::from_be_bytes([payload[0], payload[1]])),
        0..=9 => return Err(invalid("malformed peer message payload")),
        _ => Message::Unknown {
            id,
            payload: payload.to_vec(),
        },
    };
    Ok(message)
}

pub async fn send_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &Message,
) -> io::Result<()> {
    let mut frame = Vec::new();
    match message {
        Message::KeepAlive => {}
        Message::Choke => frame.push(0),
        Message::Unchoke => frame.push(1),
        Message::Interested => frame.push(2),
        Message::NotInterested => frame.push(3),
        Message::Have(index) => {
            frame.push(4);
            put_u32(&mut frame, *index);
        }
        Message::Bitfield(bits) => {
            frame.push(5);
            frame.extend_from_slice(bits);
        }
        Message::Request { index, begin, length } => {
            frame.push(6);
            put_u32(&mut frame, *index);
            put_u32(&mut frame, *begin);
            put_u32(&mut frame, *length);
        }
        Message::Piece { index, begin, block } => {
            frame.push(7);
            put_u32(&mut frame, *index);
            put_u32(&mut frame, *begin);
            frame.extend_from_slice(block);
        }
        Message::Cancel { index, begin, length } => {
            frame.push(8);
            put_u32(&mut frame, *index);
            put_u32(&mut frame, *begin);
            put_u32(&mut frame, *length);
        }
        Message::Port(port) => {
            frame.push(9);
            frame.extend_from_slice(&port.to_be_bytes());
        }
        Message::Unknown { id, payload } => {
            frame.push(*id);
            frame.extend_from_slice(payload);
        }
    }
    if frame.len() > MAX_MESSAGE {
        return Err(invalid("outbound peer message exceeds frame limit"));
    }
    writer.write_u32(frame.len() as u32).await?;
    writer.write_all(&frame).await
}

fn be_u32(input: &[u8]) -> u32 {
    u32::from_be_bytes(input.try_into().expect("validated 4-byte field"))
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn handshake_validates_info_hash_and_peer_id() {
        let (mut left, mut right) = duplex(256);
        let hash = [7; 20];
        let id = [9; 20];
        send_handshake(&mut left, &hash, &id).await.unwrap();
        let reply = read_handshake(&mut right, &hash).await.unwrap();
        assert_eq!(reply.peer_id, id);
    }

    #[tokio::test]
    async fn protocol_messages_round_trip_over_length_prefixed_stream() {
        let messages = vec![
            Message::KeepAlive,
            Message::Choke,
            Message::Unchoke,
            Message::Interested,
            Message::NotInterested,
            Message::Have(12),
            Message::Bitfield(vec![0b1010_0000]),
            Message::Request { index: 3, begin: 16_384, length: 8_192 },
            Message::Piece { index: 3, begin: 0, block: vec![0, 1, 255] },
            Message::Cancel { index: 3, begin: 16_384, length: 8_192 },
            Message::Port(6881),
            Message::Unknown { id: 20, payload: vec![0, 1] },
        ];
        let (mut writer, mut reader) = duplex(1024);
        for message in &messages {
            send_message(&mut writer, message).await.unwrap();
        }
        for expected in messages {
            assert_eq!(read_message(&mut reader).await.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn rejects_frame_size_attack() {
        let (mut writer, mut reader) = duplex(16);
        writer.write_u32((MAX_MESSAGE + 1) as u32).await.unwrap();
        assert_eq!(read_message(&mut reader).await.unwrap_err().kind(), ErrorKind::InvalidData);
    }
}
