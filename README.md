# Riptide

A terminal-first BitTorrent v1 client written in Rust. The implementation is intentionally small enough to navigate, but the fundamentals are real: exact metainfo hashing, compact tracker announces, TCP peer sessions, rarest-first block scheduling, hash-verified writes, and a resume journal that does not blindly trust the files on disk.

Riptide downloads actual `.torrent` files. It does not generate sample output or emulate network progress.

## Build

Requires a current stable Rust toolchain, Cargo, network access for dependencies and torrents, and a terminal for the interactive view.

```sh
cargo build --release
cargo test
```

The binary is `target/release/riptide`.

## Usage

```sh
cargo run --release -- ./example.torrent
cargo run --release -- ./example.torrent --output ~/Downloads/torrents
cargo run --release -- ./example.torrent --output ./downloads --connections 32
cargo run --release -- ./example.torrent --output ./downloads --no-tui
cargo run --release -- ./example.torrent --output ./downloads --resume ./session.json
```

Run the same command again to resume an interrupted download. `q`, Escape, and Ctrl-C stop the client. When standard input or output is redirected, it prints periodic progress lines instead of activating the terminal UI.

| Option | Purpose |
| --- | --- |
| `torrent` | Path to a local BitTorrent v1 `.torrent` file |
| `-o, --output` | Root directory for downloaded files, default `./downloads` |
| `-c, --connections` | Concurrent outbound peer slots, from 1 through 128, default 24 |
| `--resume` | Override the resume journal path |
| `--no-tui` | Plain-text progress, suitable for logs and scripts |

By default, Riptide writes its journal to `OUTPUT/.riptide/<info-hash>.json`. It contains the torrent's info hash and the indices of completed pieces. On restart, each recorded piece is read from disk and checked against its original SHA-1 before being marked complete. Invalid or missing pieces are downloaded again. The journal is written using a temporary file and rename, after syncing torrent data.

The client creates the target file lengths up front and only writes piece data after a complete SHA-1 match. A piece may cross several files, including files in nested directories. On filesystems supporting sparse allocation, the unwritten ranges remain holes until data arrives.

## Legal test torrents

Download an official `.torrent` file from a project that explicitly distributes its material over BitTorrent:

- [Debian installation images](https://www.debian.org/CD/torrent-cd/) — choose an official Debian image torrent.
- [Fedora Project torrent downloads](https://torrent.fedoraproject.org/) — Fedora Linux releases are distributed as torrents.

Some official torrents advertise only UDP trackers. This implementation currently supports **HTTP and HTTPS trackers**, so check that the chosen `.torrent` contains at least one HTTP(S) announce URL. A torrent's `announce-list` may contain both kinds; Riptide uses its HTTP(S) entries. For a small first test, a Debian CD image is generally more practical than a multi-gigabyte DVD.

Use torrents you are authorised to download and share. BitTorrent itself is a neutral transport protocol; the rights to the content matter.

## Project map

```text
riptide/
├── Cargo.toml
├── README.md
├── LICENSE
└── src/
    ├── lib.rs       Library entry point and public modules
    ├── main.rs      CLI, progress dashboard, termination
    ├── bencode.rs   Strict encoder/decoder, raw dictionary value offsets
    ├── torrent.rs   BitTorrent v1 metadata, paths, SHA-1 info hash
    ├── tracker.rs   HTTP(S) announces, compact IPv4/IPv6 peer decoding
    ├── peer.rs      Handshake, framing, protocol messages
    ├── picker.rs    Rarest-first scheduling, 16 KiB block lifecycle
    ├── storage.rs   Sparse file layout, cross-file reads/writes, SHA-1
    └── client.rs    Connection supervision, peer workers, journal, sessions
```

## How transfers work

1. Parse the torrent using strict bencode rules, retaining the exact byte span of `info` for the SHA-1 info hash.
2. Validate all torrent file names and components before creating the destination layout. Reject traversal paths and symlink escapes.
3. Check the resume journal against data already on disk; never mark a piece verified based only on the journal.
4. Start an inbound TCP listener on an automatically selected port. Announce the actual port and binary identifiers to HTTP(S) trackers.
5. Connect to returned peers, exchange BitTorrent handshakes, advertise the local bitfield, and track peer `have`/`bitfield` availability.
6. Choose the rarest available unfinished piece, preferring previously started pieces when rarity is tied. Assign each block to one peer at a time, with six outstanding requests per connection.
7. Requeue a peer's requested blocks on disconnect. Keep already received blocks in their piece buffers.
8. Assemble finished pieces, compare the SHA-1 against the metainfo, write verified data, and update the checkpoint.
9. Send `have` updates to connected peers and serve block requests for pieces already verified. Stop once every piece has been checked and stored.

## Tests

```sh
cargo test
cargo test bencode
cargo test picker
cargo test storage
cargo test peer
```

The tests cover nested/binary bencode round trips, canonical representation failures, info-hash byte spans, path safety, compact IPv4/IPv6 trackers, peer message framing, handshake validation, rarest-first decisions, disconnect requeueing, partial blocks, cross-file verified writes, tampering, resume revalidation, and an end-to-end loopback download using a local HTTP tracker and a mock peer.

## Scope and trade-offs

Riptide targets straightforward **BitTorrent v1** downloads over HTTP(S) trackers and TCP peers. It handles single-file and multi-file torrents, incoming and outbound peer sessions, block requests and uploads during downloads, and peer message keepalives. It does not implement magnet links, DHT, UDP trackers, uTP, PEX, encrypted transports, BitTorrent v2, a dedicated seeding mode after completion, automatic tracker interval reannounces, or an endgame duplicate-request algorithm.

Peer discovery is a bounded tracker pass followed by attempts to connect to the discovered peers. If a swarm is unreachable or too sparse, the client can exit with an incomplete transfer; rerunning resumes already verified pieces. The UI's rate counter measures received block bytes, while the progress bar counts only fully verified data. SHA-1 is mandated by BitTorrent v1's piece hashes, not chosen as a modern general-purpose security primitive.

This is a foundation for a compact, inspectable downloader rather than a replacement for mature clients such as Transmission or qBittorrent. Network and filesystem failures are surfaced rather than silently interpreted as success.
