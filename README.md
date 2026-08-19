# Decentralized P2P Storage

Peer-to-peer file storage with gossip-based peer discovery, file replication,
SQLite-backed metadata, and optional `systemd` service support.

## Features

- Peer discovery via gossip between connected nodes
- File replication to the peers a node is connected to
- Content-addressed storage: files are identified by the SHA-256 of their
  contents, so identical bytes are stored once however many names refer to them
- AES-256-CTR encryption of files at rest
- SQLite-backed metadata storage
- CLI commands for serving, storing, fetching, listing, deleting, and inspecting peers
- Optional `systemd` service installation

## Protocol

Every connection opens with a handshake carrying a protocol version, the
node's identity, and the port it listens on. A version mismatch ends the
connection rather than letting two builds misread each other's frames. The
receiving node pairs the advertised port with the address the connection
actually arrived from, so a node configured with a bare `:3000` is still
recorded at an address its peers can reach. Identity is what lets a node
recognise a connection back to itself, which gossip eventually produces.

After the handshake, each frame begins with a tag: a length-prefixed message,
or a file body. A fetch runs in two rounds:

1. The requester asks every peer whether it holds a name, tagging the question
   with a request id.
2. Every peer answers either way, resolving the name to the digest of the
   contents it holds for it. A name nobody holds therefore fails as soon as
   the last peer has spoken, rather than waiting out a timeout.
3. Exactly one peer that answered yes is asked for those contents, by digest.
   Only that peer streams, so the same bytes do not arrive several times over.
4. The announcement and the file body are written as one indivisible transfer.
   The receiver hashes the bytes as they arrive and only moves the file into
   place if it matches the digest that was asked for, so data that fails
   verification never becomes readable.

Because the second round names a digest rather than a file, the reply is
self-verifying: a peer cannot substitute different contents for the ones that
were requested.

## Storage layout

A file's identity is the SHA-256 of its plaintext. That digest is sharded into
nested directories, so a digest of `a1b2c3d4e5...` is stored at
`ROOT/a1b2c/3d4e5/a1b2c3d4e5...`. Spreading files across many directories
keeps any single one small enough for fast lookups.

Because the path follows the contents, storing the same bytes under two names
writes one file. The database maps each name to the digest it refers to, and
the bytes are removed only when the last name pointing at them is deleted.

## Current limitations

The project is a work in progress. What it does not yet do:

- **Files travel between peers in plaintext.** Encryption is applied at rest
  only, and each node holds its own key in the SQLite database beside the data
  it protects.
- **Peers are not authenticated.** Any node that can complete the handshake
  may store files or broadcast a deletion. Contents cannot be substituted, as
  a fetch is verified against the digest it asked for, but a peer can still
  answer an availability query with a digest for contents nobody asked for.
  The system assumes the out-of-band trust of a private group.
- **Replication has no target factor and no repair.** A file goes to whichever
  peers happen to be connected when it is stored; nothing re-replicates it if
  those peers later leave.
- **NAT is not handled.** A node behind NAT advertises a port that may not be
  reachable from outside its network.

## Build

```bash
make build          # builds bin/p2p
```

## Tests

```bash
make test           # unit and multi-node tests
make test-race      # the same suite under the race detector
make check          # gofmt, go vet, then the race suite
```

The suite covers the storage layer, the crypto helpers, the SQLite repository,
the TCP transport and frame decoder, the handshake and fetch protocol, and a
set of multi-node tests that bring several real nodes up on loopback ports to
exercise store, fetch, delete and gossip end to end. The transport and server
are concurrent throughout, so `make test-race` is the run that matters.

## CLI

The binary is invoked as:

```bash
./bin/p2p <command> [arguments] [flags]
```

Common flags:

- `--db <path>`: SQLite database path. Defaults to `p2p.db`.
- `--listen <addr>`: Local listen address for commands that start a temporary node, for example `:3000`.
- `--bootstrap <host:port>`: One or more peer addresses to join an existing network.

Command formats:

- `serve --listen <addr> [--db <path>] [--bootstrap <host:port>] [--config <path>]`
  Starts a node and keeps it running. Use this for long-lived peers.
- `store <key> <file> --listen <addr> [--db <path>] [--bootstrap <host:port>]`
  Stores a local file under a key and broadcasts it to peers.
- `get <key> --listen <addr> [--db <path>] [--bootstrap <host:port>] [--out <file>]`
  Fetches a file by key from the local node or the network. If `--out` is omitted, the file is written to stdout.
- `delete <key> --listen <addr> [--db <path>] [--bootstrap <host:port>]`
  Deletes a file by key locally and propagates the deletion to peers.
- `files list [--db <path>]`
  Lists files known to the local database.
- `shares [--db <path>]`
  Lists file share records for files stored on other peers.
- `peers [--db <path>]`
  Lists known peers and their last seen status.
- `cleanup [--db <path>]`
  Removes stale peer records from the database.
- `demo`
  Starts the built-in local three-node demo flow.

## Local Testing

Use the helper script to prepare three local nodes:

```bash
./setup_local_nodes.sh
```

Start each node in a separate terminal. Nodes advertise whatever is passed to
`--listen`, so use an explicit host when peers are on different machines:

```bash
./bin/p2p serve --listen 127.0.0.1:3000 --db node_3000/p2p.db
./bin/p2p serve --listen 127.0.0.1:4000 --db node_4000/p2p.db --bootstrap 127.0.0.1:3000
./bin/p2p serve --listen 127.0.0.1:5000 --db node_5000/p2p.db --bootstrap 127.0.0.1:4000
```

Each node shuts down cleanly on Ctrl-C.

Use node `5000` for client operations:

```bash
echo "Hello P2P World" > hello.txt

./bin/p2p store hello hello.txt --listen 127.0.0.1:6000 --db node_5000/p2p.db --bootstrap 127.0.0.1:4000
./bin/p2p files list --db node_5000/p2p.db
./bin/p2p get hello --listen 127.0.0.1:6000 --db node_5000/p2p.db --bootstrap 127.0.0.1:4000 --out retrieved_hello.txt
./bin/p2p delete hello --listen 127.0.0.1:6000 --db node_5000/p2p.db --bootstrap 127.0.0.1:4000
./bin/p2p peers --db node_5000/p2p.db
```

## Systemd

Install the service with:

```bash
./install.sh
```

The installer builds the binary, copies `DecentralizedP2PStorage` to `/usr/local/bin`, creates `~/.p2p/config` if it does not exist, and installs `p2p-storage@.service`.

Common commands:

```bash
sudo systemctl start p2p-storage@$USER
sudo systemctl enable p2p-storage@$USER
sudo systemctl status p2p-storage@$USER
sudo journalctl -u p2p-storage@$USER -f
```

Service configuration lives in `~/.p2p/config`.

To remove the service and binary:

```bash
./uninstall.sh
```
