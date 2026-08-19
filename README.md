# Decentralized P2P Storage

Peer-to-peer file storage with gossip-based peer discovery, file replication,
SQLite-backed metadata, and optional `systemd` service support.

## Features

- Peer discovery via gossip between connected nodes
- File replication to the peers a node is connected to
- AES-256-CTR encryption of files at rest
- SQLite-backed metadata storage
- CLI commands for serving, storing, fetching, listing, deleting, and inspecting peers
- Optional `systemd` service installation

## Current limitations

The project is a work in progress. What it does not yet do:

- **Files travel between peers in plaintext.** Encryption is applied at rest
  only, and each node holds its own key in the SQLite database beside the data
  it protects.
- **Peers are not authenticated.** Any node that can connect may store files
  or broadcast a deletion.
- **Transfers are not integrity checked.** A short transfer is detected by its
  announced length, but contents are not verified against a digest.
- **Replication has no target factor and no repair.** A file goes to whichever
  peers happen to be connected when it is stored; nothing re-replicates it if
  those peers later leave.
- **Nodes advertise the address given to `--listen`.** A port-only value such
  as `:3000` is not routable from another machine, so discovery across hosts
  needs an explicit `host:port`.

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
the TCP transport and frame decoder, and a set of multi-node tests that bring
several real nodes up on loopback ports to exercise store, fetch, delete and
gossip end to end. The transport and server are concurrent throughout, so
`make test-race` is the run that matters.

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
