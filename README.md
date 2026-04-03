# Decentralized P2P Storage

Peer-to-peer file storage with automatic peer discovery, file replication, SQLite-backed metadata, and optional `systemd` service support.

## Features

- Automatic peer discovery via gossip
- Distributed file replication across peers
- SQLite-backed metadata storage
- CLI commands for serving, storing, fetching, listing, deleting, and inspecting peers
- Optional `systemd` service installation

## Build

```bash
go build -o bin/p2p
```

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

Start each node in a separate terminal:

```bash
./bin/p2p serve --listen :3000 --db node_3000/p2p.db
./bin/p2p serve --listen :4000 --db node_4000/p2p.db --bootstrap localhost:3000
./bin/p2p serve --listen :5000 --db node_5000/p2p.db --bootstrap localhost:4000
```

Use node `5000` for client operations:

```bash
echo "Hello P2P World" > hello.txt

./bin/p2p store hello hello.txt --listen :6000 --db node_5000/p2p.db --bootstrap localhost:4000
./bin/p2p files list --db node_5000/p2p.db
./bin/p2p get hello --listen :6000 --db node_5000/p2p.db --bootstrap localhost:4000 --out retrieved_hello.txt
./bin/p2p delete hello --listen :6000 --db node_5000/p2p.db --bootstrap localhost:4000
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
