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
