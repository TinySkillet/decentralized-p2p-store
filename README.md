# Decentralized P2P Storage

Peer-to-peer file storage with gossip-based peer discovery, file replication,
SQLite-backed metadata, and optional `systemd` service support.

## Releases

This project began as an undergraduate thesis and has continued since. Tags
mark the states worth distinguishing:

| Tag | What it is |
| --- | --- |
| `v1.0-thesis` | The artifact as the thesis describes it. Every claim in the report holds of this code. |
| `v1.1-durability` | Adds a replication target with automatic repair. |
| `v1.2-consistency` | Fixes the data-integrity bugs found by auditing the two releases above. |
| `v1.3-identity` | Peers prove who they are, and deleting a file requires its owner's authorisation. |
| `v1.4-control` | Commands ask the running node to act instead of starting one of their own. |
| _unreleased_ | Peers keyed by identity rather than address; repair works through every file; re-storing a deleted file sticks. Existing databases are migrated in place on first start. |

**Run the newest tag.** The earlier tags are kept as reference
points, not as versions to deploy.

### Known issues in `v1.0-thesis`

Auditing the submitted artifact afterwards turned up six bugs that could lose
or misreport data. They do not affect the report's claims, which hold of that
code as written, but they matter if you intend to run it.

| Issue | Fixed in |
| --- | --- |
| A copy arriving from a peer under a name already used here overwrote the local mapping, so any peer could silently repoint one of your files at its own contents. | `v1.1` |
| Two processes sharing a database could each generate an encryption key and overwrite the other's, leaving files encrypted under the loser undecryptable. | `v1.2` |
| The same race on node identity, which peers remember and use to recognise a connection back to a node. | `v1.2` |
| Replacing a file left the previous contents on disk with nothing referring to them, and nothing reclaimed the space. | `v1.2` |
| Deleting a name decided the contents were unreferenced and then unlinked them as two separate steps, so a name recorded in between was left pointing at deleted data. | `v1.2` |
| A deletion travelled by name alone, so deleting your file also deleted a peer's different file of the same name. | `v1.2` |

### Known issues in `v1.1-durability`

Automatic repair introduced one problem of its own, alongside the `v1.0` issues
above that were still outstanding:

| Issue | Fixed in |
| --- | --- |
| A peer offline during a deletion still held the file, and its repair cycle offered it back to every node that had removed it, undoing the deletion across the network. | `v1.2` |

Every issue listed here has a regression test that fails against the code
before its fix.

## Features

- Peer discovery via gossip between connected nodes
- File replication to the peers a node is connected to
- Content-addressed storage: files are identified by the SHA-256 of their
  contents, so identical bytes are stored once however many names refer to them
- A replication target with automatic repair, so files that fall below it are
  copied back up as peers come and go
- Cryptographic peer identity: a node is named by its public key and proves it
  holds the matching private key before any file traffic is allowed
- Deletion requires the file owner's signed authorisation
- AES-256-CTR encryption of files at rest
- SQLite-backed metadata storage
- CLI commands for serving, storing, fetching, listing, deleting, and inspecting peers
- Optional `systemd` service installation

## Protocol

Every connection opens with a handshake that settles four things: that both
sides speak the same protocol version, that the peer holds the private key for
the identity it claims, that it is not this node reached by a roundabout route,
and what address other nodes should use to reach it.

A node is named by its Ed25519 public key. Each side sends a random challenge
and signs the other's, over a transcript naming both parties, so a captured
handshake is useless on another connection. A version mismatch ends the
connection rather than letting two builds misread each other's frames. The
receiving node pairs the advertised port with the address the connection
actually arrived from, so a node configured with a bare `:3000` is still
recorded at an address its peers can reach.

Proving identity is what makes the rest meaningful: an unverified name would
make any decision about what a peer is allowed to do worthless.

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

## Durability

A file is replicated to every connected peer when it is stored, which is only
as durable as whoever happened to be online at that moment. Each node also
runs a repair cycle: for the files it holds, it asks who else has them and
offers copies to peers that do not, until the replication target is met.

The target defaults to 3 and is set with `--replicas` on `serve`, or `replicas`
in the config file. `p2p status` shows which files are below it. A target
larger than the network can satisfy is reported rather than retried forever.

Repair is deduplication-aware: several names sharing one blob cost one check
and one transfer, not one per name.

A cycle checks a bounded number of files so that a large store does not flood
its peers with availability queries in one burst, and resumes where the previous
cycle stopped. That position is persisted, so a node holding more files than one
cycle covers still works through all of them rather than re-checking the same
few for ever.

## Deletion

Deleting a file removes the name that refers to it. Because several names can
share one set of contents, the bytes are reclaimed only once no name refers to
them, by a sweep that treats the name mapping as the single source of truth.
Deciding data is unreachable and removing it happen together there, so a name
recorded at the same moment cannot be left pointing at deleted data.

A deletion is recorded as a tombstone, which is what makes it stick. Peers that
were offline when it was broadcast still hold the file, and their repair cycle
would otherwise offer it back and undo the deletion everywhere. Instead the
offer is refused and the straggler is told, so the deletion reaches it late
rather than never.

Storing a file again after deleting it clears the tombstone here, and a peer
that still holds one cannot undo that: an authorisation stays valid for ever, so
a node that owns a file and holds no deletion record for it treats an incoming
authorisation as a stale replay and keeps the file.

Deletions carry the content digest as well as the name. Two nodes may
legitimately use the same name for different files, and a peer only acts when
its own name refers to the same contents.

Every file records the identity that stored it, and a deletion must carry that
owner's signature over the name and digest together. Reaching a peer is not by
itself permission to destroy what it holds. The authorisation is kept with the
tombstone and replayed to peers that still hold the file, so a relaying node
does not have to be trusted. Files stored before ownership was recorded have no
owner and accept unsigned deletions, so upgrading a network does not strand
data nobody can remove.

Ownership follows the database rather than the process. A one-shot command joins
the network under a throwaway key, so the node whose database it borrows does
not refuse the connection as one to itself, but the files it stores belong to
that database's identity.

What deletion does not promise: a peer that is unreachable keeps its copy until
it next contacts a node that knows about the deletion.

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
- **Anyone who can complete the handshake may store files.** Identity is
  proven and deletion is authorised, but there is no admission control on
  incoming data and no quota, so a peer can fill a node's disk. Joining still
  depends on knowing a bootstrap address, and the number of identities accepted
  from one host is capped so a single machine cannot flood the peer table.
  Loopback is exempt, so several nodes can share a machine for local testing.
- **Anyone with the database can act as its owner.** Signing authority lives in
  the database beside the encryption key, so file permissions are only as
  strong as the filesystem permissions on that file.
- **NAT is not handled.** A node behind NAT advertises a port that may not be
  reachable from outside its network.

## How commands reach a node

A running node accepts commands on a unix socket beside its database, and
`store`, `get`, `delete`, `status` and `repair` use it when one is there. That
means they need no `--listen` or `--bootstrap`:

```bash
p2p store report report.pdf --db ~/.p2p/p2p.db
p2p status --db ~/.p2p/p2p.db
```

Earlier releases had commands start a second node against the running node's
database and storage. That produced a run of bugs: two writers racing on the
same row, replica counts that treated the borrowed storage as an extra copy,
and files owned by a key that vanished when the command exited. Asking the node
that owns the data to act removes the whole class.

When no node is running, commands still start a temporary one and do the work
themselves. That path is safe precisely when it is taken: the hazard was ever
sharing storage with a running node, and that is exactly the case where the
socket exists.

The socket carries no authentication and grants full authority, including
deleting files as their owner. It is created with owner-only permissions, which
is the same boundary as the database beside it. Where the database path is too
long for a unix socket, the socket moves to the user's runtime directory under a
name derived from that path, so node and commands still agree where to meet.

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
- `store <key> <file> [--db <path>]`
  Stores a local file under a key and broadcasts it to peers.
- `get <key> [--db <path>] [--out <file>]`
  Fetches a file by key from the local node or the network. If `--out` is omitted, the file is written to stdout.
- `delete <key> [--db <path>]`
  Deletes a file by key locally and propagates the deletion to peers.
- `files list [--db <path>]`
  Lists files known to the local database.
- `shares [--db <path>]`
  Lists file share records for files stored on other peers.
- `peers [--db <path>]`
  Lists known peers and their last seen status.
- `status [--db <path>] [--replicas <n>]`
  Reports how many copies of each local file the network holds, and which are
  below the replication target.
- `repair [--db <path>] [--replicas <n>]`
  Places copies of under-replicated files immediately, rather than waiting for
  the next automatic cycle.
- `cleanup [--db <path>]`
  Removes stale peer records from the database.
- `demo`
  Starts three nodes in temporary directories, stores a file on one, fetches it
  from another, then deletes it and confirms it is gone.

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
