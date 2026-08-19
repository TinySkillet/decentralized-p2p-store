package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"io/fs"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

// dialRaw opens a connection and speaks the handshake by hand, so a test can
// present a handshake a well-behaved node never would.
func dialRaw(t *testing.T, addr string, hs Handshake) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })

	if err := gob.NewEncoder(conn).Encode(hs); err != nil {
		t.Fatalf("sending handshake: %v", err)
	}
	return conn
}

// assertNoPeers gives the node a moment to accept the connection, then checks
// it did not.
func assertNoPeers(t *testing.T, node *testNode, why string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.peerCount() > 0 {
			t.Fatalf("node accepted a peer it should have rejected: %s", why)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestHandshakeRejectsProtocolVersionMismatch(t *testing.T) {
	node := newTestNode(t)

	// A future version of the wire format must be refused outright. Accepting
	// it would mean two nodes misreading each other's frames, which shows up
	// much later as corrupt data instead of a refused connection.
	dialRaw(t, node.addr, Handshake{
		Version:    p2p.ProtocolVersion + 1,
		NodeID:     "some-other-node",
		ListenPort: "1234",
	})

	assertNoPeers(t, node, "the peer announced a different protocol version")
}

func TestHandshakeRejectsUnidentifiedPeer(t *testing.T) {
	node := newTestNode(t)

	dialRaw(t, node.addr, Handshake{
		Version:    p2p.ProtocolVersion,
		ListenPort: "1234",
	})

	assertNoPeers(t, node, "the peer sent no node id")
}

func TestHandshakeRejectsPeerWithoutListenPort(t *testing.T) {
	node := newTestNode(t)

	// Without a port there is no way to record an address others can reach.
	dialRaw(t, node.addr, Handshake{
		Version: p2p.ProtocolVersion,
		NodeID:  "some-other-node",
	})

	assertNoPeers(t, node, "the peer advertised no listen port")
}

// TestHandshakeRejectsSelfConnection covers the case gossip eventually
// produces: a node is handed its own address and dials it.
func TestHandshakeRejectsSelfConnection(t *testing.T) {
	node := newTestNode(t)

	if err := node.Transport.Dial(node.addr); err != nil {
		t.Fatalf("Dial: %v", err)
	}

	assertNoPeers(t, node, "the peer is this node itself")
}

// TestPeerAddressIsRoutable is a regression test. A node used to advertise
// whatever was passed to --listen, so a peer configured with a bare ":3000"
// was recorded as ":3000" by everyone. That address resolves to the reader's
// own machine, which made discovery across hosts impossible.
func TestPeerAddressIsRoutable(t *testing.T) {
	// Deliberately configured with a port and no host.
	bare := newTestNodeAt(t, ":"+portOf(t, freeAddr(t)))
	observer := newTestNode(t, "127.0.0.1:"+portOf(t, bare.addr))

	waitForPeerCount(t, observer, 1)

	_, addrs := observer.connectedPeers()
	if len(addrs) != 1 {
		t.Fatalf("got %d peers, want 1", len(addrs))
	}

	addr := addrs[0]
	if strings.HasPrefix(addr, ":") {
		t.Fatalf("peer recorded as %q, which names no host and is not reachable from another machine", addr)
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("recorded address %q is not a host:port pair: %v", addr, err)
	}
	if host == "" {
		t.Errorf("recorded address %q has an empty host", addr)
	}
	if want := portOf(t, bare.addr); port != want {
		t.Errorf("recorded port = %s, want the peer's listen port %s", port, want)
	}

	// The recorded address must be usable: dialling it should reach the peer.
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("the recorded address %q is not dialable: %v", addr, err)
	}
	conn.Close()
}

func TestNodeIDSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2p.db")

	ids := make([]string, 0, 2)
	for range 2 {
		d, err := dbpkg.Open(path)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := d.Migrate(context.Background()); err != nil {
			t.Fatalf("Migrate: %v", err)
		}

		s, err := makeServerWithDB("127.0.0.1:0", d)
		if err != nil {
			t.Fatalf("makeServerWithDB: %v", err)
		}
		ids = append(ids, s.NodeID)
		d.Close()
	}

	if ids[0] == "" {
		t.Fatal("node id is empty")
	}
	// Peers remember this value, so a restart that changed it would look like
	// a brand new node and could no longer recognise a connection to itself.
	if ids[0] != ids[1] {
		t.Errorf("node id changed across restart: %q then %q", ids[0], ids[1])
	}
}

func TestNodeIDsAreDistinctPerNode(t *testing.T) {
	first := newTestNode(t)
	second := newTestNode(t)

	if first.NodeID == second.NodeID {
		t.Fatal("two nodes were given the same identity")
	}
}

// TestOnlyOnePeerStreamsOnGet covers the point of the offer round. Every peer
// holding the file answers the availability query, but exactly one is asked
// to send it, so the same bytes no longer arrive several times over.
func TestOnlyOnePeerStreamsOnGet(t *testing.T) {
	origin := newTestNode(t)
	replicaA := newTestNode(t, origin.addr)
	replicaB := newTestNode(t, origin.addr)

	waitForPeerCount(t, origin, 2)
	waitForPeerCount(t, replicaA, 1)
	waitForPeerCount(t, replicaB, 1)

	payload := randomBytes(t, 16*1024)
	if err := origin.Store("popular", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	hash := contentKey(payload)
	waitFor(t, "both replicas to receive the file", 10*time.Second, func() bool {
		return replicaA.store.Has(hash) && replicaB.store.Has(hash)
	})

	fetcher := newTestNode(t, origin.addr)
	waitForPeerCount(t, fetcher, 3)

	_, r, err := fetcher.Get("popular")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("the fetched contents differ from the original")
	}

	// One accepted transfer means one incoming share. Three would mean all
	// three holders streamed the file.
	shares, err := fetcher.db.ListShares(context.Background())
	if err != nil {
		t.Fatalf("ListShares: %v", err)
	}
	incoming := 0
	for _, s := range shares {
		if s.Direction == "incoming" {
			incoming++
		}
	}
	if incoming != 1 {
		t.Errorf("%d peers streamed the file, want exactly 1", incoming)
	}
}

// TestStoreIsContentAddressed checks that a file's identity is derived from
// its contents, which is what Objective 2 calls for: the hash a name maps to
// must follow the bytes, not the name.
func TestStoreIsContentAddressed(t *testing.T) {
	node := newTestNode(t)
	payload := []byte("content addressed by its digest")

	if err := node.Store("doc", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	files, err := node.db.ListFiles(context.Background())
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("got %d files, want 1", len(files))
	}
	if files[0].Hash != contentKey(payload) {
		t.Errorf("Hash = %q, want the digest of the contents %q", files[0].Hash, contentKey(payload))
	}

	// Storing the same bytes under a different name must give the same hash,
	// and different bytes a different one.
	if err := node.Store("same-bytes", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := node.Store("other", bytes.NewReader([]byte("other contents"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	hashes := map[string]string{}
	files, err = node.db.ListFiles(context.Background())
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	for _, f := range files {
		hashes[f.Name] = f.Hash
	}
	if hashes["doc"] != hashes["same-bytes"] {
		t.Error("identical contents stored under two names produced different hashes")
	}
	if hashes["doc"] == hashes["other"] {
		t.Error("different contents produced the same hash")
	}
}

// TestStoreDeduplicatesIdenticalContents is the deduplication half of
// Objective 2: the same bytes under two names must occupy one file on disk.
func TestStoreDeduplicatesIdenticalContents(t *testing.T) {
	node := newTestNode(t)
	payload := randomBytes(t, 64*1024)

	for _, name := range []string{"first", "second", "third"} {
		if err := node.Store(name, bytes.NewReader(payload)); err != nil {
			t.Fatalf("Store %s: %v", name, err)
		}
	}

	files, err := node.db.ListFiles(context.Background())
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("got %d name records, want 3", len(files))
	}

	// Three names, one blob.
	blobs := countStoredFiles(t, node.store.Root)
	if blobs != 1 {
		t.Errorf("%d files on disk, want 1: identical contents were not deduplicated", blobs)
	}

	// Every name must still resolve to readable contents.
	for _, name := range []string{"first", "second", "third"} {
		_, r, err := node.Get(name)
		if err != nil {
			t.Fatalf("Get %s: %v", name, err)
		}
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll %s: %v", name, err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("%s returned the wrong contents", name)
		}
	}
}

// TestDeleteKeepsContentsSharedWithAnotherName checks the reference counting
// deduplication makes necessary: removing one name must not destroy bytes
// another name still points at.
func TestDeleteKeepsContentsSharedWithAnotherName(t *testing.T) {
	node := newTestNode(t)
	payload := []byte("shared between two names")

	for _, name := range []string{"keeper", "goner"} {
		if err := node.Store(name, bytes.NewReader(payload)); err != nil {
			t.Fatalf("Store %s: %v", name, err)
		}
	}

	if err := node.Delete("goner"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, _, err := node.Get("goner"); err == nil {
		t.Error("the deleted name is still resolvable")
	}

	_, r, err := node.Get("keeper")
	if err != nil {
		t.Fatalf("Get keeper after deleting goner: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("deleting one name corrupted the contents the other name refers to")
	}

	// Removing the last name must reclaim the space.
	if err := node.Delete("keeper"); err != nil {
		t.Fatalf("Delete keeper: %v", err)
	}
	if n := countStoredFiles(t, node.store.Root); n != 0 {
		t.Errorf("%d files left on disk after deleting every name, want 0", n)
	}
}

// countStoredFiles counts the blobs held under a store root.
func countStoredFiles(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return n
}

// TestHandleStreamRejectsSubstitutedContents is what makes a fetch
// self-verifying: the requester asked for a specific digest, so a peer that
// sends anything else is refused and the bytes never become readable.
func TestHandleStreamRejectsSubstitutedContents(t *testing.T) {
	node := newTestNode(t)

	promised := []byte("what the sender promised")
	actual := []byte("what the sender actually sent!")

	peer := &countingPeer{body: bytes.NewReader(actual)}
	node.peersLock.Lock()
	node.peers["lying-peer"] = peer
	node.peersLock.Unlock()

	node.transferLock.Lock()
	node.pendingFileTransfers["lying-peer"] = MessageStoreFile{
		Name:   "tampered",
		Digest: contentKey(promised),
		Size:   int64(len(actual)),
	}
	node.transferLock.Unlock()

	err := node.handleStream("lying-peer")
	if err == nil {
		t.Fatal("contents that did not match the requested digest were accepted")
	}
	if !strings.Contains(err.Error(), "does not match") {
		t.Errorf("error = %v, want a digest mismatch", err)
	}

	// Neither the promised nor the substituted contents may be readable.
	if node.store.Has(contentKey(promised)) {
		t.Error("the store holds something under the promised digest")
	}
	if node.store.Has(contentKey(actual)) {
		t.Error("the substituted contents were stored anyway")
	}
	if n := countStoredFiles(t, node.store.Root); n != 0 {
		t.Errorf("%d files left on disk after a rejected transfer, want 0", n)
	}
}

// TestWriteContentExpectingRejectsMismatch covers the store-level guarantee
// the protocol relies on.
func TestWriteContentExpectingRejectsMismatch(t *testing.T) {
	s := NewStore(StoreOpts{Root: t.TempDir(), PathTransformFunc: CASPathTransformFunc})
	key := mustKey(t)

	actual := []byte("the real bytes")
	wrong := contentKey([]byte("something else entirely"))

	if _, err := s.WriteContentExpecting(key, wrong, bytes.NewReader(actual)); err == nil {
		t.Fatal("WriteContentExpecting accepted contents that did not match")
	}
	if s.Has(contentKey(actual)) {
		t.Error("the rejected contents were stored")
	}
	if n := countStoredFiles(t, s.Root); n != 0 {
		t.Errorf("%d files left behind after a rejected write, want 0", n)
	}

	// The matching case must still succeed.
	if _, err := s.WriteContentExpecting(key, contentKey(actual), bytes.NewReader(actual)); err != nil {
		t.Fatalf("WriteContentExpecting: %v", err)
	}
	if !s.Has(contentKey(actual)) {
		t.Error("matching contents were not stored")
	}
}

// TestOfferForUnknownRequestIsIgnored checks the correlation ids do their job:
// a reply that belongs to no outstanding request is dropped rather than
// satisfying whichever request happens to be waiting.
func TestOfferForUnknownRequestIsIgnored(t *testing.T) {
	node := newTestNode(t)

	err := node.handleMessageFileOffer("some-peer", MessageFileOffer{
		RequestID: "a-request-that-never-existed",
		Name:      "k",
		Have:      true,
		Digest:    contentKey([]byte("k")),
	})
	if err != nil {
		t.Errorf("handleMessageFileOffer returned %v for an unknown request, want nil", err)
	}
}

func TestLocalListenPort(t *testing.T) {
	cases := []struct {
		name string
		src  stubPortSource
		want string
	}{
		{"bound port preferred", stubPortSource{address: ":3000", bound: "127.0.0.1:45678"}, "45678"},
		{"falls back to configured", stubPortSource{address: ":3000"}, "3000"},
		{"ignores an unbound zero port", stubPortSource{address: "127.0.0.1:0", bound: "127.0.0.1:0"}, ""},
		{"host and port", stubPortSource{address: "10.0.0.1:4000"}, "4000"},
		{"nothing usable", stubPortSource{}, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := localListenPort(tc.src); got != tc.want {
				t.Errorf("localListenPort = %q, want %q", got, tc.want)
			}
		})
	}
}

type stubPortSource struct {
	address string
	bound   string
}

func (s stubPortSource) Address() string   { return s.address }
func (s stubPortSource) BoundAddr() string { return s.bound }
