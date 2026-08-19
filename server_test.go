package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

// freeAddr reserves a loopback port and releases it, so the node started next
// can bind it. Nodes advertise their listen address during the handshake, so
// tests cannot simply use port 0.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// testNode is one in-process node with its own database and storage root.
type testNode struct {
	*FileServer
	addr string
	db   *dbpkg.DB
}

func newTestNode(t *testing.T, bootstrap ...string) *testNode {
	t.Helper()
	return newTestNodeAt(t, freeAddr(t), bootstrap...)
}

// portOf returns the port half of a host:port pair.
func portOf(t *testing.T, addr string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("splitting %q: %v", addr, err)
	}
	return port
}

// newTestNodeAt starts a node on a specific listen address, so a test can
// choose how the node is configured to advertise itself.
func newTestNodeAt(t *testing.T, addr string, bootstrap ...string) *testNode {
	t.Helper()

	d, err := dbpkg.Open(filepath.Join(t.TempDir(), "p2p.db"))
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	s, err := makeServerWithDB(addr, d, bootstrap...)
	if err != nil {
		t.Fatalf("makeServerWithDB: %v", err)
	}

	key, err := loadOrInitKey(d)
	if err != nil {
		t.Fatalf("loadOrInitKey: %v", err)
	}
	s.EncryptionKey = key

	if err := s.Listen(); err != nil {
		t.Fatalf("Listen on %s: %v", addr, err)
	}
	go s.Serve()

	t.Cleanup(func() {
		s.Stop()
		d.Close()
	})

	return &testNode{FileServer: s, addr: addr, db: d}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func waitForPeerCount(t *testing.T, n *testNode, want int) {
	t.Helper()
	waitFor(t, fmt.Sprintf("node %s to have %d peer(s)", n.addr, want), 15*time.Second, func() bool {
		return n.peerCount() >= want
	})
}

// randomBytes returns n bytes that no test could match by accident.
func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		t.Fatalf("generating test data: %v", err)
	}
	return b
}

func TestSingleNodeStoreAndGet(t *testing.T) {
	node := newTestNode(t)
	payload := []byte("hello from a lonely node")

	if err := node.Store("greeting", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	_, r, err := node.Get("greeting")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("got %q, want %q", got, payload)
	}
}

func TestStoreRecordsMetadata(t *testing.T) {
	node := newTestNode(t)
	payload := []byte("metadata check")

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
	if files[0].Name != "doc" {
		t.Errorf("Name = %q, want doc", files[0].Name)
	}
	// The recorded size must be the plaintext length, not the on-disk length
	// which carries an extra IV.
	if files[0].Size != int64(len(payload)) {
		t.Errorf("Size = %d, want %d (the plaintext length)", files[0].Size, len(payload))
	}
}

func TestStoreReplicatesToPeer(t *testing.T) {
	origin := newTestNode(t)
	replica := newTestNode(t, origin.addr)

	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, replica, 1)

	payload := randomBytes(t, 4096)
	if err := origin.Store("shared", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Replicas are stored under the digest of their contents.
	hash := contentKey(payload)
	waitFor(t, "the replica to receive the file", 10*time.Second, func() bool {
		return replica.store.Has(hash)
	})

	_, r, err := replica.store.ReadDecrypt(replica.EncryptionKey, hash)
	if err != nil {
		t.Fatalf("reading the replica: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("the replica's contents differ from the original")
	}
}

func TestGetFetchesFromNetwork(t *testing.T) {
	origin := newTestNode(t)
	waitForPeerCount(t, newTestNode(t, origin.addr), 1)

	payload := randomBytes(t, 8192)
	if err := origin.Store("remote-file", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// A third node that has never seen the file must fetch it over the wire.
	fetcher := newTestNode(t, origin.addr)
	waitForPeerCount(t, fetcher, 1)

	if fetcher.store.Has("remote-file") {
		t.Fatal("the fetching node already holds the file; the test proves nothing")
	}

	_, r, err := fetcher.Get("remote-file")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("the fetched contents differ from the original")
	}
}

func TestGetLargeFileFromNetwork(t *testing.T) {
	origin := newTestNode(t)

	// Comfortably larger than the 32KiB copy buffer, so the streaming path is
	// exercised across many reads rather than a single one.
	payload := randomBytes(t, 512*1024)
	if err := origin.Store("big", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	fetcher := newTestNode(t, origin.addr)
	waitForPeerCount(t, fetcher, 1)

	_, r, err := fetcher.Get("big")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("got %d bytes, want %d", len(got), len(payload))
	}
	if !bytes.Equal(got, payload) {
		t.Error("the fetched contents differ from the original")
	}
}

// TestGetWithMultipleRespondersSucceeds covers the case where every peer holds
// the file and answers the same request. The first response wins and the rest
// are drained, rather than a second responder closing an already closed
// channel or overwriting the accepted copy.
func TestGetWithMultipleRespondersSucceeds(t *testing.T) {
	origin := newTestNode(t)
	replicaA := newTestNode(t, origin.addr)
	replicaB := newTestNode(t, origin.addr)

	waitForPeerCount(t, origin, 2)
	waitForPeerCount(t, replicaA, 1)
	waitForPeerCount(t, replicaB, 1)

	payload := randomBytes(t, 32*1024)
	if err := origin.Store("contested", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	hash := contentKey(payload)
	waitFor(t, "both replicas to receive the file", 10*time.Second, func() bool {
		return replicaA.store.Has(hash) && replicaB.store.Has(hash)
	})

	fetcher := newTestNode(t, origin.addr)
	waitForPeerCount(t, fetcher, 3)

	_, r, err := fetcher.Get("contested")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("the fetched contents differ from the original")
	}

	// The node must stay healthy afterwards: a duplicate response that was
	// mishandled would have wedged the connection it arrived on.
	if err := fetcher.Store("after", bytes.NewReader([]byte("still working"))); err != nil {
		t.Errorf("the node is unusable after duplicate responses: %v", err)
	}
}

func TestGetWithNoPeersFailsImmediately(t *testing.T) {
	node := newTestNode(t)

	start := time.Now()
	_, _, err := node.Get("never-stored")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Get for an unknown key returned nil error")
	}
	if !strings.Contains(err.Error(), "no peers connected") {
		t.Errorf("error = %v, want it to say there are no peers", err)
	}
	// There is nobody to ask, so there is nothing to wait for.
	if elapsed > time.Second {
		t.Errorf("Get took %v with no peers connected, want an immediate failure", elapsed)
	}
}

// TestGetNoPeerHasFileFailsFast covers the case where peers are connected but
// none holds the key. Every peer answers the availability query either way, so
// the request can fail as soon as the last one has spoken rather than waiting
// out the download timeout.
func TestGetNoPeerHasFileFailsFast(t *testing.T) {
	first := newTestNode(t)
	second := newTestNode(t, first.addr)
	waitForPeerCount(t, second, 1)
	waitForPeerCount(t, first, 1)

	start := time.Now()
	_, _, err := second.Get("nobody-has-this")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Get for a key no peer holds returned nil error")
	}
	if !strings.Contains(err.Error(), "no peer has") {
		t.Errorf("error = %v, want it to report that no peer holds the key", err)
	}
	if elapsed >= downloadTimeout {
		t.Errorf("Get took %v, want it to fail well before the %v download timeout", elapsed, downloadTimeout)
	}
}

func TestDeletePropagatesToPeers(t *testing.T) {
	origin := newTestNode(t)
	replica := newTestNode(t, origin.addr)

	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, replica, 1)

	payload := []byte("delete me")
	if err := origin.Store("doomed", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	hash := contentKey(payload)
	waitFor(t, "the replica to receive the file", 10*time.Second, func() bool {
		return replica.store.Has(hash)
	})

	if err := origin.Delete("doomed"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if origin.store.Has("doomed") {
		t.Error("the origin still holds the file after deleting it")
	}
	waitFor(t, "the replica to drop the file", 10*time.Second, func() bool {
		return !replica.store.Has(hash)
	})

	files, err := origin.db.ListFiles(context.Background())
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("got %d files in the origin database after delete, want 0", len(files))
	}
}

func TestDeleteUnknownKeySucceeds(t *testing.T) {
	node := newTestNode(t)
	// Deleting something this node never held must not fail; the same code
	// path runs on every peer that receives a broadcast delete.
	if err := node.Delete("never-stored"); err != nil {
		t.Errorf("Delete for an unknown key returned %v, want nil", err)
	}
}

func TestGossipDiscoversIndirectPeers(t *testing.T) {
	// first <- second, then third bootstraps only from first. Gossip should
	// tell third about second without it ever being configured.
	first := newTestNode(t)
	second := newTestNode(t, first.addr)
	waitForPeerCount(t, first, 1)
	waitForPeerCount(t, second, 1)

	third := newTestNode(t, first.addr)

	waitFor(t, "the third node to discover both peers via gossip", 20*time.Second, func() bool {
		return third.peerCount() >= 2
	})

	_, addrs := third.connectedPeers()
	found := false
	for _, a := range addrs {
		if a == second.addr {
			found = true
		}
	}
	if !found {
		t.Errorf("connected peers = %v, want one of them to be %s", addrs, second.addr)
	}
}

func TestSharesRecordedOnBothSides(t *testing.T) {
	origin := newTestNode(t)
	replica := newTestNode(t, origin.addr)

	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, replica, 1)

	if err := origin.Store("tracked", bytes.NewReader([]byte("payload"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	waitFor(t, "the replica to record an incoming share", 10*time.Second, func() bool {
		shares, err := replica.db.ListShares(context.Background())
		return err == nil && len(shares) > 0
	})

	outgoing, err := origin.db.ListShares(context.Background())
	if err != nil {
		t.Fatalf("ListShares: %v", err)
	}
	if len(outgoing) != 1 || outgoing[0].Direction != "outgoing" {
		t.Errorf("origin shares = %+v, want one outgoing record", outgoing)
	}

	incoming, err := replica.db.ListShares(context.Background())
	if err != nil {
		t.Fatalf("ListShares: %v", err)
	}
	if incoming[0].Direction != "incoming" {
		t.Errorf("replica share direction = %q, want incoming", incoming[0].Direction)
	}
}

func TestPeerDisconnectIsObserved(t *testing.T) {
	origin := newTestNode(t)
	replica := newTestNode(t, origin.addr)

	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, replica, 1)

	// Stopping a peer must drop it from the other node's peer set, otherwise
	// broadcasts keep targeting a dead connection.
	replica.Stop()

	waitFor(t, "the origin to notice the peer left", 10*time.Second, func() bool {
		return origin.peerCount() == 0
	})
}

func TestStopIsIdempotent(t *testing.T) {
	node := newTestNode(t)
	node.Stop()
	// The deferred cleanup calls Stop again; a second close of quitch would
	// panic without the guard.
	node.Stop()
}

func TestConcurrentStoresFromOneNode(t *testing.T) {
	origin := newTestNode(t)
	replica := newTestNode(t, origin.addr)

	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, replica, 1)

	// Peer exchange runs on its own goroutine while these stores are in
	// flight, so the connection carries concurrent writers throughout. The
	// payloads are large enough that each transfer takes many writes, which
	// is what gives an unsynchronised implementation room to interleave.
	const files = 8
	payloads := make([][]byte, files)
	for i := range files {
		payloads[i] = randomBytes(t, 256*1024)
	}

	errs := make(chan error, files)
	for i := range files {
		go func(i int) {
			key := fmt.Sprintf("concurrent-%d", i)
			errs <- origin.Store(key, bytes.NewReader(payloads[i]))
		}(i)
	}
	for range files {
		if err := <-errs; err != nil {
			t.Fatalf("Store: %v", err)
		}
	}

	waitFor(t, "every file to reach the replica", 20*time.Second, func() bool {
		for i := range files {
			if !replica.store.Has(contentKey(payloads[i])) {
				return false
			}
		}
		return true
	})
}

// countingPeer records each write separately, so a test can tell one frame
// written whole from a frame assembled by several writes.
type countingPeer struct {
	net.Conn
	mu        sync.Mutex
	writeLock sync.Mutex
	writes    [][]byte

	// body, when set, is what the peer delivers on the stream.
	body io.Reader
}

func (p *countingPeer) Read(b []byte) (int, error) {
	if p.body == nil {
		return 0, io.EOF
	}
	return p.body.Read(b)
}

func (p *countingPeer) Send(b []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writes = append(p.writes, append([]byte(nil), b...))
	return nil
}

func (p *countingPeer) Write(b []byte) (int, error) {
	if err := p.Send(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (p *countingPeer) SendStream(header []byte, body io.Reader) (int64, error) {
	p.writeLock.Lock()
	defer p.writeLock.Unlock()

	p.mu.Lock()
	p.writes = append(p.writes, append([]byte(nil), header...))
	p.mu.Unlock()

	return io.Copy(io.Discard, body)
}

func (p *countingPeer) CloseStream() {}

// Close and RemoteAddr stand in for the embedded net.Conn, which is nil in
// tests that never put this peer on a real socket.
func (p *countingPeer) Close() error { return nil }

func (p *countingPeer) ID() string { return "fake-node" }

func (p *countingPeer) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
}

func (p *countingPeer) writeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.writes)
}

// TestSendMessageWritesOneFrame is a regression test. Messages used to go out
// as three writes: the tag, the length, then the payload. Any concurrent send
// on the same connection could land between them, and the receiver would then
// read a length that belonged to a different message and desynchronise
// permanently.
func TestSendMessageWritesOneFrame(t *testing.T) {
	peer := &countingPeer{}

	msg := Message{Payload: MessageStoreFile{Name: "abc", Digest: contentKey([]byte("abc")), Size: 1234}}
	if err := sendMessage(peer, &msg); err != nil {
		t.Fatalf("sendMessage: %v", err)
	}

	if got := peer.writeCount(); got != 1 {
		t.Fatalf("sendMessage issued %d writes, want exactly 1", got)
	}

	// The single write must be a well-formed frame the decoder accepts.
	var rpc p2p.RPC
	if err := (p2p.DefaultDecoder{}).Decode(bytes.NewReader(peer.writes[0]), &rpc); err != nil {
		t.Fatalf("decoding the frame: %v", err)
	}

	var decoded Message
	if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&decoded); err != nil {
		t.Fatalf("decoding the payload: %v", err)
	}
	store, ok := decoded.Payload.(MessageStoreFile)
	if !ok {
		t.Fatalf("payload type = %T, want MessageStoreFile", decoded.Payload)
	}
	if store.Name != "abc" || store.Size != 1234 {
		t.Errorf("payload = %+v, want name abc size 1234", store)
	}
}

func TestSendMessageRejectsOversizedPayload(t *testing.T) {
	peer := &countingPeer{}

	// A message the far side would refuse to decode must be rejected here,
	// rather than written and then killing the connection.
	msg := Message{Payload: MessageStoreFile{Name: strings.Repeat("k", p2p.MaxMessageSize+1)}}
	if err := sendMessage(peer, &msg); err == nil {
		t.Fatal("an oversized message was sent, want an error")
	}
	if got := peer.writeCount(); got != 0 {
		t.Errorf("issued %d writes for a rejected message, want 0", got)
	}
}

// TestHandleStreamRejectsTruncatedTransfer is a regression test. A peer that
// dies partway through a transfer ends the body early, which reads as a clean
// EOF, so the short file used to be stored and then served on as if complete.
func TestHandleStreamRejectsTruncatedTransfer(t *testing.T) {
	node := newTestNode(t)

	full := randomBytes(t, 4096)
	delivered := full[:100]

	peer := &countingPeer{body: bytes.NewReader(delivered)}

	node.peersLock.Lock()
	node.peers["truncating-peer"] = peer
	node.peersLock.Unlock()

	node.transferLock.Lock()
	node.pendingFileTransfers["truncating-peer"] = MessageStoreFile{
		Name:   "short",
		Digest: contentKey(full),
		Size:   int64(len(full)),
	}
	node.transferLock.Unlock()

	err := node.handleStream("truncating-peer")
	if err == nil {
		t.Fatal("a truncated transfer was accepted")
	}
	if node.store.Has(contentKey(full)) || node.store.Has(contentKey(delivered)) {
		t.Error("the truncated file was left in the store")
	}
}

// TestHandleStreamWithoutAnnouncementReleasesConnection is a regression test.
// An early return skipped CloseStream, and the transport blocks that peer's
// read loop until CloseStream is called, so the connection wedged forever.
func TestHandleStreamWithoutAnnouncementReleasesConnection(t *testing.T) {
	node := newTestNode(t)

	peer := &countingPeer{body: bytes.NewReader(nil)}
	node.peersLock.Lock()
	node.peers["silent-peer"] = peer
	node.peersLock.Unlock()

	// No pending transfer was announced for this peer.
	released := make(chan struct{})
	go func() {
		defer close(released)
		_ = node.handleStream("silent-peer")
	}()

	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("handleStream did not return; the peer connection would be wedged")
	}
}
