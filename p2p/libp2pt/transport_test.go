package libp2pt

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

func generateTestKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// newTestTransport starts a transport with a fresh identity on an ephemeral
// loopback port.
func newTestTransport(t *testing.T, opts Opts) *Transport {
	t.Helper()

	if opts.ListenAddr == "" {
		opts.ListenAddr = "127.0.0.1:0"
	}
	if opts.Key == nil {
		_, priv, err := generateTestKey()
		if err != nil {
			t.Fatalf("GenerateKey: %v", err)
		}
		opts.Key = priv
	}

	tr, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tr.ListenAndAccept(); err != nil {
		t.Fatalf("ListenAndAccept: %v", err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

func (t *Transport) nodeID() string {
	pub := ed25519.PrivateKey(t.Key).Public().(ed25519.PublicKey)
	return hex.EncodeToString(pub)
}

func (t *Transport) peerCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.peers)
}

// addrOf names tr the way a caller that knows its identity would.
func addrOf(tr *Transport) p2p.Addr {
	return p2p.Addr{NodeID: tr.nodeID(), Addrs: []string{tr.BoundAddr()}}
}

// frame builds one message frame: the same bytes node.encodeMessage produces.
func frame(payload []byte) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(p2p.IncomingMessage)
	binary.Write(buf, binary.LittleEndian, int64(len(payload)))
	buf.Write(payload)
	return buf.Bytes()
}

func consumeRPC(t *testing.T, tr *Transport) p2p.RPC {
	t.Helper()
	select {
	case rpc := <-tr.Consume():
		return rpc
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an RPC")
		return p2p.RPC{}
	}
}

// dialPeer dials to and returns the dialling side's Peer.
func dialPeer(t *testing.T, from *Transport, to *Transport) p2p.Peer {
	t.Helper()
	got := make(chan p2p.Peer, 1)
	from.OnPeer = func(p p2p.Peer) error { got <- p; return nil }
	if err := from.Dial(addrOf(to)); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case p := <-got:
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the dialling peer")
		return nil
	}
}

// TestNodeIDRoundTrip pins the identity conversion: the canonical hex node id
// survives the trip through libp2p's peer id for any Ed25519 key. If this
// breaks, every previously stored file becomes undeletable, because owners
// are recorded and verified in the hex form.
func TestNodeIDRoundTrip(t *testing.T) {
	for i := 0; i < 100; i++ {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("GenerateKey: %v", err)
		}
		nodeID := hex.EncodeToString(pub)

		pid, err := peerIDForNode(nodeID)
		if err != nil {
			t.Fatalf("peerIDForNode(%s): %v", nodeID, err)
		}
		back, err := nodeIDForPeer(pid)
		if err != nil {
			t.Fatalf("nodeIDForPeer(%s): %v", pid, err)
		}
		if back != nodeID {
			t.Fatalf("round trip changed the id: %s -> %s -> %s", nodeID, pid, back)
		}
	}
}

// TestHostIDMatchesNodeID checks the host derives its peer id from the same
// key bytes the keys table stores, so a node keeps its identity across the
// transport switch.
func TestHostIDMatchesNodeID(t *testing.T) {
	tr := newTestTransport(t, Opts{})

	want, err := peerIDForNode(tr.nodeID())
	if err != nil {
		t.Fatalf("peerIDForNode: %v", err)
	}
	if got := tr.host.ID(); got != want {
		t.Errorf("host id is %s, want %s derived from the persisted key", got, want)
	}
}

func TestMessageRoundTrip(t *testing.T) {
	receiver := newTestTransport(t, Opts{})
	sender := newTestTransport(t, Opts{})

	peer := dialPeer(t, sender, receiver)
	if err := peer.Send(frame([]byte("hello over libp2p"))); err != nil {
		t.Fatalf("Send: %v", err)
	}

	rpc := consumeRPC(t, receiver)
	if string(rpc.Payload) != "hello over libp2p" {
		t.Errorf("payload is %q", rpc.Payload)
	}
	if rpc.From != sender.nodeID() {
		t.Errorf("RPC.From is %q, want the sender's node id %q", rpc.From, sender.nodeID())
	}
}

// TestDialNeedsIdentity pins the documented break with the TCP transport: a
// bare location cannot be dialled, because libp2p cannot connect to an
// address without knowing whose it is.
func TestDialNeedsIdentity(t *testing.T) {
	receiver := newTestTransport(t, Opts{})
	sender := newTestTransport(t, Opts{})

	err := sender.Dial(p2p.Addr{Addrs: []string{receiver.BoundAddr()}})
	if err == nil {
		t.Fatal("Dial with no identity returned nil, want an error")
	}
}

// TestDialByFullMultiaddr covers the bootstrap form this transport does
// accept: a multiaddr whose /p2p/ component names the peer.
func TestDialByFullMultiaddr(t *testing.T) {
	receiver := newTestTransport(t, Opts{})
	sender := newTestTransport(t, Opts{})

	pid, err := peerIDForNode(receiver.nodeID())
	if err != nil {
		t.Fatalf("peerIDForNode: %v", err)
	}
	m, err := multiaddrFrom(receiver.BoundAddr())
	if err != nil {
		t.Fatalf("multiaddrFrom: %v", err)
	}
	full := m.String() + "/p2p/" + pid.String()

	got := make(chan p2p.Peer, 1)
	sender.OnPeer = func(p p2p.Peer) error { got <- p; return nil }
	if err := sender.Dial(p2p.Addr{Addrs: []string{full}}); err != nil {
		t.Fatalf("Dial(%s): %v", full, err)
	}
	select {
	case p := <-got:
		if p.ID() != receiver.nodeID() {
			t.Errorf("connected to %s, want %s", p.ID(), receiver.nodeID())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the peer")
	}
}

func TestSelfDialRefused(t *testing.T) {
	tr := newTestTransport(t, Opts{})
	if err := tr.Dial(addrOf(tr)); err == nil {
		t.Error("dialling this node's own identity returned nil, want an error")
	}
}

// TestNonEd25519PeerRefused: the gater must refuse a peer whose id does not
// embed an Ed25519 key. Without the gate, an RSA peer connects successfully
// and then fails delete authorisation much later, which reads as corruption
// rather than as an unsupported peer.
func TestNonEd25519PeerRefused(t *testing.T) {
	receiver := newTestTransport(t, Opts{})
	registered := make(chan p2p.Peer, 1)
	receiver.OnPeer = func(p p2p.Peer) error { registered <- p; return nil }

	rsaKey, _, err := crypto.GenerateRSAKeyPair(2048, rand.Reader)
	if err != nil {
		t.Fatalf("GenerateRSAKeyPair: %v", err)
	}
	intruder, err := libp2p.New(
		libp2p.Identity(rsaKey),
		libp2p.NoListenAddrs,
	)
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	defer intruder.Close()

	listen, err := multiaddrFrom(receiver.BoundAddr())
	if err != nil {
		t.Fatalf("multiaddrFrom: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = intruder.Connect(ctx, peer.AddrInfo{ID: receiver.host.ID(), Addrs: []ma.Multiaddr{listen}})
	if err == nil {
		// The connection itself may survive on some paths; the peer must
		// still never be admitted.
		if _, serr := intruder.NewStream(ctx, receiver.host.ID(), protocolID); serr == nil {
			t.Error("an RSA-keyed peer opened a protocol stream, want it refused")
		}
	}

	select {
	case p := <-registered:
		t.Fatalf("an RSA-keyed peer was registered as %s, want it refused at the gate", p.ID())
	case <-time.After(500 * time.Millisecond):
	}
}

// TestProtocolVersionMismatch: a peer speaking another protocol version must
// fail to open a stream, not connect and misread frames.
func TestProtocolVersionMismatch(t *testing.T) {
	receiver := newTestTransport(t, Opts{})
	registered := make(chan p2p.Peer, 1)
	receiver.OnPeer = func(p p2p.Peer) error { registered <- p; return nil }

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	key, err := libp2pKey(priv)
	if err != nil {
		t.Fatalf("libp2pKey: %v", err)
	}
	other, err := libp2p.New(libp2p.Identity(key), libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	defer other.Close()

	listen, err := multiaddrFrom(receiver.BoundAddr())
	if err != nil {
		t.Fatalf("multiaddrFrom: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := other.Connect(ctx, peer.AddrInfo{ID: receiver.host.ID(), Addrs: []ma.Multiaddr{listen}}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	wrongVersion := fmt.Sprintf("/p2pstorage/gob/%d", p2p.ProtocolVersion+1)
	if _, err := other.NewStream(ctx, receiver.host.ID(), protocol.ID(wrongVersion)); err == nil {
		t.Error("a stream opened despite the protocol version mismatch")
	}

	select {
	case p := <-registered:
		t.Fatalf("peer %s was registered despite never opening the protocol stream", p.ID())
	case <-time.After(500 * time.Millisecond):
	}
}

// TestSimultaneousOpen: both sides dial each other at once. Exactly one
// stream must survive on each side — and the same one, or the two nodes
// disagree about being connected.
func TestSimultaneousOpen(t *testing.T) {
	a := newTestTransport(t, Opts{})
	b := newTestTransport(t, Opts{})

	errs := make(chan error, 2)
	go func() { errs <- a.Dial(addrOf(b)) }()
	go func() { errs <- b.Dial(addrOf(a)) }()
	for i := 0; i < 2; i++ {
		// A dial can lose the race and fail; what matters is the state that
		// settles, checked below.
		<-errs
	}

	// The tie-break plays out asynchronously: counts pass through 1/1 while
	// the losing stream is still being replaced, so the only stable check is
	// that a message actually round-trips on whatever survived.
	assertSettles(t, a, b, "from a")
	assertSettles(t, b, a, "from b")

	if a.peerCount() != 1 || b.peerCount() != 1 {
		t.Errorf("peer counts after settling: a has %d, b has %d, want 1 and 1", a.peerCount(), b.peerCount())
	}
}

// assertSettles retries a send from's single peer until one is delivered:
// during simultaneous-open settling a grabbed peer may be replaced mid-send.
func assertSettles(t *testing.T, from, to *Transport, payload string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for attempt := 0; ; attempt++ {
		if time.Now().After(deadline) {
			t.Fatalf("no message got through: %d peers on the sending side after every attempt", from.peerCount())
		}

		from.mu.Lock()
		var peer p2p.Peer
		for _, p := range from.peers {
			peer = p
		}
		from.mu.Unlock()
		if peer == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		want := fmt.Sprintf("%s #%d", payload, attempt)
		if err := peer.Send(frame([]byte(want))); err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		timeout := time.After(time.Second)
	drain:
		for {
			select {
			case rpc := <-to.Consume():
				if string(rpc.Payload) == want {
					return
				}
				// An earlier attempt's message; keep draining.
			case <-timeout:
				break drain
			}
		}
	}
}

// TestStreamIsIndivisible: a message sent concurrently with a file transfer
// must not interleave into the middle of the body.
func TestStreamIsIndivisible(t *testing.T) {
	receiver := newTestTransport(t, Opts{})
	sender := newTestTransport(t, Opts{})

	arrived := make(chan p2p.Peer, 1)
	receiver.OnPeer = func(p p2p.Peer) error { arrived <- p; return nil }

	sendPeer := dialPeer(t, sender, receiver)
	var recvPeer p2p.Peer
	select {
	case recvPeer = <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver never saw the peer")
	}

	body := make([]byte, 1<<20)
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	// Hammer messages while the stream is in flight.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				sendPeer.Send(frame([]byte("interleaver")))
			}
		}
	}()

	// SendStream must run concurrently with the drain below: yamux flow
	// control blocks the body until the receiver reads it.
	sent := make(chan error, 1)
	go func() {
		_, err := sendPeer.SendStream(frame([]byte("announce")), bytes.NewReader(body))
		close(stop)
		sent <- err
	}()

	// Drain until the stream frame; every earlier frame is a whole message.
	for {
		rpc := consumeRPC(t, receiver)
		if !rpc.Stream {
			if s := string(rpc.Payload); s != "interleaver" && s != "announce" {
				t.Fatalf("a frame was corrupted: %q", s)
			}
			continue
		}
		break
	}

	got := make([]byte, len(body))
	if _, err := io.ReadFull(recvPeer, got); err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	recvPeer.CloseStream()

	if err := <-sent; err != nil {
		t.Fatalf("SendStream: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("the body was corrupted in flight")
	}
}

// TestCloseStreamUnblocksReadLoop: after a stream body is consumed,
// CloseStream must resume the read loop so later messages flow.
func TestCloseStreamUnblocksReadLoop(t *testing.T) {
	receiver := newTestTransport(t, Opts{})
	sender := newTestTransport(t, Opts{})

	arrived := make(chan p2p.Peer, 1)
	receiver.OnPeer = func(p p2p.Peer) error { arrived <- p; return nil }

	sendPeer := dialPeer(t, sender, receiver)
	recvPeer := <-arrived

	body := []byte("small body")
	if _, err := sendPeer.SendStream(frame([]byte("announce")), bytes.NewReader(body)); err != nil {
		t.Fatalf("SendStream: %v", err)
	}

	rpc := consumeRPC(t, receiver)
	if !rpc.Stream {
		// The announce frame decodes first.
		rpc = consumeRPC(t, receiver)
	}
	if !rpc.Stream {
		t.Fatal("no stream RPC arrived")
	}

	got := make([]byte, len(body))
	if _, err := io.ReadFull(recvPeer, got); err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	recvPeer.CloseStream()

	if err := sendPeer.Send(frame([]byte("after the stream"))); err != nil {
		t.Fatalf("Send after stream: %v", err)
	}
	after := consumeRPC(t, receiver)
	if string(after.Payload) != "after the stream" {
		t.Errorf("read loop delivered %q after the stream", after.Payload)
	}
}

// TestReconnectAfterRestartWithNewAddress: the same identity comes back on a
// different port — the case an address-keyed system could not even express.
func TestReconnectAfterRestartWithNewAddress(t *testing.T) {
	sender := newTestTransport(t, Opts{})

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	first := newTestTransport(t, Opts{Key: priv})
	firstPeer := dialPeer(t, sender, first)
	if firstPeer.ID() != first.nodeID() {
		t.Fatalf("connected to %s, want %s", firstPeer.ID(), first.nodeID())
	}
	oldAddr := first.BoundAddr()
	first.Close()

	// Wait for the sender to notice the peer is gone.
	deadline := time.Now().Add(5 * time.Second)
	for sender.peerCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the sender never noticed the peer left")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The node restarts: same key, different port.
	second := newTestTransport(t, Opts{Key: priv})
	if second.BoundAddr() == oldAddr {
		t.Fatalf("the restarted node bound the same address %s, the test needs a new one", oldAddr)
	}

	secondPeer := dialPeer(t, sender, second)
	if secondPeer.ID() != firstPeer.ID() {
		t.Errorf("the restarted node has id %s, want the same identity %s", secondPeer.ID(), firstPeer.ID())
	}
}
