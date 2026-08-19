package p2p

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestTransport starts a transport on an ephemeral loopback port.
func newTestTransport(t *testing.T, opts TCPTransportOpts) *TCPTransport {
	t.Helper()

	opts.ListenAddr = "127.0.0.1:0"
	if opts.HandshakeFunc == nil {
		// RPC.From carries the peer's identity, which only a handshake can
		// establish, so the default here assigns one rather than leaving
		// every peer anonymous.
		opts.HandshakeFunc = assignTestIdentity
	}
	if opts.Decoder == nil {
		opts.Decoder = DefaultDecoder{}
	}

	tr := NewTCPTransport(opts)
	if err := tr.ListenAndAccept(); err != nil {
		t.Fatalf("ListenAndAccept: %v", err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

// assignTestIdentity is a handshake that gives the peer a unique identity
// without exchanging anything, standing in for the real one.
func assignTestIdentity(p any) error {
	peer, ok := p.(*TCPPeer)
	if !ok {
		return fmt.Errorf("unexpected peer type %T", p)
	}
	peer.NodeID = fmt.Sprintf("test-node-%d", atomic.AddInt64(&testIdentityCounter, 1))
	return nil
}

var testIdentityCounter int64

// recvRPC waits for one RPC or fails the test.
func recvRPC(t *testing.T, tr *TCPTransport) RPC {
	t.Helper()
	select {
	case rpc := <-tr.Consume():
		return rpc
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for an RPC")
		return RPC{}
	}
}

// dialPeer dials addr and returns the dialling side's Peer.
func dialPeer(t *testing.T, sender *TCPTransport, addr string) Peer {
	t.Helper()
	got := make(chan Peer, 1)
	sender.OnPeer = func(p Peer) error { got <- p; return nil }
	if err := sender.Dial(addr); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case p := <-got:
		return p
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the dialling peer")
		return nil
	}
}

// messageFrame builds a wire-format message frame around body.
func messageFrame(body []byte) []byte {
	var wire bytes.Buffer
	wire.WriteByte(IncomingMessage)
	binary.Write(&wire, binary.LittleEndian, int64(len(body)))
	wire.Write(body)
	return wire.Bytes()
}

func TestTransportListenAndAcceptBindsEphemeralPort(t *testing.T) {
	tr := newTestTransport(t, TCPTransportOpts{})

	if tr.BoundAddr() == "" || tr.BoundAddr() == "127.0.0.1:0" {
		t.Errorf("BoundAddr = %q, want a concrete port", tr.BoundAddr())
	}
	// Address still reports what peers are told, not the bound socket.
	if tr.Address() != "127.0.0.1:0" {
		t.Errorf("Address = %q, want the configured listen address", tr.Address())
	}
}

func TestTransportCloseIsSafeBeforeListening(t *testing.T) {
	// Commands construct a transport and may abort before listening; Close
	// must not dereference a nil listener.
	tr := NewTCPTransport(TCPTransportOpts{ListenAddr: "127.0.0.1:0"})
	if err := tr.Close(); err != nil {
		t.Errorf("Close before ListenAndAccept returned %v, want nil", err)
	}
}

func TestTransportDialUnreachableAddress(t *testing.T) {
	tr := newTestTransport(t, TCPTransportOpts{})
	// Port 1 on loopback is not listening.
	if err := tr.Dial("127.0.0.1:1"); err == nil {
		t.Error("Dial to an unreachable address returned nil, want an error")
	}
}

func TestTransportMessageRoundTrip(t *testing.T) {
	receiver := newTestTransport(t, TCPTransportOpts{})
	sender := newTestTransport(t, TCPTransportOpts{})

	peer := dialPeer(t, sender, receiver.BoundAddr())

	body := []byte("a framed message")
	if err := peer.Send(messageFrame(body)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	rpc := recvRPC(t, receiver)
	if !bytes.Equal(rpc.Payload, body) {
		t.Errorf("Payload = %q, want %q", rpc.Payload, body)
	}
	// From is the sender's identity, not its address: a node keeps its
	// identity as it moves between networks.
	if rpc.From == "" {
		t.Error("RPC.From is empty, so the receiver cannot tell who sent this")
	}
}

func TestTransportOnPeerDisconnectFires(t *testing.T) {
	connected := make(chan Peer, 1)
	disconnected := make(chan Peer, 1)

	receiver := newTestTransport(t, TCPTransportOpts{
		OnPeer:           func(p Peer) error { connected <- p; return nil },
		OnPeerDisconnect: func(p Peer) { disconnected <- p },
	})
	sender := newTestTransport(t, TCPTransportOpts{})

	if err := sender.Dial(receiver.BoundAddr()); err != nil {
		t.Fatalf("Dial: %v", err)
	}

	var peer Peer
	select {
	case peer = <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("OnPeer never fired")
	}

	// Drop the connection from the receiver's own side.
	peer.Close()

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("OnPeerDisconnect never fired; the server would keep broadcasting to a dead peer")
	}
}

// TestPeerConcurrentSendsDoNotInterleave is a regression test. Broadcasts,
// stream replies and peer exchange all write to the same connection from
// different goroutines. Without a per-peer write lock their bytes interleave
// and the receiver's frame stream is corrupted beyond recovery.
func TestPeerConcurrentSendsDoNotInterleave(t *testing.T) {
	receiver := newTestTransport(t, TCPTransportOpts{})
	sender := newTestTransport(t, TCPTransportOpts{})

	peer := dialPeer(t, sender, receiver.BoundAddr())

	const senders = 16
	const perSender = 8
	const fillerLen = 512

	// Derived rather than hardcoded, so the check cannot drift from the format.
	headerLen := len(fmt.Sprintf("sender-%02d-msg-%02d:", 0, 0))

	var wg sync.WaitGroup
	for i := range senders {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range perSender {
				// A distinctive header plus a filler of one repeated byte, so
				// any interleaving shows up as a body that disagrees with its
				// own header rather than passing by luck.
				body := []byte(fmt.Sprintf("sender-%02d-msg-%02d:", id, j))
				body = append(body, bytes.Repeat([]byte{byte('A' + id)}, fillerLen)...)

				if err := peer.Send(messageFrame(body)); err != nil {
					t.Errorf("Send: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool)
	for range senders * perSender {
		payload := recvRPC(t, receiver).Payload

		if len(payload) != headerLen+fillerLen {
			t.Fatalf("payload length = %d, want %d: frames interleaved", len(payload), headerLen+fillerLen)
		}

		header := string(payload[:headerLen])
		var id, msg int
		if _, err := fmt.Sscanf(header, "sender-%02d-msg-%02d:", &id, &msg); err != nil {
			t.Fatalf("corrupted header %q: %v", header, err)
		}
		if want := bytes.Repeat([]byte{byte('A' + id)}, fillerLen); !bytes.Equal(payload[headerLen:], want) {
			t.Fatalf("body of %q does not match its header: frames interleaved", header)
		}
		seen[header] = true
	}

	if len(seen) != senders*perSender {
		t.Errorf("received %d distinct messages, want %d", len(seen), senders*perSender)
	}
}

func TestTransportHandshakeFailureDropsConnection(t *testing.T) {
	onPeerCalled := make(chan struct{}, 1)

	receiver := newTestTransport(t, TCPTransportOpts{
		HandshakeFunc: func(any) error { return fmt.Errorf("rejected") },
		OnPeer:        func(p Peer) error { onPeerCalled <- struct{}{}; return nil },
	})
	sender := newTestTransport(t, TCPTransportOpts{})

	if err := sender.Dial(receiver.BoundAddr()); err != nil {
		t.Fatalf("Dial: %v", err)
	}

	select {
	case <-onPeerCalled:
		t.Fatal("OnPeer fired even though the handshake failed")
	case <-time.After(500 * time.Millisecond):
	}
}

// recordingConn captures everything written to it, in order.
type recordingConn struct {
	net.Conn
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *recordingConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(b)
}

func (c *recordingConn) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.buf.Bytes()...)
}

// chunkReader hands out data a few bytes at a time, the way a network read
// does, so a copy loop issues many writes instead of one.
type chunkReader struct {
	data  []byte
	chunk int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := min(min(r.chunk, len(p)), len(r.data))
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

// TestSendStreamIsIndivisible is a regression test. A transfer is an
// announcement followed by a file body. Sending them as separate writes let a
// concurrent transfer interleave between the two, and the receiver then
// paired one file's announcement with another file's bytes.
func TestSendStreamIsIndivisible(t *testing.T) {
	conn := &recordingConn{}
	peer := NewTCPPeer(conn, true)

	const transfers = 6
	const bodyLen = 2048

	var wg sync.WaitGroup
	for i := range transfers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			header := []byte{'H', byte('0' + i)}
			body := bytes.Repeat([]byte{byte('a' + i)}, bodyLen)

			// Small chunks widen the window an unsynchronised implementation
			// would interleave in.
			if _, err := peer.SendStream(header, &chunkReader{data: body, chunk: 16}); err != nil {
				t.Errorf("SendStream: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// The connection must read back as a sequence of whole transfers.
	out := conn.bytes()
	if want := transfers * (2 + 1 + bodyLen); len(out) != want {
		t.Fatalf("wrote %d bytes, want %d", len(out), want)
	}

	seen := make(map[byte]bool)
	for len(out) > 0 {
		if len(out) < 3 {
			t.Fatalf("trailing %d bytes do not form a transfer header", len(out))
		}
		if out[0] != 'H' {
			t.Fatalf("expected a transfer header, found %q: transfers interleaved", out[0])
		}

		id := out[1]
		if out[2] != IncomingStream {
			t.Fatalf("transfer %q: expected the stream tag after the header, found 0x%02x: transfers interleaved", id, out[2])
		}
		if seen[id] {
			t.Fatalf("transfer %q appears twice: transfers interleaved", id)
		}
		seen[id] = true

		body := out[3:]
		if len(body) < bodyLen {
			t.Fatalf("transfer %q: only %d body bytes remain, want %d", id, len(body), bodyLen)
		}
		want := bytes.Repeat([]byte{'a' + (id - '0')}, bodyLen)
		if !bytes.Equal(body[:bodyLen], want) {
			t.Fatalf("transfer %q: body does not match its header: transfers interleaved", id)
		}

		out = body[bodyLen:]
	}

	if len(seen) != transfers {
		t.Errorf("recovered %d transfers, want %d", len(seen), transfers)
	}
}

// TestPeerInterfacesAreSatisfied pins the shape of the transport contract.
//
// Peer is deliberately not a net.Conn: a libp2p stream has Read, Write, Close
// and deadlines but no addresses, and requiring them would exclude it. Located
// carries addressing separately, for the per-host admission limit that is the
// one place genuinely needing it.
func TestPeerInterfacesAreSatisfied(t *testing.T) {
	var _ Peer = (*TCPPeer)(nil)
	var _ Located = (*TCPPeer)(nil)
}

func TestTCPPeerReportsItsLocation(t *testing.T) {
	receiver := newTestTransport(t, TCPTransportOpts{})
	sender := newTestTransport(t, TCPTransportOpts{})

	peer := dialPeer(t, sender, receiver.BoundAddr())

	located, ok := peer.(Located)
	if !ok {
		t.Fatal("a TCP peer does not implement Located")
	}
	if host := located.RemoteHost(); host == "" {
		t.Error("RemoteHost is empty for a peer on a real socket")
	}
	addrs := located.AdvertisedAddrs()
	if len(addrs) != 1 || addrs[0] == "" {
		t.Errorf("AdvertisedAddrs = %v, want exactly one usable address", addrs)
	}
}
