package p2p

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"
)

// newTestTransport starts a transport on an ephemeral loopback port.
func newTestTransport(t *testing.T, opts TCPTransportOpts) *TCPTransport {
	t.Helper()

	opts.ListenAddr = "127.0.0.1:0"
	if opts.HandshakeFunc == nil {
		opts.HandshakeFunc = NOPHandshakeFunc
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
	if rpc.From == "" {
		t.Error("RPC.From is empty")
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
