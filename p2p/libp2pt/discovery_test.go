package libp2pt

import (
	"crypto/rand"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// The notifee is tested directly with synthesised AddrInfo values: real
// multicast is unreliable in CI containers, and the translation is the part
// this package owns.

func addrInfoFor(t *testing.T, priv crypto.PrivKey) peer.AddrInfo {
	t.Helper()
	pid, err := peer.IDFromPublicKey(priv.GetPublic())
	if err != nil {
		t.Fatalf("IDFromPublicKey: %v", err)
	}
	addr, err := ma.NewMultiaddr("/ip4/192.168.1.20/tcp/3000")
	if err != nil {
		t.Fatalf("NewMultiaddr: %v", err)
	}
	return peer.AddrInfo{ID: pid, Addrs: []ma.Multiaddr{addr}}
}

func TestDiscoveryReportsFoundPeer(t *testing.T) {
	tr := newTestTransport(t, Opts{})

	found := make(chan struct {
		id    string
		addrs []string
	}, 1)
	tr.found = func(id string, addrs []string) {
		found <- struct {
			id    string
			addrs []string
		}{id, addrs}
	}

	other := newTestTransport(t, Opts{})
	info := addrInfoFor(t, mustKey(t, other.Key))

	tr.peerFound(info)

	select {
	case got := <-found:
		if got.id != other.nodeID() {
			t.Errorf("reported id %s, want %s", got.id, other.nodeID())
		}
		if len(got.addrs) != 1 || !strings.HasSuffix(got.addrs[0], "/p2p/"+info.ID.String()) {
			t.Errorf("reported addrs %v, want one complete dial target ending in /p2p/%s", got.addrs, info.ID)
		}
	default:
		t.Fatal("the discovered peer was not reported")
	}
}

// TestDiscoveryIgnoresSelf: gossip taught this lesson already — a node is
// eventually handed itself, and mDNS hears its own announcements.
func TestDiscoveryIgnoresSelf(t *testing.T) {
	tr := newTestTransport(t, Opts{})
	tr.found = func(id string, addrs []string) {
		t.Errorf("this node reported itself as a discovered peer (%s)", id)
	}
	tr.peerFound(addrInfoFor(t, mustKey(t, tr.Key)))
}

// TestDiscoveryIgnoresNonEd25519 keeps the discovered list consistent with
// the connection gate: a peer that could never connect is not shown.
func TestDiscoveryIgnoresNonEd25519(t *testing.T) {
	tr := newTestTransport(t, Opts{})
	tr.found = func(id string, addrs []string) {
		t.Errorf("an RSA peer was reported as discovered (%s)", id)
	}

	rsaKey, _, err := crypto.GenerateRSAKeyPair(2048, rand.Reader)
	if err != nil {
		t.Fatalf("GenerateRSAKeyPair: %v", err)
	}
	tr.peerFound(addrInfoFor(t, rsaKey))
}

func TestDiscoverRequiresARunningHost(t *testing.T) {
	tr, err := New(Opts{ListenAddr: "127.0.0.1:0", Key: freshKey(t)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tr.Discover(func(string, []string) {}); err == nil {
		t.Error("Discover before ListenAndAccept returned nil, want an error")
	}
}

func mustKey(t *testing.T, raw []byte) crypto.PrivKey {
	t.Helper()
	k, err := libp2pKey(raw)
	if err != nil {
		t.Fatalf("libp2pKey: %v", err)
	}
	return k
}

func freshKey(t *testing.T) []byte {
	t.Helper()
	_, priv, err := generateTestKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return priv
}
