package node

import (
	"context"
	"testing"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
)

// discoveryReport simulates what the transport's discovery loop would hand
// the node for other. Real multicast is exercised outside CI; these tests own
// the node's decision about a discovery, not the noticing itself.
func discoveryReport(t *testing.T, of *testNode) (string, []string) {
	t.Helper()
	return of.NodeID(), []string{qualifyAddr(of.addr)}
}

// qualifyAddr turns a test node's listen address into a dialable location
// for whichever transport the suite runs on.
func qualifyAddr(addr string) string {
	q := qualifyBootstrap(addr)
	if id, rest, ok := cutID(q); ok {
		_ = id
		return rest
	}
	return q
}

func cutID(entry string) (string, string, bool) {
	for i := range entry {
		if entry[i] == '@' {
			return entry[:i], entry[i+1:], true
		}
	}
	return "", "", false
}

// TestDiscoveredUntrustedPeerIsNotDialled pins the trust model to discovery:
// on a network full of strangers, a discovered peer becomes visible and
// approvable — never connected. Approval is the gate; discovery is just the
// doorbell.
func TestDiscoveredUntrustedPeerIsNotDialled(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t)

	id, addrs := discoveryReport(t, b)
	a.onPeerDiscovered(id, addrs)

	// The peer must be recorded and visible for approval...
	waitFor(t, "the discovered peer to be recorded", 5*time.Second, func() bool {
		peers, err := a.DB.ListKnownPeers(context.Background())
		if err != nil {
			return false
		}
		for _, p := range peers {
			if p.NodeID == id && p.Status == "discovered" {
				return true
			}
		}
		return false
	})

	// ...but never dialled.
	time.Sleep(500 * time.Millisecond)
	if got := a.peerCount(); got != 0 {
		t.Fatalf("the node connected to %d discovered peer(s) nobody approved", got)
	}
}

// TestDiscoveredTrustedPeerIsDialled covers the reconnection this exists
// for: two mutually approved laptops joining the same network find each
// other and connect with no addresses configured anywhere.
func TestDiscoveredTrustedPeerIsDialled(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t)

	id, addrs := discoveryReport(t, b)
	if err := a.DB.TrustPeer(context.Background(), id, ""); err != nil {
		t.Fatalf("TrustPeer: %v", err)
	}
	a.trust.add(id)

	a.onPeerDiscovered(id, addrs)

	waitForPeerCount(t, a, 1)
}

// TestDiscoveryDebounces: mDNS re-announces every few seconds; each repeat
// must not become another event and another database write.
func TestDiscoveryDebounces(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t)

	events, cancel := a.Subscribe(16)
	defer cancel()

	id, addrs := discoveryReport(t, b)
	for range 5 {
		a.onPeerDiscovered(id, addrs)
	}

	seen := 0
	timeout := time.After(2 * time.Second)
drain:
	for {
		select {
		case e := <-events:
			if e.Kind == EventPeerDiscovered {
				seen++
			}
		case <-timeout:
			break drain
		default:
			if seen > 0 {
				break drain
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if seen != 1 {
		t.Fatalf("5 discovery reports produced %d events, want 1", seen)
	}
}

// TestDiscoveryIgnoresAConnectedPeer: discovering a peer that is already
// connected must not overwrite its record with "discovered".
func TestDiscoveryIgnoresAConnectedPeer(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t, a.addr)
	waitForPeerCount(t, a, 1)

	id, addrs := discoveryReport(t, b)
	a.onPeerDiscovered(id, addrs)

	peers, err := a.DB.ListKnownPeers(context.Background())
	if err != nil {
		t.Fatalf("ListKnownPeers: %v", err)
	}
	for _, p := range peers {
		if p.NodeID == id && p.Status == "discovered" {
			t.Fatal("a connected peer was downgraded to discovered")
		}
	}
	_ = dbpkg.Peer{}
}

// TestApprovingADiscoveredPeerConnectsIt closes the discovery loop: a
// stranger appears in the list, someone approves it, and the connection —
// and the copies approval makes possible — should follow immediately, not at
// some later rediscovery.
func TestApprovingADiscoveredPeerConnectsIt(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t)

	id, addrs := discoveryReport(t, b)
	a.onPeerDiscovered(id, addrs)
	if a.peerCount() != 0 {
		t.Fatal("connected before anyone approved; the discovery gate is broken")
	}

	if err := a.Trust(id, "the laptop from the kitchen"); err != nil {
		t.Fatalf("Trust: %v", err)
	}

	waitForPeerCount(t, a, 1)
}
