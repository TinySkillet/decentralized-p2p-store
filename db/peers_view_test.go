package db

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()

	d, err := Open(filepath.Join(t.TempDir(), "p2p.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// ListKnownPeers must not apply the per-host cap. GetActivePeers hides the 4th
// and later identity on a host to bound gossip; a view for a person that did
// the same would conceal exactly the peers worth noticing.
func TestListKnownPeersDoesNotHidePeersSharingAHost(t *testing.T) {
	d := openTestDB(t)
	ctx := context.Background()
	now := time.Now()

	for i := 0; i < 5; i++ {
		seen := now.Add(-time.Duration(i) * time.Minute)
		if err := d.UpsertPeer(ctx, Peer{
			NodeID:   fmt.Sprintf("node-%d", i),
			Address:  fmt.Sprintf("192.0.2.10:%d", 4000+i),
			Status:   "connected",
			LastSeen: &seen,
		}); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}

	known, err := d.ListKnownPeers(ctx)
	if err != nil {
		t.Fatalf("ListKnownPeers: %v", err)
	}
	if len(known) != 5 {
		t.Fatalf("ListKnownPeers returned %d peers, want all 5", len(known))
	}

	// The contrast that justifies the separate query.
	active, err := d.GetActivePeers(ctx, time.Hour, 100)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}
	if len(active) != DefaultMaxPeersPerHost {
		t.Fatalf("GetActivePeers returned %d, want the %d-per-host cap; "+
			"if this changed, ListKnownPeers may no longer be needed",
			len(active), DefaultMaxPeersPerHost)
	}
}

// ListKnownPeers must not apply a recency cutoff either: a peer that has been
// offline for a week is still a peer you may want to see and reconnect to.
func TestListKnownPeersIgnoresRecency(t *testing.T) {
	d := openTestDB(t)
	ctx := context.Background()

	ancient := time.Now().Add(-30 * 24 * time.Hour)
	if err := d.UpsertPeer(ctx, Peer{
		NodeID: "long-gone", Address: "192.0.2.99:4000",
		Status: "disconnected", LastSeen: &ancient,
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}

	known, err := d.ListKnownPeers(ctx)
	if err != nil {
		t.Fatalf("ListKnownPeers: %v", err)
	}
	if len(known) != 1 {
		t.Fatalf("ListKnownPeers returned %d peers, want the one month-old peer", len(known))
	}

	active, err := d.GetActivePeers(ctx, time.Hour, 100)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("GetActivePeers returned %d, want 0 for a month-old peer", len(active))
	}
}

// A peer recorded without a last_seen must still be listed, sorted last.
func TestListKnownPeersIncludesPeersNeverSeen(t *testing.T) {
	d := openTestDB(t)
	ctx := context.Background()

	seen := time.Now()
	if err := d.UpsertPeer(ctx, Peer{
		NodeID: "seen", Address: "192.0.2.1:4000", Status: "connected", LastSeen: &seen,
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}
	if err := d.UpsertPeer(ctx, Peer{
		NodeID: "never", Address: "192.0.2.2:4000", Status: "known",
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}

	known, err := d.ListKnownPeers(ctx)
	if err != nil {
		t.Fatalf("ListKnownPeers: %v", err)
	}
	if len(known) != 2 {
		t.Fatalf("ListKnownPeers returned %d peers, want 2", len(known))
	}
	if known[0].NodeID != "seen" || known[1].NodeID != "never" {
		t.Fatalf("order was %s then %s, want the never-seen peer last",
			known[0].NodeID, known[1].NodeID)
	}
	if known[1].LastSeen != nil {
		t.Fatalf("the never-seen peer reported a last_seen of %v", known[1].LastSeen)
	}
}
