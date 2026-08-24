package node

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
)

// The peers table records "connected" on connect and never corrects it if the
// node dies, so a view derived from SQLite reports a dead peer as live for
// ever. Liveness must come from the live peer set instead.
//
// Fails against any database-derived peer view.
func TestPeerViewsReportAStaleConnectedRowAsOffline(t *testing.T) {
	n := newTestNode(t)
	ctx := context.Background()

	// Exactly what a node that connected and then crashed leaves behind.
	now := time.Now()
	if err := n.DB.UpsertPeer(ctx, dbpkg.Peer{
		NodeID:   "a-node-that-is-not-here",
		Address:  "192.0.2.50:4000",
		Status:   "connected",
		LastSeen: &now,
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}

	views, err := n.PeerViews(ctx)
	if err != nil {
		t.Fatalf("PeerViews: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("PeerViews returned %d peers, want the one recorded", len(views))
	}
	if views[0].Online {
		t.Fatal("a peer recorded as connected but absent from the live set was reported online")
	}
	if views[0].NodeID != "a-node-that-is-not-here" {
		t.Fatalf("PeerViews returned %q", views[0].NodeID)
	}
	if views[0].LastSeen == nil {
		t.Fatal("the offline peer lost its last-seen time")
	}
}

// The mirror image: a peer that is connected right now must be online even
// before anything about it reaches the database.
func TestPeerViewsReportALiveConnectionAsOnline(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t, a.addr)
	waitForPeerCount(t, a, 1)

	views, err := a.PeerViews(context.Background())
	if err != nil {
		t.Fatalf("PeerViews: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("PeerViews returned %d peers, want 1", len(views))
	}
	if !views[0].Online {
		t.Fatal("a connected peer was reported offline")
	}
	if views[0].NodeID != b.NodeID() {
		t.Fatalf("PeerViews reported %q, want %q", views[0].NodeID, b.NodeID())
	}
	if views[0].Address == "" {
		t.Fatal("a connected peer was reported without an address")
	}
}

// A peer appearing both live and in the database is one entry, not two: the
// database contributes its last-seen time, the live set its liveness.
func TestPeerViewsMergeTheLiveAndRecordedPeer(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t, a.addr)
	waitForPeerCount(t, a, 1)

	// OnPeer records asynchronously; wait for the row rather than racing it.
	ctx := context.Background()
	waitFor(t, "the peer to be recorded", 10*time.Second, func() bool {
		known, err := a.DB.ListKnownPeers(ctx)
		return err == nil && len(known) == 1
	})

	views, err := a.PeerViews(ctx)
	if err != nil {
		t.Fatalf("PeerViews: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("PeerViews returned %d entries for one peer", len(views))
	}
	if !views[0].Online {
		t.Fatal("the merged peer lost its liveness")
	}
	if views[0].LastSeen == nil {
		t.Fatal("the merged peer lost the recorded last-seen time")
	}
	if views[0].NodeID != b.NodeID() {
		t.Fatalf("merged peer is %q, want %q", views[0].NodeID, b.NodeID())
	}
}

// Online peers sort first, and the order is total so a refresh does not
// shuffle the list.
func TestPeerViewsSortOnlineFirstAndStably(t *testing.T) {
	a := newTestNode(t)
	newTestNode(t, a.addr)
	waitForPeerCount(t, a, 1)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		seen := time.Now().Add(-time.Duration(i+1) * time.Hour)
		if err := a.DB.UpsertPeer(ctx, dbpkg.Peer{
			NodeID:   fmt.Sprintf("offline-%d", i),
			Address:  fmt.Sprintf("192.0.2.%d:4000", 60+i),
			Status:   "disconnected",
			LastSeen: &seen,
		}); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}

	first, err := a.PeerViews(ctx)
	if err != nil {
		t.Fatalf("PeerViews: %v", err)
	}
	if len(first) != 4 {
		t.Fatalf("PeerViews returned %d peers, want 4", len(first))
	}
	if !first[0].Online {
		t.Fatal("the online peer did not sort first")
	}
	for _, v := range first[1:] {
		if v.Online {
			t.Fatal("an offline peer sorted among the online ones")
		}
	}

	second, err := a.PeerViews(ctx)
	if err != nil {
		t.Fatalf("PeerViews: %v", err)
	}
	for i := range first {
		if first[i].NodeID != second[i].NodeID {
			t.Fatalf("the order changed between calls at %d: %q then %q",
				i, first[i].NodeID, second[i].NodeID)
		}
	}
}

// NodeView counts live peers, not database rows, and totals what is stored.
func TestNodeViewCountsLivePeersAndStoredBytes(t *testing.T) {
	a := newTestNode(t)
	ctx := context.Background()

	// A ghost row must not inflate the peer count.
	now := time.Now()
	if err := a.DB.UpsertPeer(ctx, dbpkg.Peer{
		NodeID: "ghost", Address: "192.0.2.70:4000", Status: "connected", LastSeen: &now,
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}

	v, err := a.NodeView(ctx)
	if err != nil {
		t.Fatalf("NodeView: %v", err)
	}
	if v.Peers != 0 {
		t.Fatalf("NodeView counted %d peers, want 0; a database row was counted as a connection", v.Peers)
	}
	if v.NodeID != a.NodeID() {
		t.Fatalf("NodeView reported node id %q, want %q", v.NodeID, a.NodeID())
	}
	if v.Address == "" {
		t.Fatal("NodeView reported no listen address")
	}
	if v.ReplicationFactor <= 0 {
		t.Fatalf("NodeView reported a replication factor of %d", v.ReplicationFactor)
	}

	payload := []byte("counted bytes")
	if err := a.Store("counted.txt", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	v, err = a.NodeView(ctx)
	if err != nil {
		t.Fatalf("NodeView: %v", err)
	}
	if v.Files != 1 {
		t.Fatalf("NodeView counted %d files, want 1", v.Files)
	}
	if v.Bytes != int64(len(payload)) {
		t.Fatalf("NodeView totalled %d bytes, want %d", v.Bytes, len(payload))
	}
}

// FileViews marks the files this node may authorise a deletion for.
func TestFileViewsMarkOwnership(t *testing.T) {
	owner := newTestNode(t)
	replica := newTestNode(t, owner.addr)
	waitForPeerCount(t, owner, 1)

	if err := owner.Store("owned.txt", bytes.NewReader([]byte("mine"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	ctx := context.Background()
	waitFor(t, "the replica to record the file", 15*time.Second, func() bool {
		views, err := replica.FileViews(ctx)
		return err == nil && len(views) == 1
	})

	mine, err := owner.FileViews(ctx)
	if err != nil {
		t.Fatalf("FileViews: %v", err)
	}
	if len(mine) != 1 {
		t.Fatalf("the owner listed %d files, want 1", len(mine))
	}
	if !mine[0].Mine {
		t.Fatal("the owner did not recognise its own file")
	}
	if mine[0].Name != "owned.txt" || mine[0].Digest == "" {
		t.Fatalf("FileViews returned %+v", mine[0])
	}
	if mine[0].Size != 4 {
		t.Fatalf("FileViews reported %d bytes, want 4", mine[0].Size)
	}

	theirs, err := replica.FileViews(ctx)
	if err != nil {
		t.Fatalf("FileViews: %v", err)
	}
	if theirs[0].Mine {
		t.Fatal("the replica claimed ownership of a file it only holds a copy of")
	}
	if theirs[0].Owner != owner.OwnerID() {
		t.Fatalf("the replica recorded owner %q, want %q", theirs[0].Owner, owner.OwnerID())
	}
}

// The read model must not touch the network: it is meant to sit behind a
// refresh. A node with a peer that never answers must still answer instantly.
func TestReadModelDoesNotTouchTheNetwork(t *testing.T) {
	a := newTestNode(t)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if err := a.Store(fmt.Sprintf("file-%d.txt", i),
			bytes.NewReader([]byte(fmt.Sprintf("contents %d", i)))); err != nil {
			t.Fatalf("Store: %v", err)
		}
	}

	// A peer that will never answer anything.
	now := time.Now()
	if err := a.DB.UpsertPeer(ctx, dbpkg.Peer{
		NodeID: "unreachable", Address: "192.0.2.200:4000", Status: "connected", LastSeen: &now,
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}

	start := time.Now()
	if _, err := a.NodeView(ctx); err != nil {
		t.Fatalf("NodeView: %v", err)
	}
	if _, err := a.PeerViews(ctx); err != nil {
		t.Fatalf("PeerViews: %v", err)
	}
	if _, err := a.FileViews(ctx); err != nil {
		t.Fatalf("FileViews: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("the read model took %v; it reached the network", elapsed)
	}
}
