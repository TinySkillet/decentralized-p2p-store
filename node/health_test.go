package node

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

// The whole point of the cache: reporting replication must not touch the
// network. ReplicationStatus pays one offer timeout per file, so with enough
// files and a peer that never answers it takes minutes; the snapshot must
// return at once.
//
// Fails against a snapshot implemented by calling checkFile.
func TestReplicationSnapshotDoesNotTouchTheNetwork(t *testing.T) {
	a := newTestNode(t)
	ctx := context.Background()

	// A peer that accepts every question and answers none. Without it
	// checkFile short-circuits on an empty peer set and the slow path this
	// test exists to rule out is never taken.
	a.peersLock.Lock()
	a.peers["silent-peer"] = &countingPeer{}
	a.peersLock.Unlock()

	const files = 30
	for i := 0; i < files; i++ {
		if err := a.Store(fmt.Sprintf("file-%d.txt", i),
			bytes.NewReader([]byte(fmt.Sprintf("contents %d", i)))); err != nil {
			t.Fatalf("Store: %v", err)
		}
	}

	start := time.Now()
	snaps, err := a.ReplicationSnapshot(ctx)
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	elapsed := time.Since(start)

	if len(snaps) != files {
		t.Fatalf("ReplicationSnapshot returned %d files, want %d", len(snaps), files)
	}
	// One offer timeout is 5s; 30 files the slow way is 150s.
	if elapsed > 2*time.Second {
		t.Fatalf("ReplicationSnapshot took %v for %d files; it reached the network", elapsed, files)
	}
}

// A file nobody has measured must say so, rather than reporting zero copies.
// "Not checked yet" and "no copies" are different facts and conflating them
// either hides files or raises false alarms.
func TestSnapshotDistinguishesUnmeasuredFromZeroCopies(t *testing.T) {
	a := newTestNode(t)
	ctx := context.Background()

	if err := a.Store("unmeasured.txt", bytes.NewReader([]byte("never checked"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Storing with no peers still counts this node's own copy, so drop the
	// measurement to model a file the node only learned about second-hand.
	snaps, err := a.ReplicationSnapshot(ctx)
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("ReplicationSnapshot returned %d files, want 1", len(snaps))
	}
	a.health.forget(snaps[0].Digest)

	snaps, err = a.ReplicationSnapshot(ctx)
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	if snaps[0].Measured() {
		t.Fatal("a file with no measurement reported one")
	}
	if !snaps[0].MeasuredAt.IsZero() {
		t.Fatalf("an unmeasured file carried a timestamp of %v", snaps[0].MeasuredAt)
	}
	if snaps[0].Target <= 0 {
		t.Fatalf("an unmeasured file reported a target of %d", snaps[0].Target)
	}
}

// Storing is itself a measurement: the peers that accepted a copy are known
// holders, so the file must not read as unmeasured afterwards.
func TestStoringRecordsAMeasurement(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t, a.addr)
	waitForPeerCount(t, a, 1)

	if err := a.Store("fresh.txt", bytes.NewReader([]byte("just stored"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	snaps, err := a.ReplicationSnapshot(context.Background())
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("ReplicationSnapshot returned %d files, want 1", len(snaps))
	}
	if !snaps[0].Measured() {
		t.Fatal("a file just stored and replicated reported no measurement")
	}
	if snaps[0].Copies != 2 {
		t.Fatalf("a file stored here and taken by one peer reported %d copies, want 2", snaps[0].Copies)
	}
	if len(snaps[0].Holders) != 1 || snaps[0].Holders[0] != b.NodeID() {
		t.Fatalf("holders were %v, want just %s", snaps[0].Holders, b.NodeID())
	}
	if snaps[0].AtRisk() && snaps[0].Target <= 2 {
		t.Fatalf("a fully replicated file was reported at risk: %d of %d", snaps[0].Copies, snaps[0].Target)
	}
}

// A peer disconnecting is the one change that makes a cached count wrong in
// the dangerous direction — a file reads as better replicated than it is — so
// the holder must be removed from the measurement, not left to age out.
//
// Fails against a cache that only expires by age.
func TestADepartedPeerStopsCountingAsAHolder(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t, a.addr)
	waitForPeerCount(t, a, 1)

	if err := a.Store("fragile.txt", bytes.NewReader([]byte("two copies, briefly"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	ctx := context.Background()
	snaps, err := a.ReplicationSnapshot(ctx)
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	if snaps[0].Copies != 2 {
		t.Fatalf("before the disconnect the file had %d copies, want 2", snaps[0].Copies)
	}

	b.Stop()
	waitForPeerCount(t, a, 0)

	waitFor(t, "the departed peer to stop counting", 10*time.Second, func() bool {
		s, err := a.ReplicationSnapshot(ctx)
		return err == nil && len(s) == 1 && s[0].Copies == 1
	})

	snaps, err = a.ReplicationSnapshot(ctx)
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	if len(snaps[0].Holders) != 0 {
		t.Fatalf("the departed peer is still listed as a holder: %v", snaps[0].Holders)
	}
	if !snaps[0].AtRisk() {
		t.Fatalf("a file down to one copy of a target of %d was not reported at risk", snaps[0].Target)
	}
}

// Deleting a file must drop its measurement, so a digest stored again later
// does not inherit a count from before.
func TestDeletingForgetsTheMeasurement(t *testing.T) {
	a := newTestNode(t)
	ctx := context.Background()

	if err := a.Store("temporary.txt", bytes.NewReader([]byte("here then gone"))); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if a.health.size() != 1 {
		t.Fatalf("the cache holds %d entries after one store, want 1", a.health.size())
	}

	if err := a.Delete("temporary.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if a.health.size() != 0 {
		t.Fatalf("the cache still holds %d entries after the file was deleted", a.health.size())
	}

	snaps, err := a.ReplicationSnapshot(ctx)
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("a deleted file still appears in the snapshot: %+v", snaps)
	}
}

// One blob under several names shares its copies, so it is measured once.
func TestTheCacheIsKeyedByDigestNotName(t *testing.T) {
	a := newTestNode(t)

	payload := []byte("the same contents twice")
	if err := a.Store("first.txt", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := a.Store("second.txt", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	if a.health.size() != 1 {
		t.Fatalf("the cache holds %d entries for one blob under two names, want 1", a.health.size())
	}

	snaps, err := a.ReplicationSnapshot(context.Background())
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("the snapshot returned %d entries for two names, want 2", len(snaps))
	}
	if snaps[0].Digest != snaps[1].Digest {
		t.Fatal("two names for the same contents reported different digests")
	}
	if snaps[0].Copies != snaps[1].Copies {
		t.Fatalf("two names for one blob reported %d and %d copies", snaps[0].Copies, snaps[1].Copies)
	}
}

// Recheck measures one file now and refreshes the cache with the result.
func TestRecheckMeasuresOneFileAndRefreshesTheCache(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t, a.addr)
	waitForPeerCount(t, a, 1)
	_ = b

	ctx := context.Background()
	if err := a.Store("checked.txt", bytes.NewReader([]byte("measure me"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Wipe the measurement so Recheck has to do real work.
	snaps, err := a.ReplicationSnapshot(ctx)
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	a.health.forget(snaps[0].Digest)

	got, err := a.Recheck(ctx, "checked.txt")
	if err != nil {
		t.Fatalf("Recheck: %v", err)
	}
	if !got.Measured() {
		t.Fatal("Recheck returned an unmeasured result")
	}
	if got.Copies != 2 {
		t.Fatalf("Recheck counted %d copies, want 2", got.Copies)
	}

	// And the cache now answers with it.
	snaps, err = a.ReplicationSnapshot(ctx)
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	if !snaps[0].Measured() || snaps[0].Copies != 2 {
		t.Fatalf("the cache was not refreshed: %+v", snaps[0])
	}
}

// Recheck must name the problem rather than reporting a false zero.
func TestRecheckRejectsAnUnknownFile(t *testing.T) {
	a := newTestNode(t)

	if _, err := a.Recheck(context.Background(), "never-stored.txt"); err == nil {
		t.Fatal("Recheck accepted a file that is not stored here")
	}
}

// The repair cycle already measures every file it examines; that measurement
// must land in the cache rather than being thrown away.
func TestTheRepairCyclePopulatesTheCache(t *testing.T) {
	a := newTestNode(t)
	b := newTestNode(t, a.addr)
	waitForPeerCount(t, a, 1)
	_ = b

	ctx := context.Background()
	if err := a.Store("repaired.txt", bytes.NewReader([]byte("watch the cycle"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	snaps, err := a.ReplicationSnapshot(ctx)
	if err != nil {
		t.Fatalf("ReplicationSnapshot: %v", err)
	}
	digest := snaps[0].Digest
	a.health.forget(digest)

	if _, err := a.RepairOnce(); err != nil {
		t.Fatalf("RepairOnce: %v", err)
	}

	if _, ok := a.health.lookup(digest); !ok {
		t.Fatal("a repair cycle measured the file and discarded the result")
	}
}

// The cache must be safe to read while the node is writing to it.
func TestHealthCacheIsSafeUnderConcurrentUse(t *testing.T) {
	c := newHealthCache()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			c.record(fmt.Sprintf("digest-%d", i%10), measurement{
				copies: 2, target: 3, holders: []string{"peer-a", "peer-b"}, at: time.Now(),
			})
		}
	}()

	for i := 0; i < 500; i++ {
		c.lookup(fmt.Sprintf("digest-%d", i%10))
		c.dropHolder("peer-a")
		c.size()
	}
	<-done
}
