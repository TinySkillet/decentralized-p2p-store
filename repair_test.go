package main

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
)

// newQuietNode is a node with the background repair loop disabled, so a test
// drives repair explicitly and its assertions are not raced by a timer.
//
// Settings are applied before the node starts: writing them afterwards races
// the goroutines Serve launches.
func newQuietNode(t *testing.T, bootstrap ...string) *testNode {
	t.Helper()
	return newQuietNodeWith(t, 0, bootstrap...)
}

// newQuietNodeWith is newQuietNode with a specific replication target.
func newQuietNodeWith(t *testing.T, replicas int, bootstrap ...string) *testNode {
	t.Helper()
	return buildTestNode(t, freeAddr(t), nodeConfig{
		replicationFactor: replicas,
		repairInterval:    -1,
	}, bootstrap...)
}

func TestReplicationStatusCountsCopies(t *testing.T) {
	origin := newQuietNodeWith(t, 2)
	replica := newQuietNode(t, origin.addr)

	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, replica, 1)

	payload := randomBytes(t, 4096)
	if err := origin.Store("tracked", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	waitFor(t, "the replica to receive the file", 10*time.Second, func() bool {
		return replica.store.Has(contentKey(payload))
	})

	health, err := origin.ReplicationStatus()
	if err != nil {
		t.Fatalf("ReplicationStatus: %v", err)
	}
	if len(health) != 1 {
		t.Fatalf("got %d files, want 1", len(health))
	}

	// This node plus the replica.
	if health[0].Copies != 2 {
		t.Errorf("Copies = %d, want 2", health[0].Copies)
	}
	if health[0].AtRisk() {
		t.Error("a file at its target was reported at risk")
	}

	// A target the network cannot meet must be reported as at risk. Checked
	// through FileHealth rather than by mutating the running node, whose
	// settings its own goroutines are reading.
	stretched := FileHealth{Copies: health[0].Copies, Target: health[0].Copies + 1}
	if !stretched.AtRisk() {
		t.Error("a file below the target was not reported at risk")
	}
}

// TestRepairRestoresMissingReplica is the point of the whole mechanism: a
// file replicated when it was stored must not quietly decay to one copy as
// the peers holding it come and go.
func TestRepairRestoresMissingReplica(t *testing.T) {
	origin := newQuietNodeWith(t, 2)

	payload := randomBytes(t, 8192)
	digest := contentKey(payload)

	// Stored with nobody else around, so it starts with a single copy.
	if err := origin.Store("lonely", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	helper := newQuietNode(t, origin.addr)
	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, helper, 1)

	if helper.store.Has(digest) {
		t.Fatal("the new peer already holds the file; the test proves nothing")
	}

	placed, err := origin.RepairOnce()
	if err != nil {
		t.Fatalf("RepairOnce: %v", err)
	}
	if placed != 1 {
		t.Fatalf("placed %d replicas, want 1", placed)
	}

	waitFor(t, "the repaired copy to land", 10*time.Second, func() bool {
		return helper.store.Has(digest)
	})

	// The repaired copy must be the real thing, readable by name.
	_, r, err := helper.Get("lonely")
	if err != nil {
		t.Fatalf("Get from the repaired peer: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("the repaired copy does not match the original")
	}
}

func TestRepairIsIdempotent(t *testing.T) {
	origin := newQuietNodeWith(t, 2)

	payload := randomBytes(t, 2048)
	if err := origin.Store("stable", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	helper := newQuietNode(t, origin.addr)
	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, helper, 1)

	if _, err := origin.RepairOnce(); err != nil {
		t.Fatalf("first RepairOnce: %v", err)
	}
	waitFor(t, "the repaired copy to land", 10*time.Second, func() bool {
		return helper.store.Has(contentKey(payload))
	})

	// Once the target is met, a second cycle must place nothing.
	placed, err := origin.RepairOnce()
	if err != nil {
		t.Fatalf("second RepairOnce: %v", err)
	}
	if placed != 0 {
		t.Errorf("a second repair cycle placed %d replicas, want 0", placed)
	}
}

func TestRepairDoesNothingWithoutPeers(t *testing.T) {
	node := newQuietNodeWith(t, 3)

	if err := node.Store("alone", bytes.NewReader([]byte("no peers here"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	placed, err := node.RepairOnce()
	if err != nil {
		t.Fatalf("RepairOnce: %v", err)
	}
	if placed != 0 {
		t.Errorf("placed %d replicas with no peers connected, want 0", placed)
	}
}

// TestRepairStopsAtTheAvailablePeerCount checks a target larger than the
// network does not loop or double-place.
func TestRepairStopsAtTheAvailablePeerCount(t *testing.T) {
	// A target larger than this network can ever provide.
	origin := newQuietNodeWith(t, 10)

	payload := randomBytes(t, 1024)
	if err := origin.Store("ambitious", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	helper := newQuietNode(t, origin.addr)
	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, helper, 1)

	placed, err := origin.RepairOnce()
	if err != nil {
		t.Fatalf("RepairOnce: %v", err)
	}
	if placed != 1 {
		t.Errorf("placed %d replicas onto a one-peer network, want 1", placed)
	}
}

// TestRepairChecksEachBlobOnce covers deduplication: several names sharing one
// blob must not each trigger their own transfer.
func TestRepairChecksEachBlobOnce(t *testing.T) {
	origin := newQuietNodeWith(t, 2)

	payload := randomBytes(t, 4096)
	for _, name := range []string{"one", "two", "three"} {
		if err := origin.Store(name, bytes.NewReader(payload)); err != nil {
			t.Fatalf("Store %s: %v", name, err)
		}
	}

	helper := newQuietNode(t, origin.addr)
	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, helper, 1)

	placed, err := origin.RepairOnce()
	if err != nil {
		t.Fatalf("RepairOnce: %v", err)
	}
	// Three names, one blob, so one transfer.
	if placed != 1 {
		t.Errorf("placed %d replicas for three names sharing one blob, want 1", placed)
	}
}

// TestCheckFileIgnoresDifferentContentUnderTheSameName pins that a peer
// holding something else under a name does not count as a copy.
func TestCheckFileIgnoresDifferentContentUnderTheSameName(t *testing.T) {
	first := newQuietNodeWith(t, 2)
	second := newQuietNode(t, first.addr)

	waitForPeerCount(t, first, 1)
	waitForPeerCount(t, second, 1)

	// Both nodes hold "notes", but with different contents.
	if err := first.Store("notes", bytes.NewReader([]byte("the first version"))); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := second.Store("notes", bytes.NewReader([]byte("a different version"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	files, err := first.db.ListFiles(context.Background())
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	var target *FileHealth
	for _, f := range files {
		if f.Name != "notes" {
			continue
		}
		health, _, err := first.checkFile(f)
		if err != nil {
			t.Fatalf("checkFile: %v", err)
		}
		target = &health
	}
	if target == nil {
		t.Fatal("the stored file was not found")
	}

	// The peer holds a different digest, so it is not a copy of this file.
	if target.Copies != 1 {
		t.Errorf("Copies = %d, want 1: a peer holding different contents was counted", target.Copies)
	}
}

// TestReplicaDoesNotHijackLocalName is a regression test. An incoming copy
// carrying a name this node already uses overwrote the local mapping, so a
// peer could silently repoint someone else's file at its own contents.
func TestReplicaDoesNotHijackLocalName(t *testing.T) {
	victim := newQuietNode(t)
	attacker := newQuietNode(t, victim.addr)

	waitForPeerCount(t, victim, 1)
	waitForPeerCount(t, attacker, 1)

	mine := []byte("my own notes, which must survive")
	if err := victim.Store("notes", bytes.NewReader(mine)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	theirs := []byte("someone else's notes")
	if err := attacker.Store("notes", bytes.NewReader(theirs)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	waitFor(t, "the incoming copy to be processed", 10*time.Second, func() bool {
		return victim.store.Has(contentKey(theirs))
	})

	// The local name must still resolve to the local contents.
	_, r, err := victim.Get("notes")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, mine) {
		t.Fatal("a peer's copy took over the local name")
	}

	// The copy is still kept and listed, just under its digest.
	files, err := victim.db.ListFiles(context.Background())
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	var foundReplica bool
	for _, f := range files {
		if f.Hash == contentKey(theirs) {
			foundReplica = true
			if f.Name == "notes" {
				t.Error("the replica claimed the local name")
			}
		}
	}
	if !foundReplica {
		t.Error("the replica was not recorded at all; it should still be held for the network")
	}
}

// TestBorrowedStorageIsNotCountedTwice is a regression test for the way the
// CLI works: a command opens the database of a running node to reuse its
// metadata and encryption key, so its copy of a file is that node's copy.
// Counting both reported one more replica than the network actually held,
// which meant a file could be shown as safe while a single machine held it.
func TestBorrowedStorageIsNotCountedTwice(t *testing.T) {
	// A long-lived node that owns its database.
	owner := buildTestNode(t, freeAddr(t), nodeConfig{
		replicationFactor: 2,
		repairInterval:    -1,
	})
	owner.OwnsDatabase = true
	if err := owner.DB.PutSetting(context.Background(), dbpkg.ServingAddressSetting, owner.addr); err != nil {
		t.Fatalf("PutSetting: %v", err)
	}

	payload := randomBytes(t, 2048)
	if err := owner.Store("shared-storage", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// A command node against the same database and storage root.
	client, err := makeClientNode(freeAddr(t), owner.db, owner.addr)
	if err != nil {
		t.Fatalf("makeClientNode: %v", err)
	}
	client.EncryptionKey = owner.EncryptionKey
	client.ReplicationFactor = 2
	if err := client.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go client.Serve()
	t.Cleanup(client.Stop)

	waitFor(t, "the command node to reach the serving node", 15*time.Second, func() bool {
		return client.peerCount() > 0
	})

	health, err := client.ReplicationStatus()
	if err != nil {
		t.Fatalf("ReplicationStatus: %v", err)
	}
	if len(health) == 0 {
		t.Fatal("no files reported")
	}

	// One machine holds the file, so there is one copy.
	if health[0].Copies != 1 {
		t.Errorf("Copies = %d, want 1: borrowed storage was counted as a second copy", health[0].Copies)
	}
	if !health[0].AtRisk() {
		t.Error("a file held on a single machine was not reported at risk")
	}
}
