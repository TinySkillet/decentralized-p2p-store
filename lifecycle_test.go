package main

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

// TestOverwritingANameReclaimsTheOldContents is a regression test. Storing a
// new version under an existing name left the previous contents on disk with
// nothing referring to them, and nothing ever reclaimed the space.
func TestOverwritingANameReclaimsTheOldContents(t *testing.T) {
	node := newQuietNode(t)

	v1 := randomBytes(t, 8192)
	v2 := randomBytes(t, 8192)

	if err := node.Store("notes", bytes.NewReader(v1)); err != nil {
		t.Fatalf("Store v1: %v", err)
	}
	if err := node.Store("notes", bytes.NewReader(v2)); err != nil {
		t.Fatalf("Store v2: %v", err)
	}

	// The replaced contents are unreferenced and must be reclaimed.
	if _, err := node.SweepOrphans(0); err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	if n := countStoredFiles(t, node.store.Root); n != 1 {
		t.Errorf("%d blobs on disk after overwriting one name, want 1: the old contents leaked", n)
	}

	// And the name must resolve to the new contents.
	_, r, err := node.Get("notes")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, v2) {
		t.Error("the name does not resolve to the newer contents")
	}
}

// TestDeleteDoesNotUnlinkNewlyReferencedContents is a regression test.
// Deciding contents were unreferenced and unlinking them were two steps, so a
// name recorded in between was left pointing at data that had just been
// deleted.
func TestDeleteDoesNotUnlinkNewlyReferencedContents(t *testing.T) {
	node := newQuietNode(t)
	payload := randomBytes(t, 4096)
	digest := contentKey(payload)

	if err := node.Store("first", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Simulate the window inside forget(): the name row is gone and the
	// contents look orphaned, but a new name is recorded before the unlink.
	hash, orphaned, err := node.db.DeleteFileByName(context.Background(), "first", "")
	if err != nil {
		t.Fatalf("DeleteFileByName: %v", err)
	}
	if !orphaned {
		t.Fatal("expected the contents to look orphaned")
	}

	// A replica arrives for the same contents, right now.
	if err := node.recordReplica("second", digest, int64(len(payload))); err != nil {
		t.Fatalf("recordReplica: %v", err)
	}

	_ = hash

	// The sweep decides and acts together, so it must see the new reference
	// and leave the contents alone.
	if _, err := node.SweepOrphans(0); err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	_, r, err := node.Get("second")
	if err != nil {
		t.Fatalf("'second' refers to contents that were unlinked: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("the surviving name resolves to the wrong contents")
	}
}

// TestDeleteWithNoPeersLeavesRemoteCopies documents what a deletion can and
// cannot promise: it removes the name here, but a peer that is unreachable
// keeps its copy until it hears about the deletion.
func TestDeleteWithNoPeersLeavesRemoteCopies(t *testing.T) {
	origin := newQuietNode(t)
	replica := newQuietNode(t, origin.addr)
	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, replica, 1)

	payload := randomBytes(t, 2048)
	if err := origin.Store("doomed", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	waitFor(t, "the replica to receive it", 10*time.Second, func() bool {
		return replica.store.Has(contentKey(payload))
	})

	// The replica goes away before the delete.
	replica.Stop()
	waitFor(t, "the origin to notice", 10*time.Second, func() bool {
		return origin.peerCount() == 0
	})

	err := origin.Delete("doomed")
	if err != nil {
		t.Logf("Delete reported an error, which is arguably right: %v", err)
	} else {
		t.Log("Delete reported success while a copy survives on an unreachable peer")
	}

	if !replica.store.Has(contentKey(payload)) {
		t.Error("the replica somehow lost the file")
	}
}

// TestDeletionSurvivesAPeerThatMissedIt is a regression test, and the reason
// tombstones exist. A peer that was not listening when a file was deleted
// still held it, and its repair cycle pushed it back to every node that had
// removed it, undoing the deletion across the network.
func TestDeletionSurvivesAPeerThatMissedIt(t *testing.T) {
	origin := newQuietNodeWith(t, 2)
	straggler := newQuietNodeWith(t, 2, origin.addr)

	waitForPeerCount(t, origin, 1)
	waitForPeerCount(t, straggler, 1)

	payload := randomBytes(t, 2048)
	digest := contentKey(payload)

	if err := origin.Store("secret", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	waitFor(t, "the straggler to receive it", 10*time.Second, func() bool {
		return straggler.store.Has(digest)
	})

	// The origin deletes it while the straggler is not listening, so only the
	// origin forgets. forget() is the local half of a delete broadcast.
	if err := origin.forget("secret", digest); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if _, err := origin.SweepOrphans(0); err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	if origin.store.Has(digest) {
		t.Fatal("the origin still holds the deleted contents")
	}

	// The straggler comes back and repairs what it thinks is under-replicated.
	if _, err := straggler.RepairOnce(); err != nil {
		t.Fatalf("RepairOnce: %v", err)
	}

	// Give the push time to arrive and be refused.
	time.Sleep(500 * time.Millisecond)

	if origin.store.Has(digest) {
		t.Error("the deleted file came back from a peer that missed the deletion")
	}

	// The refusal also tells the straggler, so the deletion spreads to nodes
	// that were not reachable when it was broadcast.
	waitFor(t, "the straggler to learn of the deletion", 10*time.Second, func() bool {
		f, err := straggler.db.FindFileByName(context.Background(), "secret")
		return err == nil && f == nil
	})
}

// TestDeleteOnlyAffectsMatchingContents is a regression test. A deletion
// travels by name, and two nodes may legitimately use the same name for
// different files. Without the digest guard, deleting your "notes" deleted
// everyone else's too.
func TestDeleteOnlyAffectsMatchingContents(t *testing.T) {
	mine := newQuietNode(t)
	theirs := newQuietNode(t, mine.addr)

	waitForPeerCount(t, mine, 1)
	waitForPeerCount(t, theirs, 1)

	myPayload := randomBytes(t, 1024)
	theirPayload := randomBytes(t, 1024)

	if err := mine.Store("notes", bytes.NewReader(myPayload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := theirs.Store("notes", bytes.NewReader(theirPayload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Each node's own "notes" must still be its own.
	waitFor(t, "both nodes to settle", 10*time.Second, func() bool {
		f, err := theirs.db.FindFileByName(context.Background(), "notes")
		return err == nil && f != nil && f.Hash == contentKey(theirPayload)
	})

	if err := mine.Delete("notes"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Give the broadcast time to be handled.
	time.Sleep(500 * time.Millisecond)

	f, err := theirs.db.FindFileByName(context.Background(), "notes")
	if err != nil {
		t.Fatalf("FindFileByName: %v", err)
	}
	if f == nil {
		t.Fatal("deleting one node's file also deleted a different file of the same name on a peer")
	}
	if f.Hash != contentKey(theirPayload) {
		t.Errorf("the peer's name now refers to %s, want its own contents", short(f.Hash))
	}

	_, r, err := theirs.Get("notes")
	if err != nil {
		t.Fatalf("Get on the peer: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, theirPayload) {
		t.Error("the peer's file contents changed")
	}
}
