package node

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// TestUnauthorisedDeleteIsRefused is the property that makes deletion safe to
// make reliable. Tombstones mean a deletion sticks and spreads, so without
// authorisation any peer that completed a handshake could destroy another
// node's files across the whole network.
func TestUnauthorisedDeleteIsRefused(t *testing.T) {
	owner := newQuietNode(t)
	attacker := newQuietNode(t, owner.addr)

	waitForPeerCount(t, owner, 1)
	waitForPeerCount(t, attacker, 1)

	payload := randomBytes(t, 2048)
	if err := owner.Store("valuable", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	digest := storage.ContentKey(payload)

	// The attacker asks the owner to delete it, with its own identity and a
	// signature it is perfectly able to produce over the right bytes.
	forged := MessageDeleteFile{
		Name:      "valuable",
		Digest:    digest,
		Owner:     attacker.NodeID(),
		Signature: attacker.Identity.Sign(deleteTranscript("valuable", digest)),
	}

	err := owner.handleMessageDeleteFile("attacker", forged)
	if err == nil {
		t.Fatal("a deletion from a node that does not own the file was accepted")
	}
	if !strings.Contains(err.Error(), "owned by") {
		t.Errorf("error = %v, want it to say the file belongs to someone else", err)
	}

	// The file must be untouched and still readable.
	_, r, err := owner.Get("valuable")
	if err != nil {
		t.Fatalf("Get after the refused deletion: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("the file changed after a refused deletion")
	}
}

// TestDeleteWithForgedSignatureIsRefused covers the case where the attacker
// claims the owner's identity rather than its own.
func TestDeleteWithForgedSignatureIsRefused(t *testing.T) {
	owner := newQuietNode(t)

	payload := randomBytes(t, 1024)
	if err := owner.Store("valuable", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	digest := storage.ContentKey(payload)

	attacker := mustIdentity(t)

	cases := map[string]MessageDeleteFile{
		"no signature at all": {
			Name: "valuable", Digest: digest, Owner: owner.NodeID(),
		},
		"a signature from the wrong key": {
			Name: "valuable", Digest: digest, Owner: owner.NodeID(),
			Signature: attacker.Sign(deleteTranscript("valuable", digest)),
		},
		"a signature over a different file": {
			Name: "valuable", Digest: digest, Owner: owner.NodeID(),
			Signature: owner.Identity.Sign(deleteTranscript("something-else", digest)),
		},
		"a handshake proof reused as an authorisation": {
			Name: "valuable", Digest: digest, Owner: owner.NodeID(),
			Signature: owner.Identity.Sign(handshakeTranscript(owner.Identity.PublicKey(), attacker.PublicKey(), make([]byte, challengeSize))),
		},
	}

	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			if err := owner.handleMessageDeleteFile("attacker", msg); err == nil {
				t.Fatalf("a deletion with %s was accepted", name)
			}
			if _, _, err := owner.Get("valuable"); err != nil {
				t.Errorf("the file became unreadable after a refused deletion: %v", err)
			}
		})
	}
}

// TestOwnerCanDeleteAcrossTheNetwork is the positive case, so the refusals
// above are not passing because deletion is simply broken.
func TestOwnerCanDeleteAcrossTheNetwork(t *testing.T) {
	owner := newQuietNode(t)
	replica := newQuietNode(t, owner.addr)

	waitForPeerCount(t, owner, 1)
	waitForPeerCount(t, replica, 1)

	payload := randomBytes(t, 2048)
	if err := owner.Store("mine", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	waitFor(t, "the replica to receive it", 10*time.Second, func() bool {
		return replica.store.Has(storage.ContentKey(payload))
	})

	if err := owner.Delete("mine"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// The peer must accept the owner's authorisation and forget the name.
	waitFor(t, "the replica to accept the deletion", 10*time.Second, func() bool {
		f, err := replica.db.FindFileByName(context.Background(), "mine")
		return err == nil && f == nil
	})
}

// TestReplicaRecordsTheOwner checks that a copy carries who is entitled to
// delete it; otherwise a peer holding a replica could not tell.
func TestReplicaRecordsTheOwner(t *testing.T) {
	owner := newQuietNode(t)
	replica := newQuietNode(t, owner.addr)

	waitForPeerCount(t, owner, 1)
	waitForPeerCount(t, replica, 1)

	payload := randomBytes(t, 1024)
	if err := owner.Store("shared", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	waitFor(t, "the replica to record the file", 10*time.Second, func() bool {
		f, err := replica.db.FindFileByName(context.Background(), "shared")
		return err == nil && f != nil
	})

	f, err := replica.db.FindFileByName(context.Background(), "shared")
	if err != nil {
		t.Fatalf("FindFileByName: %v", err)
	}
	if f.Owner != owner.NodeID() {
		t.Errorf("Owner = %q, want the storing node %q", storage.Short(f.Owner), storage.Short(owner.NodeID()))
	}
}

// TestDeletingSomeoneElsesFileLocallyIsRefused covers the CLI path: a node
// cannot authorise removing a file it does not own, so it does not silently
// delete its local copy and leave the network inconsistent.
func TestDeletingSomeoneElsesFileLocallyIsRefused(t *testing.T) {
	owner := newQuietNode(t)
	replica := newQuietNode(t, owner.addr)

	waitForPeerCount(t, owner, 1)
	waitForPeerCount(t, replica, 1)

	payload := randomBytes(t, 1024)
	if err := owner.Store("theirs", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	waitFor(t, "the replica to record the file", 10*time.Second, func() bool {
		f, err := replica.db.FindFileByName(context.Background(), "theirs")
		return err == nil && f != nil
	})

	err := replica.Delete("theirs")
	if err == nil {
		t.Fatal("a node deleted a file it does not own")
	}
	if !strings.Contains(err.Error(), "owned by") {
		t.Errorf("error = %v, want it to name the owner", err)
	}
}

// TestFilesStoredByACommandStayDeletable is a regression test. A one-shot
// command joins the network under a throwaway key so the node whose database
// it borrows does not refuse the connection as one to itself. When ownership
// followed that key too, the command stored files owned by an identity that
// ceased to exist the moment it exited, so nobody could ever delete them.
func TestFilesStoredByACommandStayDeletable(t *testing.T) {
	node := newQuietNode(t)
	node.OwnsDatabase = true

	// A command against the same database, with its own network identity.
	client, err := NewClient(freeAddr(t), node.db)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.EncryptionKey = node.EncryptionKey
	client.RepairInterval = -1
	client.SweepInterval = -1
	if err := client.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go client.Serve()
	t.Cleanup(client.Stop)

	// It joins under a throwaway key, which is what makes this work at all.
	if client.NodeID() == node.NodeID() {
		t.Fatal("the command shares the node's network identity; it would be refused as a self connection")
	}

	payload := randomBytes(t, 1024)
	if err := client.Store("stored-by-command", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Ownership must follow the database, not the process.
	f, err := node.db.FindFileByName(context.Background(), "stored-by-command")
	if err != nil {
		t.Fatalf("FindFileByName: %v", err)
	}
	if f == nil {
		t.Fatal("the file was not recorded")
	}
	if f.Owner != node.OwnerID() {
		t.Errorf("Owner = %q, want the database's identity %q", storage.Short(f.Owner), storage.Short(node.OwnerID()))
	}

	// And the node itself must be able to delete what its command stored.
	if err := node.Delete("stored-by-command"); err != nil {
		t.Fatalf("the node cannot delete a file its own command stored: %v", err)
	}
}
