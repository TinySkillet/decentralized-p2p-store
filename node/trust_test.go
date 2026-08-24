package node

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// Stand-in identities for peers a node has never met. They must be full
// 64-character hex, because ResolvePeerID deliberately refuses to approve an
// abbreviation it cannot match: recording trust for an identity that does not
// exist is approval that silently never applies.
const (
	strangerID   = "1111111111111111111111111111111111111111111111111111111111111111"
	rememberedID = "2222222222222222222222222222222222222222222222222222222222222222"
)

// enforcingNode starts a node that refuses untrusted peers.
func enforcingNode(t *testing.T, bootstrap ...string) *testNode {
	t.Helper()
	return buildTestNode(t, freeAddr(t), nodeConfig{trustMode: dbpkg.TrustModeEnforcing}, bootstrap...)
}

// newTrustingPair starts two enforcing nodes that have approved each other, so
// a test about something else can still replicate.
func newTrustingPair(t *testing.T) (*testNode, *testNode) {
	t.Helper()

	a := enforcingNode(t)
	b := enforcingNode(t, a.addr)
	waitForPeerCount(t, a, 1)
	waitForPeerCount(t, b, 1)

	if err := a.Trust(b.NodeID(), "b"); err != nil {
		t.Fatalf("a trusting b: %v", err)
	}
	if err := b.Trust(a.NodeID(), "a"); err != nil {
		t.Fatalf("b trusting a: %v", err)
	}
	return a, b
}

// An untrusted peer may connect, be seen, and gossip. It may not put a file
// here that was never asked for.
func TestUntrustedPushIsRefused(t *testing.T) {
	receiver := enforcingNode(t)
	sender := enforcingNode(t, receiver.addr)
	waitForPeerCount(t, receiver, 1)
	waitForPeerCount(t, sender, 1)

	// Trust runs one way here. The sender approves the receiver, so it really
	// does try to send; the receiver has approved nobody, so it must refuse.
	// Without the sender's half it would never send at all and the test would
	// pass without exercising the refusal.
	if err := sender.Trust(receiver.NodeID(), "receiver"); err != nil {
		t.Fatalf("Trust: %v", err)
	}

	// The sender is connected and visible.
	views, err := receiver.PeerViews(context.Background())
	if err != nil {
		t.Fatalf("PeerViews: %v", err)
	}
	if len(views) != 1 || !views[0].Online {
		t.Fatal("an untrusted peer was not visible as connected")
	}

	if err := sender.Store("unwanted.txt", bytes.NewReader([]byte("not asked for"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Give the push time to be refused rather than accepted slowly.
	time.Sleep(2 * time.Second)

	files, err := receiver.FileViews(context.Background())
	if err != nil {
		t.Fatalf("FileViews: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("an untrusted push was accepted: %+v", files)
	}
}

// The refusal must drain the body and release the stream, or the connection
// wedges permanently and every later operation on it hangs.
//
// Fails against a refusal that returns without draining, or without the
// deferred CloseStream.
func TestTheConnectionSurvivesARefusedPush(t *testing.T) {
	receiver := enforcingNode(t)
	sender := enforcingNode(t, receiver.addr)
	waitForPeerCount(t, receiver, 1)
	waitForPeerCount(t, sender, 1)

	// One-way trust, so the sender sends and the receiver refuses.
	if err := sender.Trust(receiver.NodeID(), "receiver"); err != nil {
		t.Fatalf("Trust: %v", err)
	}

	// Big enough that a refusal which does not drain leaves bytes behind.
	refused := randomBytes(t, 256*1024)
	if err := sender.Store("refused.txt", bytes.NewReader(refused)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	waitFor(t, "the push to be refused", 15*time.Second, func() bool {
		files, err := receiver.FileViews(context.Background())
		return err == nil && len(files) == 0
	})

	// Now approve the sender and push again on the same connection. If the
	// stream was left open or the body left unread, this never arrives.
	if err := receiver.Trust(sender.NodeID(), "sender"); err != nil {
		t.Fatalf("Trust: %v", err)
	}

	accepted := randomBytes(t, 4096)
	if err := sender.Store("accepted.txt", bytes.NewReader(accepted)); err != nil {
		t.Fatalf("Store after trusting: %v", err)
	}

	waitFor(t, "the second push to be accepted", 20*time.Second, func() bool {
		files, err := receiver.FileViews(context.Background())
		if err != nil {
			return false
		}
		for _, f := range files {
			if f.Name == "accepted.txt" {
				return true
			}
		}
		return false
	})

	// And the contents are intact, so the connection was not left misaligned
	// by the refused transfer.
	_, r, err := receiver.Get("accepted.txt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, accepted) {
		t.Fatal("the file that followed a refused push arrived corrupted")
	}
}

// An untrusted peer may still serve contents this node asked for. The request
// names a digest and the transfer is verified against it, so refusing gains
// nothing and would break fetching on a network where trust is one-way.
//
// Guards against over-tightening.
func TestAnUntrustedPeerMayStillServeAFetch(t *testing.T) {
	holder := enforcingNode(t)
	fetcher := enforcingNode(t, holder.addr)
	waitForPeerCount(t, holder, 1)
	waitForPeerCount(t, fetcher, 1)

	payload := randomBytes(t, 8192)
	if err := holder.Store("wanted.txt", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Nobody trusts anybody, so the fetcher holds nothing.
	if fetcher.store.Has("wanted.txt") {
		t.Fatal("the fetcher already holds the file; the test proves nothing")
	}

	_, r, err := fetcher.Get("wanted.txt")
	if err != nil {
		t.Fatalf("an untrusted peer refused to serve a fetch: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("the fetched contents differ from the original")
	}
}

// A deletion is destructive and cannot be undone by the peer that asked, so it
// is refused from an unapproved peer.
func TestUntrustedDeleteIsRefused(t *testing.T) {
	owner, replica := newTrustingPair(t)

	payload := []byte("worth keeping")
	if err := owner.Store("guarded.txt", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}
	waitFor(t, "the replica to receive it", 15*time.Second, func() bool {
		files, err := replica.FileViews(context.Background())
		return err == nil && len(files) == 1
	})

	// The replica withdraws its approval of the owner, then the owner tries to
	// delete. The authorisation is genuine; the sender is no longer trusted.
	if _, err := replica.Untrust(owner.NodeID()); err != nil {
		t.Fatalf("Untrust: %v", err)
	}

	if err := owner.Delete("guarded.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	time.Sleep(2 * time.Second)

	files, err := replica.FileViews(context.Background())
	if err != nil {
		t.Fatalf("FileViews: %v", err)
	}
	if len(files) != 1 {
		t.Fatal("a deletion from an untrusted peer was carried out")
	}
}

// A file stored before ownership was recorded has no signature to check. It
// must still not be deletable by an unapproved peer.
//
// Fails against the original rule, which accepted an unsigned deletion from
// any handshaked peer.
func TestUnsignedLegacyDeleteRequiresTrust(t *testing.T) {
	n := enforcingNode(t)
	ctx := context.Background()

	// A file with no owner, as an upgrade leaves behind.
	if err := n.Store("legacy.txt", bytes.NewReader([]byte("from before ownership"))); err != nil {
		t.Fatalf("Store: %v", err)
	}
	files, err := n.DB.ListFiles(ctx)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if _, err := n.DB.SQL().ExecContext(ctx, `UPDATE files SET owner='' WHERE name=?`, "legacy.txt"); err != nil {
		t.Fatalf("clearing the owner: %v", err)
	}

	err = n.authorizeDelete(strangerID, MessageDeleteFile{
		Name:   "legacy.txt",
		Digest: files[0].Hash,
	})
	if err == nil {
		t.Fatal("an unsigned deletion of an unowned file was accepted from an untrusted peer")
	}

	// And a trusted peer may still do it, so upgrading does not strand data.
	if err := n.Trust(strangerID, ""); err != nil {
		t.Fatalf("Trust: %v", err)
	}
	if err := n.authorizeDelete(strangerID, MessageDeleteFile{
		Name:   "legacy.txt",
		Digest: files[0].Hash,
	}); err != nil {
		t.Fatalf("a trusted peer was refused an unsigned legacy deletion: %v", err)
	}
}

// Replication hands contents over, so it goes only to approved peers.
func TestReplicationSkipsUntrustedPeers(t *testing.T) {
	sender := enforcingNode(t)
	trusted := enforcingNode(t, sender.addr)

	// Deliberately open, so it would accept anything sent to it. The only
	// thing that can keep the file away is the sender declining to send it —
	// an enforcing node here would refuse the push itself, and the test could
	// not tell the two apart.
	untrusted := buildTestNode(t, freeAddr(t), nodeConfig{trustMode: dbpkg.TrustModeOpen}, sender.addr)
	waitForPeerCount(t, sender, 2)

	if err := sender.Trust(trusted.NodeID(), "trusted"); err != nil {
		t.Fatalf("Trust: %v", err)
	}
	if err := trusted.Trust(sender.NodeID(), "sender"); err != nil {
		t.Fatalf("Trust: %v", err)
	}

	if err := sender.Store("selective.txt", bytes.NewReader([]byte("only for the approved"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	ctx := context.Background()
	waitFor(t, "the trusted peer to receive it", 15*time.Second, func() bool {
		files, err := trusted.FileViews(ctx)
		return err == nil && len(files) == 1
	})

	time.Sleep(time.Second)
	files, err := untrusted.FileViews(ctx)
	if err != nil {
		t.Fatalf("FileViews: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("an untrusted peer was sent a replica: %+v", files)
	}
}

// A file kept alive only by peers that are no longer trusted is healthy today
// and fragile tomorrow. Reporting one number would hide that.
func TestUntrustedHoldersAreCountedSeparately(t *testing.T) {
	owner, replica := newTrustingPair(t)

	if err := owner.Store("shared.txt", bytes.NewReader([]byte("held by both"))); err != nil {
		t.Fatalf("Store: %v", err)
	}
	waitFor(t, "the replica to receive it", 15*time.Second, func() bool {
		files, err := replica.FileViews(context.Background())
		return err == nil && len(files) == 1
	})

	if _, err := owner.Untrust(replica.NodeID()); err != nil {
		t.Fatalf("Untrust: %v", err)
	}

	health, err := owner.ReplicationStatus()
	if err != nil {
		t.Fatalf("ReplicationStatus: %v", err)
	}
	if len(health) != 1 {
		t.Fatalf("ReplicationStatus returned %d files, want 1", len(health))
	}

	h := health[0]
	if h.Copies != 2 {
		t.Fatalf("Copies = %d, want 2; an untrusted holder still holds the file", h.Copies)
	}
	if len(h.UntrustedHolders) != 1 {
		t.Fatalf("UntrustedHolders = %v, want the one withdrawn peer", h.UntrustedHolders)
	}
	if h.TrustedCopies() != 1 {
		t.Fatalf("TrustedCopies = %d, want 1", h.TrustedCopies())
	}
}

// Trust must survive a restart: it lives in the database, and the in-memory
// set is only a cache of it.
func TestTrustSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/p2p.db"
	addr := freeAddr(t)

	first := buildTestNodeWithDB(t, dbPath, addr, nodeConfig{trustMode: dbpkg.TrustModeEnforcing})
	if err := first.Trust(rememberedID, "friend"); err != nil {
		t.Fatalf("Trust: %v", err)
	}
	first.Stop()
	first.db.Close()

	second := buildTestNodeWithDB(t, dbPath, addr, nodeConfig{trustMode: dbpkg.TrustModeEnforcing})
	waitFor(t, "trust to be loaded", 5*time.Second, func() bool {
		return second.Trusts(rememberedID)
	})

	trusted, err := second.TrustedPeers(context.Background())
	if err != nil {
		t.Fatalf("TrustedPeers: %v", err)
	}
	if len(trusted) != 1 || trusted[0].Label != "friend" {
		t.Fatalf("TrustedPeers returned %+v", trusted)
	}
}

// With enforcement off, nothing is refused. This is what an upgraded network
// gets, so that installing a new version changes no behaviour.
func TestOpenModeRefusesNothing(t *testing.T) {
	receiver := buildTestNode(t, freeAddr(t), nodeConfig{trustMode: dbpkg.TrustModeOpen})
	sender := buildTestNode(t, freeAddr(t), nodeConfig{trustMode: dbpkg.TrustModeOpen}, receiver.addr)
	waitForPeerCount(t, receiver, 1)

	if receiver.Trusts(sender.NodeID()) {
		t.Fatal("the sender is trusted; the test would prove nothing")
	}

	if err := sender.Store("accepted.txt", bytes.NewReader([]byte("open mode"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	waitFor(t, "the push to be accepted in open mode", 15*time.Second, func() bool {
		files, err := receiver.FileViews(context.Background())
		return err == nil && len(files) == 1
	})
}

// A node always trusts itself, or a command borrowing its database could not
// act on its behalf.
func TestANodeTrustsItself(t *testing.T) {
	n := enforcingNode(t)

	if !n.Trusts(n.NodeID()) {
		t.Fatal("a node did not trust its own network identity")
	}
	if !n.Trusts(n.OwnerID()) {
		t.Fatal("a node did not trust its own owner identity")
	}
	if n.Trusts("") {
		t.Fatal("an empty identity was trusted")
	}
}

// Setting an unknown mode must be refused rather than silently disabling
// enforcement.
func TestSetTrustModeRejectsUnknownModes(t *testing.T) {
	n := enforcingNode(t)

	if err := n.SetTrustMode("permissive"); err == nil {
		t.Fatal("an unknown trust mode was accepted")
	}
	if !n.TrustEnforced() {
		t.Fatal("a rejected mode change turned enforcement off")
	}

	if err := n.SetTrustMode(dbpkg.TrustModeOpen); err != nil {
		t.Fatalf("SetTrustMode: %v", err)
	}
	if n.TrustEnforced() {
		t.Fatal("enforcement stayed on after switching to open")
	}
}

// Approving a peer by the abbreviation every table shows must reach the real
// identity. Recording trust for a 12-character string that is not an identity
// would be approval that silently never applies — the worst outcome for a
// security control, because it looks like it worked.
func TestTrustAcceptsTheAbbreviationShownToTheOperator(t *testing.T) {
	a := enforcingNode(t)
	b := enforcingNode(t, a.addr)
	waitForPeerCount(t, a, 1)

	// Exactly what `p2p peers` prints.
	shown := storage.Short(b.NodeID())
	if shown == b.NodeID() {
		t.Fatal("Short did not abbreviate; the test would prove nothing")
	}

	if err := a.Trust(shown, "by abbreviation"); err != nil {
		t.Fatalf("Trust by abbreviation: %v", err)
	}

	if !a.Trusts(b.NodeID()) {
		t.Fatal("approving the abbreviation did not approve the peer")
	}

	trusted, err := a.TrustedPeers(context.Background())
	if err != nil {
		t.Fatalf("TrustedPeers: %v", err)
	}
	if len(trusted) != 1 || trusted[0].NodeID != b.NodeID() {
		t.Fatalf("trust was recorded against %+v, want the full identity", trusted)
	}
	if !trusted[0].Online {
		t.Fatal("the approved peer reads as offline, so trust was recorded against the wrong id")
	}
}

// An abbreviation matching several peers must be refused rather than resolved
// arbitrarily: approving the wrong peer is worse than approving none.
func TestTrustRefusesAnAmbiguousAbbreviation(t *testing.T) {
	n := enforcingNode(t)
	ctx := context.Background()

	now := time.Now()
	for _, id := range []string{
		"abcd111111111111111111111111111111111111111111111111111111111111",
		"abcd222222222222222222222222222222222222222222222222222222222222",
	} {
		if err := n.DB.UpsertPeer(ctx, dbpkg.Peer{
			NodeID: id, Address: "192.0.2.1:4000", Status: "known", LastSeen: &now,
		}); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}

	err := n.Trust("abcd", "")
	if err == nil {
		t.Fatal("an ambiguous abbreviation was resolved instead of refused")
	}

	trusted, terr := n.TrustedPeers(ctx)
	if terr != nil {
		t.Fatalf("TrustedPeers: %v", terr)
	}
	if len(trusted) != 0 {
		t.Fatalf("a refused approval still recorded %d peer(s)", len(trusted))
	}
}

// A full identity is taken as given, so a peer can be approved before it has
// ever connected — which is how trust is bootstrapped.
func TestTrustAcceptsAFullIdentityForAnUnseenPeer(t *testing.T) {
	n := enforcingNode(t)

	if err := n.Trust(strangerID, "not met yet"); err != nil {
		t.Fatalf("Trust: %v", err)
	}
	if !n.Trusts(strangerID) {
		t.Fatal("a peer approved by full identity is not trusted")
	}
}
