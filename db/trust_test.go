package db

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// The reason trust is its own table. CleanupStalePeers deletes peer rows by
// recency, so trust recorded on peers would be silently revoked for any peer
// that had been offline for an hour — turning a deliberate approval into
// something with a timeout nobody asked for.
//
// Fails against trust stored as a column on peers.
func TestCleanupStalePeersDoesNotRevokeTrust(t *testing.T) {
	d := openTestDB(t)
	ctx := context.Background()

	ancient := time.Now().Add(-48 * time.Hour)
	if err := d.UpsertPeer(ctx, Peer{
		NodeID: "old-friend", Address: "192.0.2.5:4000",
		Status: "disconnected", LastSeen: &ancient,
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}
	if err := d.TrustPeer(ctx, "old-friend", "a laptop that is usually off"); err != nil {
		t.Fatalf("TrustPeer: %v", err)
	}

	removed, err := d.CleanupStalePeers(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupStalePeers: %v", err)
	}
	if removed != 1 {
		t.Fatalf("CleanupStalePeers removed %d rows, want 1", removed)
	}

	trusted, err := d.ListTrustedPeers(ctx)
	if err != nil {
		t.Fatalf("ListTrustedPeers: %v", err)
	}
	if len(trusted) != 1 {
		t.Fatal("cleaning up a stale peer record revoked its trust")
	}
	if trusted[0].Label != "a laptop that is usually off" {
		t.Fatalf("the label was lost: %+v", trusted[0])
	}
}

func TestTrustPeerIsIdempotentAndUpdatesTheLabel(t *testing.T) {
	d := openTestDB(t)
	ctx := context.Background()

	if err := d.TrustPeer(ctx, "peer", "first"); err != nil {
		t.Fatalf("TrustPeer: %v", err)
	}
	if err := d.TrustPeer(ctx, "peer", "second"); err != nil {
		t.Fatalf("TrustPeer again: %v", err)
	}

	trusted, err := d.ListTrustedPeers(ctx)
	if err != nil {
		t.Fatalf("ListTrustedPeers: %v", err)
	}
	if len(trusted) != 1 {
		t.Fatalf("trusting twice produced %d rows, want 1", len(trusted))
	}
	if trusted[0].Label != "second" {
		t.Fatalf("label = %q, want the updated one", trusted[0].Label)
	}
}

func TestUntrustPeerReportsWhetherItChangedAnything(t *testing.T) {
	d := openTestDB(t)
	ctx := context.Background()

	had, err := d.UntrustPeer(ctx, "never-trusted")
	if err != nil {
		t.Fatalf("UntrustPeer: %v", err)
	}
	if had {
		t.Fatal("untrusting an unknown peer reported a change")
	}

	if err := d.TrustPeer(ctx, "peer", ""); err != nil {
		t.Fatalf("TrustPeer: %v", err)
	}
	had, err = d.UntrustPeer(ctx, "peer")
	if err != nil {
		t.Fatalf("UntrustPeer: %v", err)
	}
	if !had {
		t.Fatal("untrusting a trusted peer reported no change")
	}
}

func TestTrustPeerRequiresAnIdentity(t *testing.T) {
	d := openTestDB(t)

	if err := d.TrustPeer(context.Background(), "", "nameless"); err == nil {
		t.Fatal("a peer was trusted without an identity")
	}
}

// A fresh database has no history to preserve, so it enforces from the start.
func TestAFreshDatabaseStartsEnforcing(t *testing.T) {
	d := openTestDB(t)

	mode, ok, err := d.GetSetting(context.Background(), TrustModeSetting)
	if err != nil {
		t.Fatalf("GetSetting: %v", err)
	}
	if !ok {
		t.Fatal("a fresh database has no trust mode")
	}
	if mode != TrustModeEnforcing {
		t.Fatalf("a fresh database is %q, want %q", mode, TrustModeEnforcing)
	}
}

// A database with data in it predates trust. It starts open, so installing a
// new version does not cut a working network off from itself.
func TestAPopulatedDatabaseStartsOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "p2p.db")

	// A database as an older version left it: migrated, with a peer recorded,
	// and no trust mode.
	d, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	now := time.Now()
	if err := d.UpsertPeer(ctx, Peer{
		NodeID: "existing", Address: "192.0.2.9:4000", Status: "connected", LastSeen: &now,
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}
	if _, err := d.SQL().ExecContext(ctx, `DELETE FROM settings WHERE key=?`, TrustModeSetting); err != nil {
		t.Fatalf("clearing the trust mode: %v", err)
	}
	d.Close()

	// Reopening runs the migration again, which is where the choice is made.
	d, err = Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer d.Close()
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	mode, ok, err := d.GetSetting(ctx, TrustModeSetting)
	if err != nil {
		t.Fatalf("GetSetting: %v", err)
	}
	if !ok {
		t.Fatal("the migration left no trust mode")
	}
	if mode != TrustModeOpen {
		t.Fatalf("an upgraded database is %q, want %q", mode, TrustModeOpen)
	}

	// And no peer was auto-trusted: trust that was never granted is not trust.
	trusted, err := d.ListTrustedPeers(ctx)
	if err != nil {
		t.Fatalf("ListTrustedPeers: %v", err)
	}
	if len(trusted) != 0 {
		t.Fatalf("the migration auto-trusted %d existing peer(s)", len(trusted))
	}
}

// A mode already chosen must not be overwritten by a later migration.
func TestMigrateDoesNotOverwriteTheTrustMode(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "p2p.db")

	d, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := d.PutSetting(ctx, TrustModeSetting, TrustModeOpen); err != nil {
		t.Fatalf("PutSetting: %v", err)
	}

	// Migrate is run on every open, so it must be idempotent here.
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate again: %v", err)
	}
	defer d.Close()

	mode, _, err := d.GetSetting(ctx, TrustModeSetting)
	if err != nil {
		t.Fatalf("GetSetting: %v", err)
	}
	if mode != TrustModeOpen {
		t.Fatalf("a later migration changed the mode to %q", mode)
	}
}
