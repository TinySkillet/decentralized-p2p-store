package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	d, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return d
}

func TestMigrateIsIdempotent(t *testing.T) {
	d := newTestDB(t)
	// Every command runs Migrate on startup, so it must be safe to repeat.
	for i := range 3 {
		if err := d.Migrate(context.Background()); err != nil {
			t.Fatalf("Migrate call %d: %v", i+2, err)
		}
	}
}

func TestParseTimeAcceptsStoredFormats(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"canonical", "2025-12-07T21:16:45.473359503Z"},
		{"rfc3339", "2025-12-07T21:16:45Z"},
		{"sqlite CURRENT_TIMESTAMP", "2025-12-07 21:16:45"},
		// Written by earlier versions, monotonic suffix and duplicated zone.
		{"legacy go string", "2025-12-07 21:16:45.473359503 +0545 +0545 m=+0.014968535"},
		{"legacy without monotonic", "2025-12-07 21:16:45.473359503 +0545 +0545"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTime(tc.input)
			if err != nil {
				t.Fatalf("parseTime(%q): %v", tc.input, err)
			}
			if got.IsZero() {
				t.Errorf("parseTime(%q) returned the zero time", tc.input)
			}
			if y := got.Year(); y != 2025 {
				t.Errorf("parseTime(%q) year = %d, want 2025", tc.input, y)
			}
		})
	}
}

func TestParseTimeRejectsGarbage(t *testing.T) {
	for _, input := range []string{"", "   ", "not a time", "99999"} {
		if _, err := parseTime(input); err == nil {
			t.Errorf("parseTime(%q) returned nil error, want a failure", input)
		}
	}
}

func TestFormatTimeRoundTrips(t *testing.T) {
	// time.Now() carries a monotonic reading; formatting must drop it and
	// still reproduce the wall clock to nanosecond precision.
	now := time.Now()
	got, err := parseTime(formatTime(now))
	if err != nil {
		t.Fatalf("parseTime: %v", err)
	}
	if !got.Equal(now) {
		t.Errorf("round trip gave %v, want %v", got, now)
	}
}

func TestUpsertPeerAndGetActivePeers(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	now := time.Now()
	if err := d.UpsertPeer(ctx, Peer{NodeID: ":4000", Address: ":4000", Status: "connected", LastSeen: &now}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}

	peers, err := d.GetActivePeers(ctx, time.Hour, 10)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("got %d peers, want 1", len(peers))
	}
	if peers[0].Address != ":4000" || peers[0].Status != "connected" {
		t.Errorf("peer = %+v, want address :4000 status connected", peers[0])
	}
	if peers[0].LastSeen == nil {
		t.Fatal("LastSeen is nil after a round trip")
	}
	// The stored timestamp must survive the round trip; peer expiry depends
	// on comparing it against a cutoff.
	if diff := peers[0].LastSeen.Sub(now); diff > time.Millisecond || diff < -time.Millisecond {
		t.Errorf("LastSeen drifted by %v", diff)
	}
}

func TestUpsertPeerUpdatesExistingRow(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	first := time.Now().Add(-time.Minute)
	second := time.Now()

	for _, tc := range []struct {
		status string
		seen   time.Time
	}{{"connected", first}, {"disconnected", second}} {
		if err := d.UpsertPeer(ctx, Peer{NodeID: ":4000", Address: ":4000", Status: tc.status, LastSeen: &tc.seen}); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}

	peers, err := d.GetActivePeers(ctx, time.Hour, 10)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("got %d peers, want 1 after upserting the same address twice", len(peers))
	}
	if peers[0].Status != "disconnected" {
		t.Errorf("Status = %q, want the updated value", peers[0].Status)
	}
}

func TestGetActivePeersExcludesStaleAndOrdersByRecency(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	recent := time.Now().Add(-time.Minute)
	older := time.Now().Add(-10 * time.Minute)
	stale := time.Now().Add(-48 * time.Hour)

	for _, p := range []Peer{
		{NodeID: ":1", Address: ":1", Status: "connected", LastSeen: &older},
		{NodeID: ":2", Address: ":2", Status: "connected", LastSeen: &recent},
		{NodeID: ":3", Address: ":3", Status: "connected", LastSeen: &stale},
	} {
		if err := d.UpsertPeer(ctx, p); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}

	peers, err := d.GetActivePeers(ctx, time.Hour, 10)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("got %d peers, want 2 (the stale one excluded)", len(peers))
	}
	if peers[0].Address != ":2" || peers[1].Address != ":1" {
		t.Errorf("order = %s,%s, want :2,:1 (most recent first)", peers[0].Address, peers[1].Address)
	}
}

func TestGetActivePeersRespectsLimit(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	now := time.Now()
	for i := range 5 {
		addr := string(rune('a' + i))
		if err := d.UpsertPeer(ctx, Peer{NodeID: addr, Address: addr, Status: "connected", LastSeen: &now}); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}

	peers, err := d.GetActivePeers(ctx, time.Hour, 3)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}
	if len(peers) != 3 {
		t.Errorf("got %d peers, want 3", len(peers))
	}
}

func TestGetActivePeersSkipsUnparseableRow(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	now := time.Now()
	if err := d.UpsertPeer(ctx, Peer{NodeID: ":good", Address: ":good", Status: "connected", LastSeen: &now}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}
	// A corrupt row must not blind the node to every other peer.
	if _, err := d.SQL().ExecContext(ctx, `
		INSERT INTO peers(id,address,status,last_seen) VALUES(':bad',':bad','connected','wat')
	`); err != nil {
		t.Fatalf("inserting corrupt row: %v", err)
	}

	peers, err := d.GetActivePeers(ctx, time.Hour, 10)
	if err != nil {
		t.Fatalf("GetActivePeers returned an error for one bad row: %v", err)
	}
	if len(peers) != 1 || peers[0].Address != ":good" {
		t.Errorf("got %+v, want only the parseable peer", peers)
	}
}

func TestCleanupStalePeers(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	recent := time.Now()
	stale := time.Now().Add(-2 * time.Hour)
	for _, p := range []Peer{
		{NodeID: ":keep", Address: ":keep", Status: "connected", LastSeen: &recent},
		{NodeID: ":drop", Address: ":drop", Status: "connected", LastSeen: &stale},
	} {
		if err := d.UpsertPeer(ctx, p); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}

	removed, err := d.CleanupStalePeers(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupStalePeers: %v", err)
	}
	if removed != 1 {
		t.Errorf("removed %d peers, want 1", removed)
	}

	peers, err := d.GetActivePeers(ctx, 24*time.Hour, 10)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}
	if len(peers) != 1 || peers[0].Address != ":keep" {
		t.Errorf("survivors = %+v, want only :keep", peers)
	}
}

func TestInsertFileAndLookups(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	f := File{ID: "id1", Name: "hello", Hash: "hash1", Size: 42, LocalPath: "/tmp/hello"}
	if err := d.InsertFileWithKey(ctx, f, "default"); err != nil {
		t.Fatalf("InsertFileWithKey: %v", err)
	}

	files, err := d.ListFiles(ctx)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) != 1 || files[0].Name != "hello" || files[0].Size != 42 {
		t.Fatalf("ListFiles = %+v, want one hello/42 row", files)
	}

	found, err := d.FindFileByHash(ctx, "hash1")
	if err != nil {
		t.Fatalf("FindFileByHash: %v", err)
	}
	if found == nil || found.Name != "hello" {
		t.Errorf("FindFileByHash = %+v, want the hello row", found)
	}

	missing, err := d.FindFileByHash(ctx, "nope")
	if err != nil {
		t.Fatalf("FindFileByHash for a missing hash: %v", err)
	}
	if missing != nil {
		t.Errorf("FindFileByHash for a missing hash = %+v, want nil", missing)
	}
}

// TestInsertFileTwiceOverwrites pins that re-storing a key updates the row.
// A plain INSERT failed on the primary key, and the caller discarded the
// error, so the metadata silently kept the old size and path.
func TestInsertFileTwiceOverwrites(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	if err := d.InsertFileWithKey(ctx, File{ID: "id1", Name: "hello", Hash: "h", Size: 10, LocalPath: "/a"}, "default"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := d.InsertFileWithKey(ctx, File{ID: "id1", Name: "hello", Hash: "h", Size: 99, LocalPath: "/b"}, "default"); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	files, err := d.ListFiles(ctx)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("got %d rows, want 1", len(files))
	}
	if files[0].Size != 99 || files[0].LocalPath != "/b" {
		t.Errorf("row = %+v, want the updated size and path", files[0])
	}
}

func TestSharesRoundTrip(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	if err := d.InsertFileWithKey(ctx, File{ID: "f1", Name: "hello", Hash: "h", Size: 5, LocalPath: "/a"}, "default"); err != nil {
		t.Fatalf("InsertFileWithKey: %v", err)
	}
	for _, s := range []Share{
		{ID: "s1", FileID: "f1", PeerID: ":4000", Direction: "outgoing"},
		{ID: "s2", FileID: "f1", PeerID: ":5000", Direction: "outgoing"},
		{ID: "s3", FileID: "f1", PeerID: ":6000", Direction: "incoming"},
	} {
		if err := d.InsertShare(ctx, s); err != nil {
			t.Fatalf("InsertShare: %v", err)
		}
	}

	shares, err := d.ListShares(ctx)
	if err != nil {
		t.Fatalf("ListShares: %v", err)
	}
	if len(shares) != 3 {
		t.Fatalf("got %d shares, want 3", len(shares))
	}
	// The join must resolve the file name and size.
	for _, s := range shares {
		if s.FileName != "hello" || s.FileSize != 5 {
			t.Errorf("share %s = name %q size %d, want hello/5", s.ID, s.FileName, s.FileSize)
		}
	}

	outgoing, err := d.GetOutgoingSharePeers(ctx, "f1")
	if err != nil {
		t.Fatalf("GetOutgoingSharePeers: %v", err)
	}
	if len(outgoing) != 2 {
		t.Errorf("outgoing peers = %v, want the two outgoing ones only", outgoing)
	}
}

func TestInsertShareIsIdempotent(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	if err := d.InsertFileWithKey(ctx, File{ID: "f1", Name: "hello", Hash: "h", Size: 5, LocalPath: "/a"}, "default"); err != nil {
		t.Fatalf("InsertFileWithKey: %v", err)
	}

	// Share IDs are derived from file, peer and direction, so re-sharing the
	// same file to the same peer must not accumulate duplicate rows.
	s := Share{ID: "s1", FileID: "f1", PeerID: ":4000", Direction: "outgoing"}
	for i := range 3 {
		if err := d.InsertShare(ctx, s); err != nil {
			t.Fatalf("InsertShare call %d: %v", i+1, err)
		}
	}

	shares, err := d.ListShares(ctx)
	if err != nil {
		t.Fatalf("ListShares: %v", err)
	}
	if len(shares) != 1 {
		t.Errorf("got %d shares, want 1", len(shares))
	}
}

func TestDeleteFileRemovesRelatedRows(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	if err := d.InsertFileWithKey(ctx, File{ID: "f1", Name: "hello", Hash: "h", Size: 5, LocalPath: "/a"}, "default"); err != nil {
		t.Fatalf("InsertFileWithKey: %v", err)
	}
	if err := d.InsertShare(ctx, Share{ID: "s1", FileID: "f1", PeerID: ":4000", Direction: "outgoing"}); err != nil {
		t.Fatalf("InsertShare: %v", err)
	}

	if _, _, err := d.DeleteFileByName(ctx, "hello", "", "", nil); err != nil {
		t.Fatalf("DeleteFileByName: %v", err)
	}

	files, err := d.ListFiles(ctx)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("got %d files after delete, want 0", len(files))
	}

	shares, err := d.ListShares(ctx)
	if err != nil {
		t.Fatalf("ListShares: %v", err)
	}
	if len(shares) != 0 {
		t.Errorf("got %d shares after delete, want 0", len(shares))
	}

	var fileKeys int
	if err := d.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM file_keys WHERE file_id='f1'`).Scan(&fileKeys); err != nil {
		t.Fatalf("counting file_keys: %v", err)
	}
	if fileKeys != 0 {
		t.Errorf("got %d file_keys rows after delete, want 0", fileKeys)
	}
}

func TestDeleteMissingFileIsNotAnError(t *testing.T) {
	d := newTestDB(t)
	// Deletions are broadcast to peers that may not hold the file.
	if _, _, err := d.DeleteFileByName(context.Background(), "never-existed", "", "", nil); err != nil {
		t.Errorf("deleting a missing name returned %v, want nil", err)
	}
}

func TestGetOrCreateDefaultKeyIsStable(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	calls := 0
	gen := func() ([]byte, error) {
		calls++
		return bytes.Repeat([]byte{byte(calls)}, 32), nil
	}

	first, err := d.GetOrCreateDefaultKey(ctx, gen)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	second, err := d.GetOrCreateDefaultKey(ctx, gen)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if !bytes.Equal(first, second) {
		t.Error("the node's key changed between calls; stored files would become undecryptable")
	}
	if calls != 1 {
		t.Errorf("generator ran %d times, want 1", calls)
	}
}

func TestGetOrCreateDefaultKeyPropagatesGeneratorError(t *testing.T) {
	d := newTestDB(t)
	sentinel := errors.New("no entropy")

	_, err := d.GetOrCreateDefaultKey(context.Background(), func() ([]byte, error) { return nil, sentinel })
	if !errors.Is(err, sentinel) {
		t.Errorf("error = %v, want the generator's error", err)
	}
}

// TestGetOrCreateDefaultKeyDoesNotRekeyOnDatabaseError is a regression test.
// Any error from the lookup used to be read as "no key yet", so a transient
// database fault would mint a new key and orphan every stored file.
func TestGetOrCreateDefaultKeyDoesNotRekeyOnDatabaseError(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	if _, err := d.GetOrCreateDefaultKey(ctx, func() ([]byte, error) { return bytes.Repeat([]byte{1}, 32), nil }); err != nil {
		t.Fatalf("seeding the key: %v", err)
	}

	// Make the lookup fail with something other than "no rows".
	if _, err := d.SQL().ExecContext(ctx, `DROP TABLE keys`); err != nil {
		t.Fatalf("dropping keys table: %v", err)
	}

	generated := false
	_, err := d.GetOrCreateDefaultKey(ctx, func() ([]byte, error) {
		generated = true
		return bytes.Repeat([]byte{2}, 32), nil
	})

	if err == nil {
		t.Fatal("a database fault was reported as success")
	}
	if generated {
		t.Error("a database fault caused a new key to be generated, orphaning stored files")
	}
}

// TestSharesCannotOutliveTheirFile covers the foreign key: a replication
// record must not exist for a file that is not there.
func TestSharesCannotOutliveTheirFile(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	err := d.InsertShare(ctx, Share{ID: "orphan", FileID: "no-such-file", PeerID: ":4000", Direction: "outgoing"})
	if err == nil {
		t.Fatal("a share was inserted for a file that does not exist")
	}

	// And a share must disappear with the file it describes.
	if err := d.InsertFileWithKey(ctx, File{ID: "f1", Name: "hello", Hash: "h", Size: 5, LocalPath: "/a"}, "default"); err != nil {
		t.Fatalf("InsertFileWithKey: %v", err)
	}
	if err := d.InsertShare(ctx, Share{ID: "s1", FileID: "f1", PeerID: ":4000", Direction: "outgoing"}); err != nil {
		t.Fatalf("InsertShare: %v", err)
	}

	if _, _, err := d.DeleteFileByName(ctx, "hello", "", "", nil); err != nil {
		t.Fatalf("DeleteFileByName: %v", err)
	}

	shares, err := d.ListShares(ctx)
	if err != nil {
		t.Fatalf("ListShares: %v", err)
	}
	if len(shares) != 0 {
		t.Errorf("%d share(s) survived the file they describe, want 0", len(shares))
	}
}

func TestFindFileByName(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	if err := d.InsertFileWithKey(ctx, File{ID: "id1", Name: "report.pdf", Hash: "digest1", Size: 42, LocalPath: "/a"}, "default"); err != nil {
		t.Fatalf("InsertFileWithKey: %v", err)
	}

	found, err := d.FindFileByName(ctx, "report.pdf")
	if err != nil {
		t.Fatalf("FindFileByName: %v", err)
	}
	if found == nil {
		t.Fatal("FindFileByName returned nil for a stored name")
	}
	if found.Hash != "digest1" {
		t.Errorf("Hash = %q, want the content digest it maps to", found.Hash)
	}

	missing, err := d.FindFileByName(ctx, "absent.pdf")
	if err != nil {
		t.Fatalf("FindFileByName: %v", err)
	}
	if missing != nil {
		t.Errorf("FindFileByName for an unknown name = %+v, want nil", missing)
	}
}

// TestSeveralNamesShareOneHash is the metadata half of deduplication: the
// same contents stored under different names are separate rows pointing at
// one hash.
func TestSeveralNamesShareOneHash(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	for _, name := range []string{"first", "second", "third"} {
		if err := d.InsertFileWithKey(ctx, File{
			ID: name, Name: name, Hash: "shared-digest", Size: 10, LocalPath: "/blob",
		}, "default"); err != nil {
			t.Fatalf("InsertFileWithKey %s: %v", name, err)
		}
	}

	files, err := d.ListFiles(ctx)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	sharing := 0
	for _, f := range files {
		if f.Hash == "shared-digest" {
			sharing++
		}
	}
	if sharing != 3 {
		t.Errorf("%d names refer to the shared contents, want 3", sharing)
	}

	// And exactly one blob is reachable, however many names point at it.
	referenced, err := d.ReferencedHashes(ctx)
	if err != nil {
		t.Fatalf("ReferencedHashes: %v", err)
	}
	if len(referenced) != 1 {
		t.Errorf("%d distinct hashes referenced, want 1", len(referenced))
	}
}

// TestDeleteFileByNameReportsWhenContentsAreOrphaned drives the decision of
// whether the bytes on disk may be removed.
func TestDeleteFileByNameReportsWhenContentsAreOrphaned(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	for _, name := range []string{"keeper", "goner"} {
		if err := d.InsertFileWithKey(ctx, File{
			ID: name, Name: name, Hash: "shared-digest", Size: 10, LocalPath: "/blob",
		}, "default"); err != nil {
			t.Fatalf("InsertFileWithKey %s: %v", name, err)
		}
	}

	hash, orphaned, err := d.DeleteFileByName(ctx, "goner", "", "", nil)
	if err != nil {
		t.Fatalf("DeleteFileByName: %v", err)
	}
	if hash != "shared-digest" {
		t.Errorf("hash = %q, want shared-digest", hash)
	}
	if orphaned {
		t.Error("contents reported orphaned while another name still refers to them")
	}

	hash, orphaned, err = d.DeleteFileByName(ctx, "keeper", "", "", nil)
	if err != nil {
		t.Fatalf("DeleteFileByName: %v", err)
	}
	if !orphaned {
		t.Error("contents not reported orphaned after the last name was removed")
	}
	if hash != "shared-digest" {
		t.Errorf("hash = %q, want shared-digest", hash)
	}
}

func TestDeleteFileByNameForUnknownName(t *testing.T) {
	d := newTestDB(t)

	hash, orphaned, err := d.DeleteFileByName(context.Background(), "never-stored", "", "", nil)
	if err != nil {
		t.Fatalf("DeleteFileByName: %v", err)
	}
	if hash != "" || orphaned {
		t.Errorf("got hash %q orphaned %v, want empty and false", hash, orphaned)
	}
}

func TestHostOf(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1:3000":   "127.0.0.1",
		"10.0.0.5:44000":   "10.0.0.5",
		"[::1]:3000":       "::1",
		"example.com:3000": "example.com",
		"10.0.0.5":         "10.0.0.5",
		"":                 "",
	}
	for addr, want := range cases {
		if got := HostOf(addr); got != want {
			t.Errorf("HostOf(%q) = %q, want %q", addr, got, want)
		}
	}
}

func TestIsLoopbackHost(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1", "localhost"} {
		if !IsLoopbackHost(host) {
			t.Errorf("IsLoopbackHost(%q) = false, want true", host)
		}
	}
	for _, host := range []string{"10.0.0.5", "example.com", ""} {
		if IsLoopbackHost(host) {
			t.Errorf("IsLoopbackHost(%q) = true, want false", host)
		}
	}
}

func TestCountActivePeersForHost(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()
	now := time.Now()
	stale := time.Now().Add(-2 * time.Hour)

	peers := []Peer{
		{NodeID: "n1", Address: "10.0.0.5:3000", Status: "connected", LastSeen: &now},
		{NodeID: "n2", Address: "10.0.0.5:3001", Status: "connected", LastSeen: &now},
		{NodeID: "n3", Address: "10.0.0.6:3000", Status: "connected", LastSeen: &now},
		{NodeID: "n4", Address: "10.0.0.5:3002", Status: "connected", LastSeen: &stale},
	}
	for _, p := range peers {
		if err := d.UpsertPeer(ctx, p); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}

	n, err := d.CountActivePeersForHost(ctx, "10.0.0.5", time.Hour)
	if err != nil {
		t.Fatalf("CountActivePeersForHost: %v", err)
	}
	// The stale identity is not counted.
	if n != 2 {
		t.Errorf("count for 10.0.0.5 = %d, want 2", n)
	}

	if n, err = d.CountActivePeersForHost(ctx, "10.0.0.7", time.Hour); err != nil {
		t.Fatalf("CountActivePeersForHost: %v", err)
	} else if n != 0 {
		t.Errorf("count for an unseen host = %d, want 0", n)
	}
}

// TestGetActivePeersLimitsIdentitiesPerHost is the filtering §4.8.1 calls for:
// one machine must not be able to crowd the peer list this node gossips on.
func TestGetActivePeersLimitsIdentitiesPerHost(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	// One host claiming many identities, plus a couple of genuine peers.
	base := time.Now()
	for i := range 10 {
		seen := base.Add(-time.Duration(i) * time.Second)
		if err := d.UpsertPeer(ctx, Peer{
			NodeID:   fmt.Sprintf("sybil-node-%d", i),
			Address:  fmt.Sprintf("10.0.0.5:%d", 3000+i),
			Status:   "connected",
			LastSeen: &seen,
		}); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}
	for i := range 2 {
		seen := base.Add(-time.Duration(i) * time.Second)
		if err := d.UpsertPeer(ctx, Peer{
			NodeID:   fmt.Sprintf("real-node-%d", i),
			Address:  fmt.Sprintf("10.0.0.%d:3000", 20+i),
			Status:   "connected",
			LastSeen: &seen,
		}); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}

	peers, err := d.getActivePeers(ctx, time.Hour, 100, 3)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}

	perHost := map[string]int{}
	for _, p := range peers {
		perHost[HostOf(p.Address)]++
	}

	if perHost["10.0.0.5"] > 3 {
		t.Errorf("one host contributed %d peers, want at most 3", perHost["10.0.0.5"])
	}
	// The genuine peers must survive the filtering.
	if perHost["10.0.0.20"] != 1 || perHost["10.0.0.21"] != 1 {
		t.Errorf("legitimate peers were filtered out: %v", perHost)
	}
}

// TestGetActivePeersExemptsLoopback keeps the documented local testing setup
// working, where several nodes share one machine.
func TestGetActivePeersExemptsLoopback(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	base := time.Now()
	for i := range 6 {
		seen := base.Add(-time.Duration(i) * time.Second)
		if err := d.UpsertPeer(ctx, Peer{
			NodeID:   fmt.Sprintf("local-node-%d", i),
			Address:  fmt.Sprintf("127.0.0.1:%d", 3000+i),
			Status:   "connected",
			LastSeen: &seen,
		}); err != nil {
			t.Fatalf("UpsertPeer: %v", err)
		}
	}

	peers, err := d.getActivePeers(ctx, time.Hour, 100, 3)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}
	if len(peers) != 6 {
		t.Errorf("got %d loopback peers, want all 6: the limit must not apply locally", len(peers))
	}
}

// TestConcurrentKeyCreationAgreesOnOneKey is a regression test. A plain
// check-then-write let two processes sharing a database each mint an
// encryption key and overwrite the other's, which silently made every file
// encrypted under the loser undecryptable.
func TestConcurrentKeyCreationAgreesOnOneKey(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	keys := make([][]byte, 2)
	start := make(chan struct{})

	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			k, err := d.GetOrCreateDefaultKey(ctx, func() ([]byte, error) {
				return bytes.Repeat([]byte{byte(i + 1)}, 32), nil
			})
			if err != nil {
				t.Errorf("GetOrCreateDefaultKey: %v", err)
				return
			}
			keys[i] = k
		}(i)
	}
	close(start)
	wg.Wait()

	if !bytes.Equal(keys[0], keys[1]) {
		t.Fatalf("two callers got different keys: %x vs %x -- the loser's files are now undecryptable", keys[0][:4], keys[1][:4])
	}

	// And the key that was handed out must be the one actually stored.
	stored, err := d.GetKey(ctx, "default")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if !bytes.Equal(stored.KeyBytes, keys[0]) {
		t.Errorf("the stored key %x is not the one callers received %x", stored.KeyBytes[:4], keys[0][:4])
	}
}

// TestConcurrentNodeIDCreationAgreesOnOneID covers the same race on identity,
// which peers remember and use to recognise a connection back to a node.
func TestConcurrentNodeIDCreationAgreesOnOneID(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	ids := make([]string, 2)
	start := make(chan struct{})

	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id, err := d.GetOrCreateNodeID(ctx, func() (string, error) {
				return []string{"aaaa", "bbbb"}[i], nil
			})
			if err != nil {
				t.Errorf("GetOrCreateNodeID: %v", err)
				return
			}
			ids[i] = id
		}(i)
	}
	close(start)
	wg.Wait()

	if ids[0] != ids[1] {
		t.Fatalf("two callers got different node ids: %q vs %q", ids[0], ids[1])
	}
}

// buildLegacyPeerSchema creates the address-keyed peers table an older build
// would have written, so the migration can be exercised against it.
func buildLegacyPeerSchema(t *testing.T, path string) {
	t.Helper()

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer raw.Close()

	stmts := []string{
		`CREATE TABLE peers (
			id TEXT PRIMARY KEY,
			address TEXT NOT NULL UNIQUE,
			status TEXT NOT NULL,
			last_seen TIMESTAMP,
			node_id TEXT NOT NULL DEFAULT '',
			host TEXT NOT NULL DEFAULT ''
		);`,
		`CREATE TABLE files (
			id TEXT PRIMARY KEY, name TEXT NOT NULL, hash TEXT NOT NULL,
			size INTEGER NOT NULL, local_path TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE shares (
			id TEXT PRIMARY KEY, file_id TEXT NOT NULL, peer_id TEXT NOT NULL,
			direction TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		// One identity seen at two addresses, which the old key could not
		// express, plus a row from before identities were recorded.
		`INSERT INTO peers(id,address,status,last_seen,node_id,host) VALUES
			('10.0.0.5:3000','10.0.0.5:3000','disconnected','2025-01-01T00:00:00.000000000Z','identity-a','10.0.0.5'),
			('10.0.0.5:4000','10.0.0.5:4000','connected',   '2025-06-01T00:00:00.000000000Z','identity-a','10.0.0.5'),
			('10.0.0.9:3000','10.0.0.9:3000','connected',   '2025-06-01T00:00:00.000000000Z','identity-b','10.0.0.9'),
			('10.0.0.7:3000','10.0.0.7:3000','connected',   '2025-06-01T00:00:00.000000000Z','',          '10.0.0.7');`,
		`INSERT INTO files(id,name,hash,size,local_path) VALUES ('f1','hello','h1',5,'/tmp/hello');`,
		`INSERT INTO shares(id,file_id,peer_id,direction) VALUES ('s1','f1','10.0.0.5:4000','outgoing');`,
	}
	for _, stmt := range stmts {
		if _, err := raw.Exec(stmt); err != nil {
			t.Fatalf("seeding legacy schema: %v", err)
		}
	}
}

// TestMigrateRekeysPeersByIdentity covers the upgrade path. An address-keyed
// table held a row per address, so one node that had moved appeared as several
// peers, and a share recorded against an address could not be resolved back to
// whoever holds it.
func TestMigrateRekeysPeersByIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	buildLegacyPeerSchema(t, path)

	d, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	ctx := context.Background()
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	peers, err := d.GetActivePeers(ctx, 100*365*24*time.Hour, 100)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}

	byIdentity := map[string]Peer{}
	for _, p := range peers {
		if _, seen := byIdentity[p.NodeID]; seen {
			t.Errorf("identity %s appears more than once", p.NodeID)
		}
		byIdentity[p.NodeID] = p
	}

	// The node seen at two addresses collapses to one row, keeping the most
	// recent address.
	a, ok := byIdentity["identity-a"]
	if !ok {
		t.Fatal("identity-a did not survive the migration")
	}
	if a.Address != "10.0.0.5:4000" {
		t.Errorf("identity-a address = %q, want the most recently seen one", a.Address)
	}
	if _, ok := byIdentity["identity-b"]; !ok {
		t.Error("identity-b did not survive the migration")
	}

	// A row from before identities were recorded cannot be keyed, so it is
	// dropped and relearned rather than carried under an invented key.
	if _, ok := byIdentity[""]; ok {
		t.Error("a peer with no identity was carried across")
	}
	if len(byIdentity) != 2 {
		t.Errorf("got %d peers, want 2", len(byIdentity))
	}

	// The share follows the identity that held it.
	holders, err := d.GetOutgoingSharePeers(ctx, "f1")
	if err != nil {
		t.Fatalf("GetOutgoingSharePeers: %v", err)
	}
	if len(holders) != 1 || holders[0] != "identity-a" {
		t.Errorf("share holders = %v, want [identity-a]", holders)
	}

	// And that identity resolves back to somewhere dialable.
	addrs, err := d.AddressesForNodes(ctx, holders)
	if err != nil {
		t.Fatalf("AddressesForNodes: %v", err)
	}
	if addrs["identity-a"] != "10.0.0.5:4000" {
		t.Errorf("resolved address = %q, want 10.0.0.5:4000", addrs["identity-a"])
	}

	// Migrate runs on every startup, so it must be safe to repeat.
	for i := range 2 {
		if err := d.Migrate(ctx); err != nil {
			t.Fatalf("Migrate repeat %d: %v", i+2, err)
		}
	}
	again, err := d.GetActivePeers(ctx, 100*365*24*time.Hour, 100)
	if err != nil {
		t.Fatalf("GetActivePeers: %v", err)
	}
	if len(again) != len(peers) {
		t.Errorf("repeating Migrate changed the peer count from %d to %d", len(peers), len(again))
	}
}
