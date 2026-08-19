package db

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
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
	if err := d.UpsertPeer(ctx, Peer{ID: ":4000", Address: ":4000", Status: "connected", LastSeen: &now}); err != nil {
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
		if err := d.UpsertPeer(ctx, Peer{ID: ":4000", Address: ":4000", Status: tc.status, LastSeen: &tc.seen}); err != nil {
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
		{ID: ":1", Address: ":1", Status: "connected", LastSeen: &older},
		{ID: ":2", Address: ":2", Status: "connected", LastSeen: &recent},
		{ID: ":3", Address: ":3", Status: "connected", LastSeen: &stale},
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
		if err := d.UpsertPeer(ctx, Peer{ID: addr, Address: addr, Status: "connected", LastSeen: &now}); err != nil {
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
	if err := d.UpsertPeer(ctx, Peer{ID: ":good", Address: ":good", Status: "connected", LastSeen: &now}); err != nil {
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
		{ID: ":keep", Address: ":keep", Status: "connected", LastSeen: &recent},
		{ID: ":drop", Address: ":drop", Status: "connected", LastSeen: &stale},
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

	if err := d.DeleteFile(ctx, "f1"); err != nil {
		t.Fatalf("DeleteFile: %v", err)
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
	if err := d.DeleteFile(context.Background(), "never-existed"); err != nil {
		t.Errorf("DeleteFile for a missing id returned %v, want nil", err)
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

	if _, _, err := d.DeleteFileByName(ctx, "hello"); err != nil {
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

	n, err := d.CountNamesForHash(ctx, "shared-digest")
	if err != nil {
		t.Fatalf("CountNamesForHash: %v", err)
	}
	if n != 3 {
		t.Errorf("CountNamesForHash = %d, want 3", n)
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

	hash, orphaned, err := d.DeleteFileByName(ctx, "goner")
	if err != nil {
		t.Fatalf("DeleteFileByName: %v", err)
	}
	if hash != "shared-digest" {
		t.Errorf("hash = %q, want shared-digest", hash)
	}
	if orphaned {
		t.Error("contents reported orphaned while another name still refers to them")
	}

	hash, orphaned, err = d.DeleteFileByName(ctx, "keeper")
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

	hash, orphaned, err := d.DeleteFileByName(context.Background(), "never-stored")
	if err != nil {
		t.Fatalf("DeleteFileByName: %v", err)
	}
	if hash != "" || orphaned {
		t.Errorf("got hash %q orphaned %v, want empty and false", hash, orphaned)
	}
}
