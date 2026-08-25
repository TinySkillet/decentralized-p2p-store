package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
)

func TestExpandHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("no home directory here: %v", err)
	}

	for _, tc := range []struct{ in, want string }{
		{"~/.p2p/p2p.db", filepath.Join(home, ".p2p/p2p.db")},
		{"~", home},
		{"p2p.db", "p2p.db"},
		{"/absolute/p2p.db", "/absolute/p2p.db"},
		// Only a leading "~/" is a home reference. A file genuinely named
		// "~backup" must not be rewritten.
		{"~backup", "~backup"},
		{"./~/p2p.db", "./~/p2p.db"},
	} {
		got, err := expandHome(tc.in)
		if err != nil {
			t.Fatalf("expandHome(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("expandHome(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// A read must not invent a database. Creating one and answering from it makes
// a command run against the wrong path report "nothing here" — the same answer
// a healthy but idle node gives, which is how a mistyped path looks like a
// working system with no data in it.
//
// Fails against a read path that calls openDB.
func TestReadingRefusesToCreateADatabase(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "nowhere", "p2p.db")

	err := onNodeReading(missing, func(t nodeTarget) error {
		return nil
	})
	if err == nil {
		t.Fatal("reading a database that does not exist was allowed")
	}
	if !strings.Contains(err.Error(), missing) {
		t.Errorf("the error does not name the path it looked at: %v", err)
	}

	if _, statErr := os.Stat(missing); !os.IsNotExist(statErr) {
		t.Fatal("a database was created by a command that only reads")
	}
	if _, statErr := os.Stat(filepath.Dir(missing)); !os.IsNotExist(statErr) {
		t.Fatal("a directory was created by a command that only reads")
	}
}

// A read must not join the network. With no node running there is no live peer
// set, so every peer is offline and that is the true answer; connecting first
// only meant waiting out the bootstrap timeout to learn the same thing.
//
// Fails against a read path that starts and bootstraps a node.
func TestReadingDoesNotJoinTheNetwork(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "p2p.db")

	// A database that remembers a peer and an address to bootstrap from,
	// neither of which is reachable. This is an ordinary stopped node.
	d, err := dbpkg.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	seen := time.Now()
	if err := d.UpsertPeer(ctx, dbpkg.Peer{
		NodeID: "a-peer-that-is-gone", Address: "192.0.2.1:53999",
		Status: "connected", LastSeen: &seen,
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}
	if err := d.PutSetting(ctx, dbpkg.ServingAddressSetting, "192.0.2.1:53999"); err != nil {
		t.Fatalf("PutSetting: %v", err)
	}
	d.Close()

	var peers int
	start := time.Now()
	err = onNodeReading(dbPath, func(target nodeTarget) error {
		views, err := target.Peers()
		if err != nil {
			return err
		}
		peers = len(views)
		for _, v := range views {
			// The database still says "connected". It is not.
			if v.Online {
				t.Error("a peer was reported online with no node running")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("onNodeReading: %v", err)
	}
	elapsed := time.Since(start)

	if peers != 1 {
		t.Fatalf("read %d peers, want the 1 recorded", peers)
	}
	// peerWaitTimeout alone is 10s, plus the discovery settle.
	if elapsed > 3*time.Second {
		t.Fatalf("the read took %v; it tried to join the network", elapsed)
	}
}

// The default database path must not depend on the working directory, or the
// same command run in two places talks to two different databases.
func TestTheDefaultDatabasePathIsAbsolute(t *testing.T) {
	if !strings.HasPrefix(defaultDBPath, "~/") {
		t.Fatalf("defaultDBPath is %q; a relative default resolves against the "+
			"working directory and silently changes meaning per directory", defaultDBPath)
	}

	expanded, err := expandHome(defaultDBPath)
	if err != nil {
		t.Fatalf("expandHome: %v", err)
	}
	if !filepath.IsAbs(expanded) {
		t.Fatalf("the default expands to %q, which is not absolute", expanded)
	}
}
