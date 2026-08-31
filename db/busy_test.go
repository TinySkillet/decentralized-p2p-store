package db

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestTransactionSurvivesAConcurrentWriter reproduces the failure a one-shot
// command hits against a running node: both processes open the same file,
// and the command's migration transaction reads before it writes. In WAL
// mode, a deferred transaction that tries to upgrade read → write after
// another connection has committed gets SQLITE_BUSY immediately — the busy
// handler is deliberately bypassed on that path, so busy_timeout never
// engages. Transactions must take the write lock at BEGIN instead.
func TestTransactionSurvivesAConcurrentWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")

	node, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()
	if err := node.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	command, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer command.Close()

	ctx := context.Background()
	tx, err := command.sql.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()

	// The transaction reads first, as Migrate's columnExists checks do.
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM peers`).Scan(&n); err != nil {
		t.Fatalf("read inside the transaction: %v", err)
	}

	// The running node commits a write in between — with an immediate
	// transaction it waits for the lock, so give it its own goroutine.
	wrote := make(chan error, 1)
	go func() {
		now := time.Now()
		wrote <- node.UpsertPeer(ctx, Peer{NodeID: "n", Address: "10.0.0.1:1", Status: "connected", LastSeen: &now})
	}()

	select {
	case err := <-wrote:
		if err != nil {
			t.Fatalf("the concurrent write failed: %v", err)
		}
		// Deferred transactions reach here (the write slipped in); immediate
		// ones resolve the send after this transaction commits.
	case <-time.After(200 * time.Millisecond):
	}

	// Now the transaction writes. This is where a deferred transaction dies
	// with "database is locked (5) (SQLITE_BUSY)".
	if _, err := tx.ExecContext(ctx, `INSERT INTO settings(key, value) VALUES('busy-test', 'x')
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`); err != nil {
		t.Fatalf("write after a concurrent commit: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if err := <-wrote; err != nil {
		t.Fatalf("the concurrent write failed: %v", err)
	}
}
