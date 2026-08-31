package db

import (
	"context"
	"database/sql"
	"errors"

	_ "modernc.org/sqlite"
)

type DB struct {
	sql  *sql.DB
	path string
}

func Open(path string) (*DB, error) {
	// _txlock=immediate: every transaction takes the write lock at BEGIN.
	// Every transaction here writes, and the deferred default is a trap: a
	// transaction that reads before writing — Migrate's columnExists checks —
	// gets SQLITE_BUSY *immediately* if another connection commits in
	// between, because SQLite bypasses the busy handler on that read→write
	// upgrade. A one-shot command migrating while the node writes hits
	// exactly that; taking the lock up front makes busy_timeout apply
	// instead.
	dsn := path + "?_txlock=immediate&_pragma=busy_timeout=5000&_pragma=journal_mode=WAL&_pragma=foreign_keys=on"
	d, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	// SQLite permits a single writer. Letting database/sql open a pool of
	// connections turns concurrent writes into "database is locked" errors
	// that busy_timeout cannot always absorb.
	d.SetMaxOpenConns(1)

	if err := d.PingContext(context.Background()); err != nil {
		d.Close()
		return nil, err
	}

	return &DB{sql: d, path: path}, nil
}

func (d *DB) Close() error { return d.sql.Close() }

func (d *DB) Path() string { return d.path }

func (d *DB) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS keys (
			id TEXT PRIMARY KEY,
			label TEXT,
			algo TEXT NOT NULL,
			key_bytes BLOB NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS files (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			hash TEXT NOT NULL,
			size INTEGER NOT NULL,
			local_path TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS file_keys (
			file_id TEXT NOT NULL,
			key_id TEXT NOT NULL,
			PRIMARY KEY (file_id, key_id)
		);`,
		`CREATE TABLE IF NOT EXISTS peers (
			id TEXT PRIMARY KEY,
			address TEXT NOT NULL UNIQUE,
			status TEXT NOT NULL,
			last_seen TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS deletions (
			name TEXT NOT NULL,
			digest TEXT NOT NULL,
			deleted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (name, digest)
		);`,
		// Trust is its own table rather than a column on peers, because
		// CleanupStalePeers deletes rows by recency: trust recorded there
		// would be silently revoked for any peer that went offline for an
		// hour, which is the opposite of what approving a peer means.
		`CREATE TABLE IF NOT EXISTS trusted_peers (
			node_id TEXT PRIMARY KEY,
			label TEXT NOT NULL DEFAULT '',
			trusted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS shares (
			id TEXT PRIMARY KEY,
			file_id TEXT NOT NULL,
			peer_id TEXT NOT NULL,
			direction TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,

		// Every lookup below is a hot path: resolving a hash back to a file
		// name on each incoming request, listing peers by recency, and
		// finding the peers that hold a file when it is deleted.
		`CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);`,
		`CREATE INDEX IF NOT EXISTS idx_files_name ON files(name);`,
		`CREATE INDEX IF NOT EXISTS idx_deletions_deleted_at ON deletions(deleted_at);`,
		`CREATE INDEX IF NOT EXISTS idx_peers_last_seen ON peers(last_seen);`,
		`CREATE INDEX IF NOT EXISTS idx_shares_file_id ON shares(file_id);`,
		`CREATE INDEX IF NOT EXISTS idx_shares_file_direction ON shares(file_id, direction);`,
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, s := range stmts {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			return err
		}
	}

	// Columns added after the original schema shipped. SQLite has no
	// ADD COLUMN IF NOT EXISTS, so an existing column is detected and the
	// statement skipped rather than treated as a failure.
	altered := []struct{ table, column, ddl string }{
		{"peers", "node_id", `ALTER TABLE peers ADD COLUMN node_id TEXT NOT NULL DEFAULT ''`},
		{"peers", "addrs", `ALTER TABLE peers ADD COLUMN addrs TEXT NOT NULL DEFAULT ''`},
		{"peers", "host", `ALTER TABLE peers ADD COLUMN host TEXT NOT NULL DEFAULT ''`},
		{"files", "digest", `ALTER TABLE files ADD COLUMN digest TEXT NOT NULL DEFAULT ''`},
		{"files", "owner", `ALTER TABLE files ADD COLUMN owner TEXT NOT NULL DEFAULT ''`},
		{"deletions", "owner", `ALTER TABLE deletions ADD COLUMN owner TEXT NOT NULL DEFAULT ''`},
		{"deletions", "signature", `ALTER TABLE deletions ADD COLUMN signature BLOB`},
	}
	for _, a := range altered {
		has, err := columnExists(ctx, tx, a.table, a.column)
		if err != nil {
			return err
		}
		if has {
			continue
		}
		if _, err := tx.ExecContext(ctx, a.ddl); err != nil {
			return err
		}
	}

	for _, stmt := range []string{
		`CREATE INDEX IF NOT EXISTS idx_peers_host ON peers(host)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	if err := backfillPeerHosts(ctx, tx); err != nil {
		return err
	}

	if err := repointPeersToIdentity(ctx, tx); err != nil {
		return err
	}

	if err := addShareForeignKey(ctx, tx); err != nil {
		return err
	}

	if err := seedTrustMode(ctx, tx); err != nil {
		return err
	}

	return tx.Commit()
}

// seedTrustMode chooses the initial trust mode for a database that has none.
//
// A database with data in it predates trust, so it starts open: recording and
// displaying trust without enforcing it means an upgrade changes no behaviour
// and cannot cut a working network off from itself. A fresh database has no
// history to preserve and starts enforcing.
//
// Existing peers are deliberately not auto-trusted. Trust that was never
// granted is not trust, and seeding it would make the approval step a
// formality on exactly the networks where it matters.
func seedTrustMode(ctx context.Context, tx *sql.Tx) error {
	var existing string
	err := tx.QueryRowContext(ctx, `SELECT value FROM settings WHERE key=?`, TrustModeSetting).Scan(&existing)
	if err == nil {
		// Already chosen, by an earlier migration or by the user.
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	populated, err := hasStoredData(ctx, tx)
	if err != nil {
		return err
	}

	mode := TrustModeEnforcing
	if populated {
		mode = TrustModeOpen
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING
	`, TrustModeSetting, mode)
	return err
}

// hasStoredData reports whether this database has ever held a file or met a
// peer, which is what distinguishes an upgrade from a first run.
func hasStoredData(ctx context.Context, tx *sql.Tx) (bool, error) {
	var n int
	if err := tx.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM files) + (SELECT COUNT(*) FROM peers)
	`).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// backfillPeerHosts fills in the host column for rows written before it
// existed, so the per-host limits apply to them too.
func backfillPeerHosts(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT id, address FROM peers WHERE host = ''`)
	if err != nil {
		return err
	}

	type peerRow struct{ id, address string }
	var pending []peerRow
	for rows.Next() {
		var p peerRow
		if err := rows.Scan(&p.id, &p.address); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	for _, p := range pending {
		if _, err := tx.ExecContext(ctx, `UPDATE peers SET host = ? WHERE id = ?`, HostOf(p.address), p.id); err != nil {
			return err
		}
	}
	return nil
}

// repointPeersToIdentity rekeys the peer table by node identity.
//
// A peer used to be keyed by the address it was reached at, which is wrong in
// two directions: one node has several addresses over its life, so it
// accumulated a row per address, and an address says nothing about who is
// there. Identity is the thing that persists, so it becomes the key.
//
// Unlike an added column this cannot be detected by inspecting the schema, so a
// settings sentinel records that it has run.
func repointPeersToIdentity(ctx context.Context, tx *sql.Tx) error {
	var done string
	err := tx.QueryRowContext(ctx, `SELECT value FROM settings WHERE key=?`, PeerKeySchemaSetting).Scan(&done)
	if err == nil && done != "" {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	stmts := []string{
		// No UNIQUE on address: one identity legitimately has several, and
		// several nodes on one machine share a host.
		`CREATE TABLE peers_new (
			id        TEXT PRIMARY KEY,
			address   TEXT NOT NULL DEFAULT '',
			addrs     TEXT NOT NULL DEFAULT '',
			host      TEXT NOT NULL DEFAULT '',
			status    TEXT NOT NULL,
			last_seen TIMESTAMP
		);`,

		// One row per identity, keeping the most recently seen address. Rows
		// with no identity are dropped rather than carried across under an
		// invented key: they are relearned on the next gossip.
		`INSERT INTO peers_new(id,address,addrs,host,status,last_seen)
			SELECT node_id,
			       address,
			       COALESCE(addrs,''),
			       host,
			       status,
			       MAX(last_seen)
			FROM peers
			WHERE node_id != ''
			GROUP BY node_id;`,

		// Shares recorded against an address follow the identity that held it.
		`UPDATE shares SET peer_id = (
			SELECT node_id FROM peers
			WHERE peers.id = shares.peer_id AND peers.node_id != ''
		) WHERE EXISTS (
			SELECT 1 FROM peers
			WHERE peers.id = shares.peer_id AND peers.node_id != ''
		);`,

		`DROP TABLE peers;`,
		`ALTER TABLE peers_new RENAME TO peers;`,
		`CREATE INDEX IF NOT EXISTS idx_peers_last_seen ON peers(last_seen);`,
		`CREATE INDEX IF NOT EXISTS idx_peers_host ON peers(host);`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO settings(key,value) VALUES(?, 'identity')
		ON CONFLICT(key) DO UPDATE SET value=excluded.value
	`, PeerKeySchemaSetting); err != nil {
		return err
	}

	return nil
}

// addShareForeignKey gives shares a foreign key onto files, so a replication
// record cannot outlive the file it describes.
//
// SQLite cannot add a constraint to an existing table, so the table is
// rebuilt when the constraint is missing. Rows whose file is already gone are
// dropped rather than carried across: they are exactly the orphans the
// constraint exists to prevent.
func addShareForeignKey(ctx context.Context, tx *sql.Tx) error {
	has, err := hasForeignKey(ctx, tx, "shares")
	if err != nil {
		return err
	}
	if has {
		return nil
	}

	stmts := []string{
		`CREATE TABLE shares_new (
			id TEXT PRIMARY KEY,
			file_id TEXT NOT NULL,
			peer_id TEXT NOT NULL,
			direction TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
		);`,
		`INSERT INTO shares_new(id,file_id,peer_id,direction,created_at)
			SELECT id,file_id,peer_id,direction,created_at FROM shares
			WHERE file_id IN (SELECT id FROM files);`,
		`DROP TABLE shares;`,
		`ALTER TABLE shares_new RENAME TO shares;`,
		`CREATE INDEX IF NOT EXISTS idx_shares_file_id ON shares(file_id);`,
		`CREATE INDEX IF NOT EXISTS idx_shares_file_direction ON shares(file_id, direction);`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// hasForeignKey reports whether table declares any foreign key.
func hasForeignKey(ctx context.Context, tx *sql.Tx, table string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT COUNT(*) FROM pragma_foreign_key_list(?)`, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if !rows.Next() {
		return false, rows.Err()
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		return false, err
	}
	return n > 0, rows.Err()
}

// columnExists reports whether table already has the named column.
func columnExists(ctx context.Context, tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (d *DB) SQL() *sql.DB { return d.sql }
