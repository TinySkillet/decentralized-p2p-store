package db

import (
	"context"
	"database/sql"

	_ "modernc.org/sqlite"
)

type DB struct {
	sql  *sql.DB
	path string
}

func Open(path string) (*DB, error) {
	dsn := path + "?_pragma=busy_timeout=5000&_pragma=journal_mode=WAL&_pragma=foreign_keys=on"
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
	return tx.Commit()
}

func (d *DB) SQL() *sql.DB { return d.sql }
