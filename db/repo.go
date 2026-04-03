package db

import (
	"context"
	"strings"
	"time"
)

type Key struct {
	ID        string
	Label     string
	Algo      string
	KeyBytes  []byte
	CreatedAt time.Time
}

type File struct {
	ID        string
	Name      string
	Hash      string
	Size      int64
	LocalPath string
	CreatedAt time.Time
}

type Peer struct {
	ID       string
	Address  string
	Status   string
	LastSeen *time.Time
}

type Share struct {
	ID        string
	FileID    string
	PeerID    string
	Direction string
	CreatedAt time.Time
}

func (d *DB) UpsertPeer(ctx context.Context, p Peer) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO peers(id,address,status,last_seen)
		VALUES(?,?,?,?)
		ON CONFLICT(address) DO UPDATE SET
			status=excluded.status,
			last_seen=excluded.last_seen
	`, p.ID, p.Address, p.Status, p.LastSeen)
	return err
}

func (d *DB) InsertFileWithKey(ctx context.Context, f File, keyID string) error {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO files(id,name,hash,size,local_path)
		VALUES(?,?,?,?,?)
	`, f.ID, f.Name, f.Hash, f.Size, f.LocalPath); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO file_keys(file_id,key_id)
		VALUES(?,?)
	`, f.ID, keyID); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DB) ListFiles(ctx context.Context) ([]File, error) {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT id,name,hash,size,local_path,created_at FROM files ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID, &f.Name, &f.Hash, &f.Size, &f.LocalPath, &f.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// Returns recently active peers for discovery
func (d *DB) GetActivePeers(ctx context.Context, maxAge time.Duration, limit int) ([]Peer, error) {
	cutoff := time.Now().Add(-maxAge)
	rows, err := d.sql.QueryContext(ctx, `
		SELECT id, address, status, last_seen 
		FROM peers 
		WHERE last_seen > ?
		ORDER BY last_seen DESC
		LIMIT ?
	`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Peer
	for rows.Next() {
		var p Peer
		var lastSeenStr string

		if err := rows.Scan(&p.ID, &p.Address, &p.Status, &lastSeenStr); err != nil {
			return nil, err
		}

		// Parse the timestamp string to time.Time
		if lastSeenStr != "" {
			// SQLite stores time.Time as its String() representation, which includes monotonic clock
			// Format: "2025-12-07 21:16:45.473359503 +0545 +0545 m=+0.014968535"
			// We need to strip the monotonic clock part (everything from " m=" onwards)

			// Find and remove the monotonic clock component
			if idx := strings.Index(lastSeenStr, " m="); idx != -1 {
				lastSeenStr = lastSeenStr[:idx]
			}

			parts := strings.Fields(lastSeenStr)
			if len(parts) >= 3 {
				// parts = ["2025-12-07", "21:16:45.473359503", "+0545", "+0545"]
				// Keep only first 3 parts (date, time, first timezone)
				lastSeenStr = strings.Join(parts[:3], " ")
			}

			parsedTime, err := time.Parse("2006-01-02 15:04:05.999999999 -0700", lastSeenStr)
			if err != nil {
				// Try without nanoseconds
				parsedTime, err = time.Parse("2006-01-02 15:04:05 -0700", lastSeenStr)
				if err != nil {
					return nil, err
				}
			}
			p.LastSeen = &parsedTime
		}

		out = append(out, p)
	}
	return out, rows.Err()
}

// Removes stale peer records
func (d *DB) CleanupStalePeers(ctx context.Context, maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	result, err := d.sql.ExecContext(ctx, `
		DELETE FROM peers WHERE last_seen < ?
	`, cutoff)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(rowsAffected), nil
}

func (d *DB) GetKey(ctx context.Context, id string) (*Key, error) {
	row := d.sql.QueryRowContext(ctx, `
		SELECT id,label,algo,key_bytes,created_at FROM keys WHERE id=?
	`, id)
	var k Key
	if err := row.Scan(&k.ID, &k.Label, &k.Algo, &k.KeyBytes, &k.CreatedAt); err != nil {
		return nil, err
	}
	return &k, nil
}

func (d *DB) PutKey(ctx context.Context, k Key) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO keys(id,label,algo,key_bytes,created_at)
		VALUES(?,?,?,?,CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			label=excluded.label,
			algo=excluded.algo,
			key_bytes=excluded.key_bytes
	`, k.ID, k.Label, k.Algo, k.KeyBytes)
	return err
}

func (d *DB) GetOrCreateDefaultKey(ctx context.Context, gen func() []byte) ([]byte, error) {
	const id = "default"
	k, err := d.GetKey(ctx, id)
	if err == nil {
		return k.KeyBytes, nil
	}
	keyBytes := gen()
	if err := d.PutKey(ctx, Key{
		ID:       id,
		Label:    "default",
		Algo:     "AES-CTR-256",
		KeyBytes: keyBytes,
	}); err != nil {
		return nil, err
	}
	return keyBytes, nil
}

// ShareInfo contains share information with file details.
type ShareInfo struct {
	Share
	FileName string
	FileSize int64
}

func (d *DB) InsertShare(ctx context.Context, share Share) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO shares(id, file_id, peer_id, direction, created_at)
		VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			created_at = excluded.created_at
	`, share.ID, share.FileID, share.PeerID, share.Direction)
	return err
}

func (d *DB) ListShares(ctx context.Context) ([]ShareInfo, error) {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT s.id, s.file_id, s.peer_id, s.direction, s.created_at,
		       COALESCE(f.name, s.file_id) as file_name,
		       COALESCE(f.size, 0) as file_size
		FROM shares s
		LEFT JOIN files f ON s.file_id = f.id
		ORDER BY s.created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ShareInfo
	for rows.Next() {
		var si ShareInfo
		if err := rows.Scan(&si.ID, &si.FileID, &si.PeerID, &si.Direction, &si.CreatedAt, &si.FileName, &si.FileSize); err != nil {
			return nil, err
		}
		out = append(out, si)
	}
	return out, rows.Err()
}

// GetOutgoingSharePeers returns peer addresses that have received the file (outgoing shares).
func (d *DB) GetOutgoingSharePeers(ctx context.Context, fileID string) ([]string, error) {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT peer_id FROM shares WHERE file_id = ? AND direction = 'outgoing'
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var peers []string
	for rows.Next() {
		var peerID string
		if err := rows.Scan(&peerID); err != nil {
			return nil, err
		}
		peers = append(peers, peerID)
	}
	return peers, rows.Err()
}

func (d *DB) DeleteFile(ctx context.Context, fileID string) error {

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM file_keys WHERE file_id = ?
	`, fileID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM shares WHERE file_id = ?
	`, fileID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM files WHERE id = ?
	`, fileID); err != nil {
		return err
	}

	return tx.Commit()
}
