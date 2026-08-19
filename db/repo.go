package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
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

// timeLayout is the canonical on-disk format for timestamps this package
// writes. Values are normalised to UTC so string ordering matches time
// ordering, which is what the SQL comparisons in this file rely on.
const timeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// formatTime renders t for storage.
//
// Passing a time.Time straight to the driver previously stored Go's
// time.Time.String() output, complete with a monotonic clock suffix, which
// then had to be unpicked by hand on the way back out.
func formatTime(t time.Time) string {
	return t.UTC().Format(timeLayout)
}

// parseTime reads a stored timestamp. It accepts the canonical format, the
// format SQLite's CURRENT_TIMESTAMP produces, and the legacy time.Time.String()
// output written by earlier versions, so an existing database still loads.
func parseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty timestamp")
	}

	// Legacy rows carry a monotonic clock reading that no layout can parse.
	if idx := strings.Index(s, " m="); idx != -1 {
		s = strings.TrimSpace(s[:idx])
	}
	// Legacy rows also repeat the zone twice ("+0545 +0545"); keep the first.
	if fields := strings.Fields(s); len(fields) > 3 {
		s = strings.Join(fields[:3], " ")
	}

	layouts := []string{
		timeLayout,
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999999 -0700",
		"2006-01-02 15:04:05 -0700",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised timestamp %q", s)
}

func (d *DB) UpsertPeer(ctx context.Context, p Peer) error {
	var lastSeen any
	if p.LastSeen != nil {
		lastSeen = formatTime(*p.LastSeen)
	}

	// Conflicts can arrive on either unique column, so both are handled.
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO peers(id,address,status,last_seen)
		VALUES(?,?,?,?)
		ON CONFLICT(address) DO UPDATE SET
			status=excluded.status,
			last_seen=excluded.last_seen
		ON CONFLICT(id) DO UPDATE SET
			address=excluded.address,
			status=excluded.status,
			last_seen=excluded.last_seen
	`, p.ID, p.Address, p.Status, lastSeen)
	return err
}

// InsertFileWithKey records a file and the key it was encrypted under.
//
// Storing the same key twice is a normal operation (overwriting a file), so
// this upserts rather than failing on the primary key.
func (d *DB) InsertFileWithKey(ctx context.Context, f File, keyID string) error {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO files(id,name,hash,size,local_path)
		VALUES(?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET
			name=excluded.name,
			hash=excluded.hash,
			size=excluded.size,
			local_path=excluded.local_path,
			created_at=CURRENT_TIMESTAMP
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

// FindFileByHash returns the file with the given hash, or nil if there is
// none. It replaces scanning the entire file list on every incoming request.
func (d *DB) FindFileByHash(ctx context.Context, hash string) (*File, error) {
	row := d.sql.QueryRowContext(ctx, `
		SELECT id,name,hash,size,local_path,created_at FROM files WHERE hash=? LIMIT 1
	`, hash)

	var f File
	if err := row.Scan(&f.ID, &f.Name, &f.Hash, &f.Size, &f.LocalPath, &f.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &f, nil
}

// GetActivePeers returns recently active peers for discovery.
//
// A row with an unreadable timestamp is skipped rather than failing the whole
// query: one malformed record should not blind the node to every other peer.
func (d *DB) GetActivePeers(ctx context.Context, maxAge time.Duration, limit int) ([]Peer, error) {
	cutoff := formatTime(time.Now().Add(-maxAge))
	rows, err := d.sql.QueryContext(ctx, `
		SELECT id, address, status, last_seen
		FROM peers
		WHERE last_seen IS NOT NULL AND last_seen > ?
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
		var lastSeen sql.NullString

		if err := rows.Scan(&p.ID, &p.Address, &p.Status, &lastSeen); err != nil {
			return nil, err
		}

		if lastSeen.Valid {
			parsed, err := parseTime(lastSeen.String)
			if err != nil {
				log.Printf("skipping peer %s: %v", p.Address, err)
				continue
			}
			p.LastSeen = &parsed
		}

		out = append(out, p)
	}
	return out, rows.Err()
}

// CleanupStalePeers removes peer records not seen within maxAge.
func (d *DB) CleanupStalePeers(ctx context.Context, maxAge time.Duration) (int, error) {
	cutoff := formatTime(time.Now().Add(-maxAge))
	result, err := d.sql.ExecContext(ctx, `
		DELETE FROM peers WHERE last_seen IS NULL OR last_seen < ?
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

// GetOrCreateDefaultKey returns the node's encryption key, generating one on
// first use.
//
// Only a genuine "no such row" may trigger generation. Treating any error as
// absent (as an earlier version did) would mint a fresh key after a transient
// database fault and leave every previously stored file undecryptable.
func (d *DB) GetOrCreateDefaultKey(ctx context.Context, gen func() ([]byte, error)) ([]byte, error) {
	const id = "default"

	k, err := d.GetKey(ctx, id)
	switch {
	case err == nil:
		return k.KeyBytes, nil
	case errors.Is(err, sql.ErrNoRows):
		// fall through and create one
	default:
		return nil, fmt.Errorf("loading encryption key: %w", err)
	}

	keyBytes, err := gen()
	if err != nil {
		return nil, err
	}
	if err := d.PutKey(ctx, Key{
		ID:       id,
		Label:    "default",
		Algo:     "AES-CTR-256",
		KeyBytes: keyBytes,
	}); err != nil {
		return nil, fmt.Errorf("storing encryption key: %w", err)
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
		SELECT DISTINCT peer_id FROM shares WHERE file_id = ? AND direction = 'outgoing'
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
