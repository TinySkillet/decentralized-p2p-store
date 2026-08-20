package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
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

// File maps a name to the content-addressed hash of the bytes stored under
// it. Several names may share a hash: identical contents are stored once on
// disk, and each name is a row pointing at the same blob.
type File struct {
	ID   string
	Name string

	// Hash is the hex SHA-256 of the file's plaintext. It identifies the
	// contents on disk and on the wire, and is what a peer verifies a
	// transfer against.
	Hash string

	Size      int64
	LocalPath string

	// Owner is the node id that stored the file. A deletion has to be signed
	// by this identity, so that reaching a peer is not by itself permission
	// to destroy what it holds.
	Owner     string
	CreatedAt time.Time
}

type Peer struct {
	ID       string
	Address  string
	NodeID   string
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

// HostOf returns the host half of a "host:port" address. An address without a
// port is treated as a bare host.
func HostOf(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return strings.TrimSpace(address)
	}
	return host
}

// loopbackHosts are exempt from the per-host peer limit. Running several
// nodes on one machine is a normal development and testing setup, and is the
// arrangement the local testing instructions describe.
var loopbackHosts = map[string]bool{
	"127.0.0.1": true,
	"::1":       true,
	"localhost": true,
}

// IsLoopbackHost reports whether the per-host peer limit should be skipped.
func IsLoopbackHost(host string) bool {
	return loopbackHosts[host]
}

func (d *DB) UpsertPeer(ctx context.Context, p Peer) error {
	var lastSeen any
	if p.LastSeen != nil {
		lastSeen = formatTime(*p.LastSeen)
	}

	// Conflicts can arrive on either unique column, so both are handled.
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO peers(id,address,node_id,host,status,last_seen)
		VALUES(?,?,?,?,?,?)
		ON CONFLICT(address) DO UPDATE SET
			node_id=excluded.node_id,
			host=excluded.host,
			status=excluded.status,
			last_seen=excluded.last_seen
		ON CONFLICT(id) DO UPDATE SET
			address=excluded.address,
			node_id=excluded.node_id,
			host=excluded.host,
			status=excluded.status,
			last_seen=excluded.last_seen
	`, p.ID, p.Address, p.NodeID, HostOf(p.Address), p.Status, lastSeen)
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
		INSERT INTO files(id,name,hash,size,local_path,owner)
		VALUES(?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET
			name=excluded.name,
			hash=excluded.hash,
			size=excluded.size,
			local_path=excluded.local_path,
			owner=excluded.owner,
			created_at=CURRENT_TIMESTAMP
	`, f.ID, f.Name, f.Hash, f.Size, f.LocalPath, f.Owner); err != nil {
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
		SELECT id,name,hash,size,local_path,owner,created_at FROM files ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID, &f.Name, &f.Hash, &f.Size, &f.LocalPath, &f.Owner, &f.CreatedAt); err != nil {
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
		SELECT id,name,hash,size,local_path,owner,created_at FROM files WHERE hash=? LIMIT 1
	`, hash)

	var f File
	if err := row.Scan(&f.ID, &f.Name, &f.Hash, &f.Size, &f.LocalPath, &f.Owner, &f.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &f, nil
}

// InsertReplica records a copy received from a peer and returns the name it
// was filed under.
//
// A replica must not take over a name this node already uses for different
// contents. Checking and inserting happen in one transaction because the
// local Store path writes the same row: a separate read and write let a local
// file be committed in between and then silently overwritten.
//
// When the name is taken, the copy is filed under its digest instead, so it
// is still held for the network and still repairable.
func (d *DB) InsertReplica(ctx context.Context, f File, keyID string) (storedName string, err error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var existingHash string
	err = tx.QueryRowContext(ctx, `SELECT hash FROM files WHERE name=? LIMIT 1`, f.Name).Scan(&existingHash)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// The name is free.
	case err != nil:
		return "", err
	case existingHash != f.Hash:
		// Taken by different contents; fall back to the digest as the name.
		f.Name = f.Hash
		f.ID = f.Hash
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO files(id,name,hash,size,local_path,owner)
		VALUES(?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET
			name=excluded.name,
			hash=excluded.hash,
			size=excluded.size,
			local_path=excluded.local_path,
			owner=excluded.owner,
			created_at=CURRENT_TIMESTAMP
	`, f.ID, f.Name, f.Hash, f.Size, f.LocalPath, f.Owner); err != nil {
		return "", err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO file_keys(file_id,key_id) VALUES(?,?)
	`, f.ID, keyID); err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}
	return f.Name, nil
}

// FindFileByName resolves a name to the content it refers to, or nil if this
// node holds nothing under that name.
func (d *DB) FindFileByName(ctx context.Context, name string) (*File, error) {
	row := d.sql.QueryRowContext(ctx, `
		SELECT id,name,hash,size,local_path,owner,created_at FROM files WHERE name=? LIMIT 1
	`, name)

	var f File
	if err := row.Scan(&f.ID, &f.Name, &f.Hash, &f.Size, &f.LocalPath, &f.Owner, &f.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &f, nil
}

// ReferencedHashes returns every content hash that at least one name refers
// to. Anything on disk outside this set is unreachable and can be reclaimed.
func (d *DB) ReferencedHashes(ctx context.Context) (map[string]struct{}, error) {
	rows, err := d.sql.QueryContext(ctx, `SELECT DISTINCT hash FROM files`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		out[hash] = struct{}{}
	}
	return out, rows.Err()
}

// CountNamesForHash reports how many names refer to the given contents.
//
// Deduplication means one blob on disk can back several names, so the bytes
// may only be removed once the last name referring to them has gone.
func (d *DB) CountNamesForHash(ctx context.Context, hash string) (int, error) {
	var n int
	err := d.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM files WHERE hash=?`, hash).Scan(&n)
	return n, err
}

// DeleteFileByName removes one name and reports whether the contents it
// referred to are now unreferenced.
//
// When expectedHash is set the name is only removed if it currently refers to
// those contents. A deletion travels by name, and two nodes may legitimately
// use the same name for different files; without this guard, deleting your
// "notes" would delete everyone else's.
//
// The removal is recorded as a tombstone in the same transaction, so a peer
// that still holds the file cannot push it back afterwards.
func (d *DB) DeleteFileByName(ctx context.Context, name, expectedHash, owner string, signature []byte) (hash string, orphaned bool, err error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()

	var id string
	err = tx.QueryRowContext(ctx, `SELECT id,hash FROM files WHERE name=? LIMIT 1`, name).Scan(&id, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	if expectedHash != "" && hash != expectedHash {
		// This name refers to something else here; not ours to delete.
		return "", false, nil
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM file_keys WHERE file_id=?`, id); err != nil {
		return "", false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM shares WHERE file_id=?`, id); err != nil {
		return "", false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM files WHERE id=?`, id); err != nil {
		return "", false, err
	}

	// The authorisation is kept with the tombstone so it can be replayed to a
	// peer that still holds the file, which is how a deletion reaches nodes
	// that were unreachable when it was broadcast.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO deletions(name,digest,owner,signature,deleted_at)
		VALUES(?,?,?,?,CURRENT_TIMESTAMP)
		ON CONFLICT(name,digest) DO UPDATE SET
			owner=excluded.owner,
			signature=excluded.signature,
			deleted_at=CURRENT_TIMESTAMP
	`, name, hash, owner, signature); err != nil {
		return "", false, err
	}

	var remaining int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM files WHERE hash=?`, hash).Scan(&remaining); err != nil {
		return "", false, err
	}

	if err := tx.Commit(); err != nil {
		return "", false, err
	}

	return hash, remaining == 0, nil
}

// IsDeleted reports whether this name and content pair has been deleted here.
//
// Without this, a peer that was offline during a deletion still holds the file
// and its repair cycle pushes it back to every node that removed it, undoing
// the deletion across the network.
func (d *DB) IsDeleted(ctx context.Context, name, digest string) (bool, error) {
	var n int
	err := d.sql.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM deletions WHERE name=? AND digest=?
	`, name, digest).Scan(&n)
	return n > 0, err
}

// GetDeletion returns the authorisation recorded with a tombstone.
func (d *DB) GetDeletion(ctx context.Context, name, digest string) (owner string, signature []byte, ok bool, err error) {
	row := d.sql.QueryRowContext(ctx, `
		SELECT owner, signature FROM deletions WHERE name=? AND digest=?
	`, name, digest)

	var sig []byte
	if err := row.Scan(&owner, &sig); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, false, nil
		}
		return "", nil, false, err
	}
	return owner, sig, true, nil
}

// ClearDeletion forgets a tombstone, so storing the same file again under the
// same name works as the user expects.
func (d *DB) ClearDeletion(ctx context.Context, name, digest string) error {
	_, err := d.sql.ExecContext(ctx, `DELETE FROM deletions WHERE name=? AND digest=?`, name, digest)
	return err
}

// PruneDeletions drops tombstones older than maxAge. They only need to outlive
// the peers that might still be holding the deleted file.
func (d *DB) PruneDeletions(ctx context.Context, maxAge time.Duration) (int, error) {
	cutoff := formatTime(time.Now().Add(-maxAge))
	result, err := d.sql.ExecContext(ctx, `DELETE FROM deletions WHERE deleted_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	return int(n), err
}

// CountActivePeersForHost reports how many recently seen peers share an IP.
//
// One machine presenting many identities is the shape a Sybil attack takes,
// so the count is what the per-host admission limit is checked against.
func (d *DB) CountActivePeersForHost(ctx context.Context, host string, maxAge time.Duration) (int, error) {
	cutoff := formatTime(time.Now().Add(-maxAge))

	var n int
	err := d.sql.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT node_id) FROM peers
		WHERE host = ? AND node_id != '' AND last_seen IS NOT NULL AND last_seen > ?
	`, host, cutoff).Scan(&n)
	return n, err
}

// GetActivePeers returns recently active peers for discovery.
//
// At most maxPerHost peers are returned for any one IP address, so a single
// machine cannot crowd out the rest of the network in the peer lists this
// node gossips onward. Loopback is exempt, because several nodes on one
// machine is a normal local setup rather than an attack.
//
// A row with an unreadable timestamp is skipped rather than failing the whole
// query: one malformed record should not blind the node to every other peer.
func (d *DB) GetActivePeers(ctx context.Context, maxAge time.Duration, limit int) ([]Peer, error) {
	return d.getActivePeers(ctx, maxAge, limit, DefaultMaxPeersPerHost)
}

// DefaultMaxPeersPerHost bounds how many identities one IP may contribute to
// the peer list.
const DefaultMaxPeersPerHost = 3

func (d *DB) getActivePeers(ctx context.Context, maxAge time.Duration, limit, maxPerHost int) ([]Peer, error) {
	cutoff := formatTime(time.Now().Add(-maxAge))
	rows, err := d.sql.QueryContext(ctx, `
		SELECT id, address, node_id, status, last_seen FROM (
			SELECT id, address, node_id, host, status, last_seen,
			       ROW_NUMBER() OVER (PARTITION BY host ORDER BY last_seen DESC) AS rank_in_host
			FROM peers
			WHERE last_seen IS NOT NULL AND last_seen > ?
		)
		WHERE rank_in_host <= ? OR host IN ('127.0.0.1', '::1', 'localhost')
		ORDER BY last_seen DESC
		LIMIT ?
	`, cutoff, maxPerHost, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Peer
	for rows.Next() {
		var p Peer
		var lastSeen sql.NullString

		if err := rows.Scan(&p.ID, &p.Address, &p.NodeID, &p.Status, &lastSeen); err != nil {
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
// absent would mint a fresh key after a transient database fault and leave
// every previously stored file undecryptable.
//
// Creation is a conditional insert followed by a read inside one transaction,
// so concurrent callers all end up with whichever key was stored first. A
// plain check-then-write let two processes sharing a database each mint a key
// and overwrite the other's, which silently orphaned the files encrypted
// under the loser.
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

	stored, err := d.putKeyIfAbsent(ctx, id, "default", "AES-CTR-256", keyBytes)
	if err != nil {
		return nil, fmt.Errorf("storing encryption key: %w", err)
	}
	return stored, nil
}

// putKeyIfAbsent stores key material only if the id is unused, and returns
// whatever is stored afterwards.
//
// The conditional insert and the read back happen in one transaction so
// concurrent callers agree on one value. A plain check-then-write let two
// processes sharing a database each store their own and overwrite the other's.
func (d *DB) putKeyIfAbsent(ctx context.Context, id, label, algo string, keyBytes []byte) ([]byte, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO keys(id,label,algo,key_bytes,created_at)
		VALUES(?,?,?,?,CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO NOTHING
	`, id, label, algo, keyBytes); err != nil {
		return nil, err
	}

	var stored []byte
	if err := tx.QueryRowContext(ctx, `SELECT key_bytes FROM keys WHERE id=?`, id).Scan(&stored); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return stored, nil
}

// identityKeyID names the row holding this node's signing key.
const identityKeyID = "node_identity"

// GetOrCreateIdentityKey returns the private key naming this node, generating
// one on first use.
//
// The key is the node's identity: peers verify signatures against the matching
// public key, so it has to survive restarts and be the same for every process
// sharing this database.
func (d *DB) GetOrCreateIdentityKey(ctx context.Context, gen func() ([]byte, error)) ([]byte, error) {
	k, err := d.GetKey(ctx, identityKeyID)
	switch {
	case err == nil:
		return k.KeyBytes, nil
	case errors.Is(err, sql.ErrNoRows):
		// fall through and create one
	default:
		return nil, fmt.Errorf("loading identity key: %w", err)
	}

	keyBytes, err := gen()
	if err != nil {
		return nil, err
	}

	stored, err := d.putKeyIfAbsent(ctx, identityKeyID, "node identity", "Ed25519", keyBytes)
	if err != nil {
		return nil, fmt.Errorf("storing identity key: %w", err)
	}
	return stored, nil
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

// GetSetting returns a stored setting, or ok=false if it is not set.
func (d *DB) GetSetting(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := d.sql.QueryRowContext(ctx, `SELECT value FROM settings WHERE key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// PutSetting stores a setting, replacing any existing value.
func (d *DB) PutSetting(ctx context.Context, key, value string) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO settings(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value
	`, key, value)
	return err
}

// RepairCursorSetting remembers how far the last repair cycle got, so cycles
// round-robin through the files instead of restarting from the same end every
// time and never reaching the rest.
const RepairCursorSetting = "repair_cursor"

// NodeIDSetting is the settings key holding this node's identity.
const NodeIDSetting = "node_id"

// nodeIDSetting is retained for internal use.
const nodeIDSetting = NodeIDSetting

// ServingAddressSetting names the address of the long-lived node that owns a
// database. Commands open the same database to reuse its metadata and
// encryption key, and need to know they are borrowing another node's storage
// rather than holding an independent copy of it.
const ServingAddressSetting = "serving_address"

// StoredNodeID returns the identity persisted in this database, which belongs
// to the long-lived node that owns it.
func (d *DB) StoredNodeID(ctx context.Context) (string, bool, error) {
	return d.GetSetting(ctx, nodeIDSetting)
}

// GetOrCreateNodeID returns this node's stable identifier, generating one on
// first use.
//
// Identity has to survive restarts: peers remember it, and a node that
// changed identity on every start would look like an endless supply of new
// peers and could no longer recognise a connection back to itself.
//
// As with the encryption key, creation is a conditional insert and a read
// back in one transaction, so concurrent callers agree on one identity.
func (d *DB) GetOrCreateNodeID(ctx context.Context, gen func() (string, error)) (string, error) {
	id, ok, err := d.GetSetting(ctx, nodeIDSetting)
	if err != nil {
		return "", fmt.Errorf("loading node id: %w", err)
	}
	if ok && id != "" {
		return id, nil
	}

	id, err = gen()
	if err != nil {
		return "", err
	}

	stored, err := d.putSettingIfAbsent(ctx, nodeIDSetting, id)
	if err != nil {
		return "", fmt.Errorf("storing node id: %w", err)
	}
	return stored, nil
}

// putSettingIfAbsent stores value only if key is unset, and returns whatever
// value is stored afterwards.
func (d *DB) putSettingIfAbsent(ctx context.Context, key, value string) (string, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO settings(key,value) VALUES(?,?)
		ON CONFLICT(key) DO NOTHING
	`, key, value); err != nil {
		return "", err
	}

	var stored string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM settings WHERE key=?`, key).Scan(&stored); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return stored, nil
}
