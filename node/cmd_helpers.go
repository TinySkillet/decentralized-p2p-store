package node

import (
	"context"
	"path/filepath"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"

	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// NewServer builds a long-lived node whose identity is persisted in db.
func NewServer(listenAddr string, db *dbpkg.DB, nodes ...string) (*FileServer, error) {
	identity, err := LoadOrInitIdentity(db)
	if err != nil {
		return nil, err
	}

	return newServer(listenAddr, identity, identity, db, storageRootFor(db), nodes...)
}

// LoadOrInitIdentity returns the node's signing identity, persisted in db so
// that it survives restarts and is shared by every process using it.
func LoadOrInitIdentity(db *dbpkg.DB) (Identity, error) {
	if db == nil {
		return newIdentity()
	}

	keyBytes, err := db.GetOrCreateIdentityKey(context.Background(), func() ([]byte, error) {
		id, err := newIdentity()
		if err != nil {
			return nil, err
		}
		return id.PrivateKey(), nil
	})
	if err != nil {
		return Identity{}, err
	}

	identity, err := identityFromKey(keyBytes)
	if err != nil {
		return Identity{}, err
	}

	// Peers and the storage-owner check both look the node up by id, so the
	// recorded value has to follow the key rather than a value from an older
	// build. Every process derives the same id from the same key, so writing
	// it unconditionally is idempotent.
	if err := db.PutSetting(context.Background(), dbpkg.NodeIDSetting, identity.NodeID()); err != nil {
		return Identity{}, err
	}

	return identity, nil
}

// NewClient builds the short-lived node a one-shot command runs on.
//
// It borrows a running node's database for metadata and for the encryption
// key that its files are stored under, but it takes a fresh identity rather
// than the persisted one. It is a separate participant on the network, and
// sharing an identity would make the node it connects to refuse the
// connection as one to itself.
func NewClient(listenAddr string, db *dbpkg.DB, nodes ...string) (*FileServer, error) {
	identity, err := newIdentity()
	if err != nil {
		return nil, err
	}

	// Files belong to the database, not to this process. Without the
	// database's own identity a command would store files owned by a key that
	// disappears when it exits, leaving them undeletable by anyone.
	owner, err := LoadOrInitIdentity(db)
	if err != nil {
		return nil, err
	}

	s, err := newServer(listenAddr, identity, owner, db, storageRootFor(db), nodes...)
	if err != nil {
		return nil, err
	}

	// A command runs for seconds and then exits. Repair is the standing
	// node's job, and a transient one starting a cycle it cannot finish would
	// only push copies onto peers on its way out.
	s.RepairInterval = -1

	return s, nil
}

// storageRootFor keeps a node's files beside the database that indexes them,
// so the two cannot be separated by moving one of them.
func storageRootFor(db *dbpkg.DB) string {
	return filepath.Join(filepath.Dir(db.Path()), "files")
}

func newServer(listenAddr string, identity, owner Identity, db *dbpkg.DB, storageRoot string, nodes ...string) (*FileServer, error) {
	// A per-process key is only a placeholder; commands with a database
	// replace it with the node's persisted key.
	key, err := storage.NewEncryptionKey()
	if err != nil {
		return nil, err
	}

	tcpTransport := p2p.NewTCPTransport(p2p.TCPTransportOpts{
		ListenAddr: listenAddr,
		Decoder:    p2p.DefaultDecoder{},
	})

	// Set after construction: the handshake reads the port the transport
	// actually bound, which is only known once it exists.
	tcpTransport.HandshakeFunc = getHandshakeFunc(identity, tcpTransport)

	s := NewFileServer(FileServerOpts{
		Identity:          identity,
		OwnerIdentity:     owner,
		EncryptionKey:     key,
		PathTransformFunc: storage.CASPathTransformFunc,
		StorageRoot:       storageRoot,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
		DB:                db,
	})

	tcpTransport.OnPeer = s.OnPeer
	tcpTransport.OnPeerDisconnect = s.OnPeerDisconnect

	// Loaded here rather than in Serve. Enforcement must be in place before
	// the first peer can connect, and a one-shot command node never calls
	// Serve at all — it would otherwise trust nobody, including the peers its
	// own database has approved.
	if err := s.loadTrust(); err != nil {
		return nil, err
	}

	return s, nil
}

func LoadOrInitKey(d *dbpkg.DB) ([]byte, error) {
	return d.GetOrCreateDefaultKey(context.Background(), storage.NewEncryptionKey)
}
