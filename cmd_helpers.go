package main

import (
	"context"
	"path/filepath"
	"strings"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

func getStorageRoot(listenAddr string) string {
	port := strings.TrimPrefix(listenAddr, ":")
	if strings.Contains(port, ":") {
		parts := strings.Split(port, ":")
		port = parts[len(parts)-1]
	}
	return "node_" + port + "_data"
}

func makeServer(listenAddr string, nodes ...string) (*FileServer, error) {
	nodeID, err := newNodeID()
	if err != nil {
		return nil, err
	}
	return newServer(listenAddr, nodeID, nil, getStorageRoot(listenAddr), nodes...)
}

// makeServerWithDB builds a long-lived node whose identity is persisted in db.
func makeServerWithDB(listenAddr string, db *dbpkg.DB, nodes ...string) (*FileServer, error) {
	nodeID, err := newNodeID()
	if db != nil {
		// Identity has to outlive the process: peers remember it, and it is
		// what lets this node recognise a connection back to itself.
		nodeID, err = db.GetOrCreateNodeID(context.Background(), newNodeID)
	}
	if err != nil {
		return nil, err
	}

	return newServer(listenAddr, nodeID, db, storageRootFor(listenAddr, db), nodes...)
}

// makeClientNode builds the short-lived node a one-shot command runs on.
//
// It borrows a running node's database for metadata and for the encryption
// key that its files are stored under, but it takes a fresh identity rather
// than the persisted one. It is a separate participant on the network, and
// sharing an identity would make the node it connects to refuse the
// connection as one to itself.
func makeClientNode(listenAddr string, db *dbpkg.DB, nodes ...string) (*FileServer, error) {
	nodeID, err := newNodeID()
	if err != nil {
		return nil, err
	}

	s, err := newServer(listenAddr, nodeID, db, storageRootFor(listenAddr, db), nodes...)
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
func storageRootFor(listenAddr string, db *dbpkg.DB) string {
	if db == nil {
		return getStorageRoot(listenAddr)
	}
	return filepath.Join(filepath.Dir(db.Path()), "files")
}

func newServer(listenAddr, nodeID string, db *dbpkg.DB, storageRoot string, nodes ...string) (*FileServer, error) {
	// A per-process key is only a placeholder; commands with a database
	// replace it with the node's persisted key.
	key, err := newEncryptionKey()
	if err != nil {
		return nil, err
	}

	tcpTransport := p2p.NewTCPTransport(p2p.TCPTransportOpts{
		ListenAddr: listenAddr,
		Decoder:    p2p.DefaultDecoder{},
	})

	// Set after construction: the handshake reads the port the transport
	// actually bound, which is only known once it exists.
	tcpTransport.HandshakeFunc = GetHandshakeFunc(nodeID, tcpTransport)

	s := NewFileServer(FileServerOpts{
		NodeID:            nodeID,
		EncryptionKey:     key,
		PathTransformFunc: CASPathTransformFunc,
		StorageRoot:       storageRoot,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
		DB:                db,
	})

	tcpTransport.OnPeer = s.OnPeer
	tcpTransport.OnPeerDisconnect = s.OnPeerDisconnect

	return s, nil
}

func loadOrInitKey(d *dbpkg.DB) ([]byte, error) {
	return d.GetOrCreateDefaultKey(context.Background(), newEncryptionKey)
}
