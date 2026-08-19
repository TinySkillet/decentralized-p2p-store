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
	return newServer(listenAddr, nil, getStorageRoot(listenAddr), nodes...)
}

func makeServerWithDB(listenAddr string, db *dbpkg.DB, nodes ...string) (*FileServer, error) {
	storageRoot := getStorageRoot(listenAddr)
	if db != nil {
		// Keep a node's files beside the database that indexes them, so the
		// two cannot be separated by moving one of them.
		storageRoot = filepath.Join(filepath.Dir(db.Path()), "files")
	}
	return newServer(listenAddr, db, storageRoot, nodes...)
}

func newServer(listenAddr string, db *dbpkg.DB, storageRoot string, nodes ...string) (*FileServer, error) {
	// A per-process key is only a placeholder; commands with a database
	// replace it with the node's persisted key.
	key, err := newEncryptionKey()
	if err != nil {
		return nil, err
	}

	tcpTransport := p2p.NewTCPTransport(p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: GetHandshakeFunc(listenAddr),
		Decoder:       p2p.DefaultDecoder{},
	})

	s := NewFileServer(FileServerOpts{
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
