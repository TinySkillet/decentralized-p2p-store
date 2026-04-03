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

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcpTransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: GetHandshakeFunc(listenAddr),
		Decoder:       p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcpTransportOpts)

	fileServerOpts := FileServerOpts{
		EncryptionKey:     newEcryptionKey(),
		PathTransformFunc: CASPathTransformFunc,
		StorageRoot:       getStorageRoot(listenAddr),
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}
	s := NewFileServer(fileServerOpts)
	tcpTransport.OnPeer = s.OnPeer

	return s
}

func makeServerWithDB(listenAddr string, db *dbpkg.DB, nodes ...string) *FileServer {
	tcpTransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: GetHandshakeFunc(listenAddr),
		Decoder:       p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcpTransportOpts)

	var storageRoot string
	if db != nil {
		dbDir := filepath.Dir(db.Path())
		storageRoot = filepath.Join(dbDir, "files")
	} else {
		storageRoot = getStorageRoot(listenAddr)
	}

	fileServerOpts := FileServerOpts{
		EncryptionKey:     newEcryptionKey(),
		PathTransformFunc: CASPathTransformFunc,
		StorageRoot:       storageRoot,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
		DB:                db,
	}
	s := NewFileServer(fileServerOpts)
	tcpTransport.OnPeer = s.OnPeer
	return s
}

func loadOrInitKey(d *dbpkg.DB) ([]byte, error) {
	return d.GetOrCreateDefaultKey(context.Background(), newEcryptionKey)
}
