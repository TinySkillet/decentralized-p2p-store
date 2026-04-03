package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"context"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

func (s *FileServer) Start() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	if len(s.BootstrapNodes) != 0 {
		s.bootstrapNetwork()
	}

	s.loop()

	return nil
}

func (s *FileServer) loop() {

	defer func() {
		log.Printf("[%s] File server stopped due to error or user quit action\n", s.Transport.Address())
	}()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			if rpc.Stream {
				if err := s.handleStream(rpc.From); err != nil {
					log.Printf("[%s] Error handling stream: %v", s.Transport.Address(), err)
				}
				continue
			}

			var msg Message
			err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg)
			if err != nil {
				log.Printf("[%s] Decoding error: %v", s.Transport.Address(), err)
			}

			if err := s.handleMessage(rpc.From, &msg); err != nil {
				log.Printf("[%s] Error while handling message: %v\n", s.Transport.Address(), err)
			}

		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		return s.handleMessageStoreFile(from, v)
	case MessageGetFile:
		return s.handleMessageGetFile(from, v)
	case MessageDeleteFile:
		return s.handleMessageDeleteFile(from, v)
	case MessagePeerExchange:
		return s.handleMessagePeerExchange(from, v)
	}
	return nil
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	fmt.Printf("[%s] Received StoreFile message from %s for key %s. Expecting stream...\n", s.Transport.Address(), from, msg.Key)
	s.pendingFileTransfers[from] = msg
	return nil
}

func (s *FileServer) handleStream(from string) error {
	// Check for pending upload (StoreFile)
	if msg, ok := s.pendingFileTransfers[from]; ok {
		delete(s.pendingFileTransfers, from)

		peer, found := s.peers[from]
		if !found {
			return fmt.Errorf("peer (%s) could not be found in the peers list", from)
		}

		// Receive plaintext, write encrypted
		n, err := s.store.WriteEncrypt(s.EncryptionKey, msg.Key, io.LimitReader(peer, msg.Size))
		if err != nil {
			return err
		}

		fmt.Printf("[%s] Written %d bytes to disk (encrypted) from %s\n", s.Transport.Address(), n, from)

		// Record share in database if configured
		if s.DB != nil {
			shareID := hashKey(msg.Key + from + "incoming")
			_ = s.DB.InsertShare(context.Background(), dbpkg.Share{
				ID:        shareID,
				FileID:    msg.Key,
				PeerID:    from,
				Direction: "incoming",
			})
		}

		peer.CloseStream()

		// Signal download completion if anyone is waiting
		if ch, ok := s.downloadChannels[msg.Key]; ok {
			close(ch)
			delete(s.downloadChannels, msg.Key)
		}

		return nil
	}

	return fmt.Errorf("peer %s sent a stream but no pending transfer was found", from)
}

func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	fmt.Printf("[%s] Received request to serve file '%s'\n", s.Transport.Address(), msg.Key)

	keyToRead := msg.Key

	if s.DB != nil {
		files, err := s.DB.ListFiles(context.Background())
		if err == nil {
			for _, f := range files {
				if f.Hash == msg.Key {
					fmt.Printf("[%s] Found original key '%s' for hash '%s'\n", s.Transport.Address(), f.Name, msg.Key)
					keyToRead = f.Name
					break
				}
			}
		}
	}

	encSize, r, err := s.store.Read(keyToRead)
	if err != nil {
		return fmt.Errorf("[%s] Failed to read file %s: %v", s.Transport.Address(), keyToRead, err)
	}
	if rc, ok := r.(io.ReadCloser); ok {
		rc.Close()
	}

	plaintextSize := encSize - 16

	_, fileReader, err := s.store.ReadDecrypt(s.EncryptionKey, keyToRead)
	if err != nil {
		return err
	}

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not found in peer list", from)
	}

	storeMsg := Message{
		Payload: MessageStoreFile{
			Key:  msg.Key,
			Size: plaintextSize,
		},
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(&storeMsg); err != nil {
		return err
	}

	peer.Send([]byte{p2p.IncomingMessage})
	binary.Write(peer, binary.LittleEndian, int64(buf.Len()))
	if err := peer.Send(buf.Bytes()); err != nil {
		return err
	}

	time.Sleep(100 * time.Millisecond)

	peer.Send([]byte{p2p.IncomingStream})

	n, err := io.Copy(peer, fileReader)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] Written %d bytes (plaintext) over the network to %s\n", s.Transport.Address(), n, from)
	return nil
}

func (s *FileServer) handleMessageDeleteFile(from string, msg MessageDeleteFile) error {
	fmt.Printf("[%s] Received delete request for file with hash '%s' from %s\n", s.Transport.Address(), msg.Key, from)

	var originalKey string
	if s.DB != nil {
		files, err := s.DB.ListFiles(context.Background())
		if err == nil {
			for _, f := range files {
				if f.Hash == msg.Key {
					originalKey = f.Name
					break
				}
			}
		}
	}

	dbDeleteFailed := false
	if s.DB != nil {
		if err := s.DB.DeleteFile(context.Background(), msg.Key); err != nil {
			fmt.Printf("[%s] WARNING: Failed to delete file with hash '%s' from database: %v. Continuing with file deletion - DATABASE INCONSISTENCY DETECTED\n", s.Transport.Address(), msg.Key, err)
			dbDeleteFailed = true
		} else {
			fmt.Printf("[%s] Deleted file with hash '%s' from database\n", s.Transport.Address(), msg.Key)
		}
	}

	fileDeleted := false

	if originalKey != "" {
		if s.store.Has(originalKey) {
			if err := s.store.Delete(originalKey); err != nil {
				return fmt.Errorf("[%s] Error deleting file '%s': %v", s.Transport.Address(), originalKey, err)
			}
			fmt.Printf("[%s] Deleted file '%s' from local storage\n", s.Transport.Address(), originalKey)
			fileDeleted = true
		}
	}

	if !fileDeleted && s.store.Has(msg.Key) {
		if err := s.store.Delete(msg.Key); err != nil {
			return fmt.Errorf("[%s] Error deleting file with hash '%s': %v", s.Transport.Address(), msg.Key, err)
		}
		fmt.Printf("[%s] Deleted file with hash '%s' from local storage\n", s.Transport.Address(), msg.Key)
		fileDeleted = true
	}

	if !fileDeleted {
		fmt.Printf("[%s] File with hash '%s' does not exist locally, skipping deletion\n", s.Transport.Address(), msg.Key)
	}

	if dbDeleteFailed {
		fmt.Printf("[%s] WARNING: Database inconsistency - file deleted from disk but database cleanup failed for hash '%s'\n", s.Transport.Address(), msg.Key)
	}
	return nil
}

func (s *FileServer) stream(msg *Message) error {
	peers := []io.Writer{}
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}

	mw := io.MultiWriter(peers...)
	return gob.NewEncoder(mw).Encode(msg)
}

func (s *FileServer) broadcast(msg *Message) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	s.peersLock.Lock()
	defer s.peersLock.Unlock()

	for addr, peer := range s.peers {
		fmt.Printf("[%s] Sending message to peer %s\n", s.Transport.Address(), addr)
		peer.Send([]byte{p2p.IncomingMessage})
		binary.Write(peer, binary.LittleEndian, int64(buf.Len()))
		if err := peer.Send(buf.Bytes()); err != nil {
			fmt.Printf("[%s] Error sending message to peer %s: %v\n", s.Transport.Address(), addr, err)
			return err
		}
	}

	return nil
}

func (s *FileServer) Get(key string) (int64, io.Reader, error) {
	if s.store.Has(key) {
		fmt.Printf("[%s] File '%s' found locally! Serving file from disk...\n", s.Transport.Address(), key)
		return s.store.ReadDecrypt(s.EncryptionKey, key)
	}

	fmt.Printf("[%s] Did not find file '%s' locally, searching on network...\n", s.Transport.Address(), key)

	// Create channel to wait for download
	hash := hashKey(key)
	ch := make(chan struct{})
	s.downloadChannels[hash] = ch

	msg := Message{
		Payload: MessageGetFile{
			Key: hash,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		delete(s.downloadChannels, hash)
		return 0, nil, err
	}

	// Wait for download to complete or timeout
	select {
	case <-ch:
		fmt.Printf("[%s] File downloaded successfully!\n", s.Transport.Address())
		// The file was downloaded and stored using the hash
		return s.store.ReadDecrypt(s.EncryptionKey, hash)
	case <-time.After(10 * time.Second):
		delete(s.downloadChannels, hash)
		return 0, nil, fmt.Errorf("timeout waiting for file download")
	}
}

func (s *FileServer) Store(key string, r io.Reader) error {

	// 1. Write Encrypted to disk.
	n, err := s.store.WriteEncrypt(s.EncryptionKey, key, r)
	if err != nil {
		return err
	}

	plaintextSize := n - 16

	if s.DB != nil {
		_ = s.DB.InsertFileWithKey(context.Background(), dbpkg.File{
			ID:        hashKey(key),
			Name:      key,
			Hash:      hashKey(key),
			Size:      plaintextSize,
			LocalPath: s.store.FullPathForKey(key),
		}, "default")
	}

	s.peersLock.Lock()
	peers := []io.Writer{}
	peerAddrs := []string{}
	for addr, peer := range s.peers {
		peers = append(peers, peer)
		peerAddrs = append(peerAddrs, addr)
	}
	s.peersLock.Unlock()

	msg := Message{
		Payload: MessageStoreFile{
			Key:  hashKey(key),
			Size: plaintextSize,
		},
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(&msg); err != nil {
		return err
	}

	for i, peer := range peers {
		addr := peerAddrs[i]
		fmt.Printf("[%s] Sending message to peer %s\n", s.Transport.Address(), addr)
		if p, ok := peer.(p2p.Peer); ok {
			p.Send([]byte{p2p.IncomingMessage})
			binary.Write(p, binary.LittleEndian, int64(buf.Len()))
			if err := p.Send(buf.Bytes()); err != nil {
				fmt.Printf("[%s] Error sending message to peer %s: %v\n", s.Transport.Address(), addr, err)
			}
		}
	}

	_, fileReader, err := s.store.ReadDecrypt(s.EncryptionKey, key)
	if err != nil {
		return err
	}

	mw := io.MultiWriter(peers...)
	mw.Write([]byte{p2p.IncomingStream})

	written, err := io.Copy(mw, fileReader)
	if err != nil {
		return err
	}

	if s.DB != nil {
		fileID := hashKey(key)
		for _, addr := range peerAddrs {
			shareID := hashKey(fileID + addr + "outgoing")
			_ = s.DB.InsertShare(context.Background(), dbpkg.Share{
				ID:        shareID,
				FileID:    fileID,
				PeerID:    addr,
				Direction: "outgoing",
			})
		}
	}

	fmt.Printf("[%s] Received and written %d bytes to disk (encrypted), sent %d bytes (plaintext) to peers\n", s.Transport.Address(), n, written)

	return nil
}

func (s *FileServer) Delete(key string) error {
	fileID := hashKey(key)

	// Query peers that have this file BEFORE deleting from DB
	var sharePeers []string
	if s.DB != nil {
		peers, err := s.DB.GetOutgoingSharePeers(context.Background(), fileID)
		if err != nil {
			fmt.Printf("[%s] Warning: could not query share peers: %v\n", s.Transport.Address(), err)
		} else {
			sharePeers = peers
		}
	}

	// Delete from local database
	if s.DB != nil {
		if err := s.DB.DeleteFile(context.Background(), fileID); err != nil {
			return fmt.Errorf("[%s] Failed to delete file '%s' from database: %v. File not deleted from disk to maintain consistency", s.Transport.Address(), key, err)
		}
		fmt.Printf("[%s] Deleted file '%s' from database\n", s.Transport.Address(), key)
	}

	// Delete from local storage
	if !s.store.Has(key) {
		fmt.Printf("[%s] File '%s' does not exist locally\n", s.Transport.Address(), key)
	} else {
		if err := s.store.Delete(key); err != nil {
			return err
		}
		fmt.Printf("[%s] Deleted file '%s' from local storage\n", s.Transport.Address(), key)
	}

	// Connect to peers that have the file (from shares table)
	if len(sharePeers) > 0 {
		fmt.Printf("[%s] Connecting to %d peer(s) from shares: %v\n", s.Transport.Address(), len(sharePeers), sharePeers)
		for _, addr := range sharePeers {
			go func(addr string) {
				if err := s.Transport.Dial(addr); err != nil {
					fmt.Printf("[%s] Could not connect to share peer %s: %v\n", s.Transport.Address(), addr, err)
				}
			}(addr)
		}
		// Wait for connections to establish
		time.Sleep(500 * time.Millisecond)
	}

	// Now broadcast to all connected peers
	s.peersLock.Lock()
	peerCount := len(s.peers)
	peerAddrs := make([]string, 0, len(s.peers))
	for addr := range s.peers {
		peerAddrs = append(peerAddrs, addr)
	}
	s.peersLock.Unlock()

	fmt.Printf("[%s] Connected to %d peer(s): %v\n", s.Transport.Address(), peerCount, peerAddrs)

	if peerCount == 0 {
		fmt.Printf("[%s] No peers connected, cannot broadcast delete message\n", s.Transport.Address())
		return nil
	}

	msg := Message{
		Payload: MessageDeleteFile{
			Key: fileID,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return err
	}

	fmt.Printf("[%s] Broadcasted delete request for '%s' to %d peer(s)\n", s.Transport.Address(), key, peerCount)
	return nil
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func (s *FileServer) OnPeer(p p2p.Peer) error {
	peerAddr := p.RemoteAddr().String()

	s.peersLock.Lock()
	defer s.peersLock.Unlock()
	s.peers[peerAddr] = p

	fmt.Printf("[%s] Connected with remote %s\n", s.Transport.Address(), peerAddr)

	if s.DB != nil {
		now := time.Now()
		_ = s.DB.UpsertPeer(context.Background(), dbpkg.Peer{
			ID:       peerAddr,
			Address:  peerAddr,
			Status:   "connected",
			LastSeen: &now,
		})
	}

	go func() {
		time.Sleep(500 * time.Millisecond)

		for i := 0; i < 5; i++ {
			if err := s.sendPeerExchange(peerAddr); err != nil {
				fmt.Printf("[%s] Error sending peer exchange to %s: %v (attempt %d/5)\n", s.Transport.Address(), peerAddr, err, i+1)
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}
	}()

	return nil
}

type Handshake struct {
	ListenAddr string
}

func GetHandshakeFunc(listenAddr string) p2p.HandshakeFunc {
	return func(p any) error {
		peer, ok := p.(*p2p.TCPPeer)
		if !ok {
			return fmt.Errorf("invalid peer type for TCP handshake")
		}

		hs := Handshake{
			ListenAddr: listenAddr,
		}

		// 1. Send our handshake
		buf := new(bytes.Buffer)
		if err := gob.NewEncoder(buf).Encode(hs); err != nil {
			return err
		}

		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}

		// 2. Receive their handshake
		var remoteHS Handshake
		if err := gob.NewDecoder(peer).Decode(&remoteHS); err != nil {
			return err
		}

		fmt.Printf("[%s] Handshake successful with %s\n", listenAddr, remoteHS.ListenAddr)
		peer.FullAddr = remoteHS.ListenAddr

		return nil
	}
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}
		go func(addr string) {
			fmt.Printf("[%s] Attempting to connect with remote: %s\n", s.Transport.Address(), addr)

			err := s.Transport.Dial(addr)
			if err != nil {
				fmt.Printf("[%s] Dial error: %v\n", s.Transport.Address(), err)
			}
		}(addr)
	}
	return nil
}

// waitForPeers waits for at least one peer connection, with a timeout
func (s *FileServer) waitForPeers(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.peersLock.Lock()
		peerCount := len(s.peers)
		s.peersLock.Unlock()

		if peerCount > 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for peer connections")
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
	gob.Register(MessageDeleteFile{})
	gob.Register(MessagePeerExchange{})
	gob.Register(PeerInfo{})
	gob.Register(Handshake{})
}

type FileServerOpts struct {
	EncryptionKey     []byte
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         p2p.Transport
	BootstrapNodes    []string
	DB                *dbpkg.DB
}

type FileServer struct {
	FileServerOpts

	peersLock sync.Mutex
	peers     map[string]p2p.Peer

	store  *Store
	quitch chan struct{}

	pendingFileTransfers map[string]MessageStoreFile
	downloadChannels     map[string]chan struct{}
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}
	return &FileServer{
		FileServerOpts:       opts,
		store:                NewStore(storeOpts),
		quitch:               make(chan struct{}),
		peers:                make(map[string]p2p.Peer),
		pendingFileTransfers: make(map[string]MessageStoreFile),
		downloadChannels:     make(map[string]chan struct{}),
	}
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	Key  string
	Size int64
}

type MessageGetFile struct {
	Key string
}

type MessageDeleteFile struct {
	Key string
}

type MessagePeerExchange struct {
	Peers []PeerInfo
}

type PeerInfo struct {
	Address  string
	LastSeen time.Time
}
