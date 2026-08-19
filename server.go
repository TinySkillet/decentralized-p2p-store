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

// downloadTimeout bounds how long Get waits for a peer to answer.
const downloadTimeout = 10 * time.Second

// duplicateResponseGrace is how long a delivered key stays marked as
// satisfied. Every peer holding a file answers the same Get, and the losing
// responses can arrive after Get has already returned its reader, so the
// marker has to outlive the request that created it.
const duplicateResponseGrace = time.Minute

// Listen binds the transport and starts connecting to bootstrap nodes. It
// returns as soon as the socket is bound, so callers can report a bind
// failure directly instead of discovering it later on a background goroutine.
func (s *FileServer) Listen() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	if len(s.BootstrapNodes) != 0 {
		s.bootstrapNetwork()
	}

	return nil
}

// Serve processes incoming messages until Stop is called.
func (s *FileServer) Serve() {
	s.loop()
}

// Start binds the transport and then serves, blocking until Stop is called.
func (s *FileServer) Start() error {
	if err := s.Listen(); err != nil {
		return err
	}

	s.Serve()

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
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				// A payload we cannot decode must not be handled: msg is
				// zero here, and dispatching on it would act on empty keys.
				log.Printf("[%s] Decoding error from %s: %v", s.Transport.Address(), rpc.From, err)
				continue
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

	s.transferLock.Lock()
	s.pendingFileTransfers[from] = msg
	s.transferLock.Unlock()

	return nil
}

func (s *FileServer) handleStream(from string) (err error) {
	s.transferLock.Lock()
	msg, pending := s.pendingFileTransfers[from]
	delete(s.pendingFileTransfers, from)
	s.transferLock.Unlock()

	peer, found := s.peer(from)

	// The transport blocks this peer's read loop until CloseStream is called.
	// Returning early without it wedges the connection permanently, so it is
	// deferred rather than called on the success path only.
	if found {
		defer peer.CloseStream()
	}

	if !pending {
		return fmt.Errorf("peer %s sent a stream but no pending transfer was found", from)
	}
	if !found {
		return fmt.Errorf("peer (%s) could not be found in the peers list", from)
	}

	body := io.LimitReader(peer, msg.Size)

	// Several peers may answer the same Get. The first response satisfies it;
	// the rest are drained off the connection and discarded, so a slower peer
	// cannot overwrite the copy already accepted.
	if s.claimDownload(msg.Key) == downloadAlreadySatisfied {
		if _, derr := io.Copy(io.Discard, body); derr != nil {
			return derr
		}
		fmt.Printf("[%s] Discarded duplicate copy of %s from %s\n", s.Transport.Address(), msg.Key, from)
		return nil
	}

	// Receive plaintext, write encrypted
	n, err := s.store.WriteEncrypt(s.EncryptionKey, msg.Key, body)
	if err != nil {
		return err
	}

	// A peer that dies mid-transfer ends the body early, which reads as a
	// clean EOF. Without this check the short file would be stored and served
	// on as though it were the whole thing.
	if got := n - ivSize; got != msg.Size {
		if derr := s.store.Delete(msg.Key); derr != nil {
			log.Printf("[%s] Failed to remove truncated file %s: %v", s.Transport.Address(), msg.Key, derr)
		}
		return fmt.Errorf("truncated transfer of %s from %s: got %d bytes, announced %d", msg.Key, from, got, msg.Size)
	}

	fmt.Printf("[%s] Written %d bytes to disk (encrypted) from %s\n", s.Transport.Address(), n, from)

	// Record share in database if configured
	if s.DB != nil {
		shareID := hashKey(msg.Key + from + "incoming")
		if derr := s.DB.InsertShare(context.Background(), dbpkg.Share{
			ID:        shareID,
			FileID:    msg.Key,
			PeerID:    from,
			Direction: "incoming",
		}); derr != nil {
			log.Printf("[%s] Failed to record incoming share for %s: %v", s.Transport.Address(), msg.Key, derr)
		}
	}

	s.completeDownload(msg.Key)

	return nil
}

type downloadClaim int

const (
	downloadNotRequested downloadClaim = iota
	downloadClaimed
	downloadAlreadySatisfied
)

// claimDownload reports whether this node should accept an incoming copy of
// key. A key nobody asked for is an unsolicited push and is always accepted.
func (s *FileServer) claimDownload(key string) downloadClaim {
	s.transferLock.Lock()
	defer s.transferLock.Unlock()

	s.pruneSatisfiedLocked()

	if _, waiting := s.downloadChannels[key]; waiting {
		return downloadClaimed
	}
	if _, satisfied := s.satisfiedDownloads[key]; satisfied {
		return downloadAlreadySatisfied
	}
	return downloadNotRequested
}

// completeDownload wakes the Get waiting on key, if any. Deleting the channel
// under the same lock that closes it makes a second responder a no-op rather
// than a close of a closed channel.
func (s *FileServer) completeDownload(key string) {
	s.transferLock.Lock()
	defer s.transferLock.Unlock()

	if ch, ok := s.downloadChannels[key]; ok {
		delete(s.downloadChannels, key)
		s.satisfiedDownloads[key] = time.Now()
		close(ch)
	}
}

// awaitDownload registers interest in key and returns a channel closed when a
// peer delivers it, plus a function to release the registration.
func (s *FileServer) awaitDownload(key string) (<-chan struct{}, func()) {
	ch := make(chan struct{})

	s.transferLock.Lock()
	s.downloadChannels[key] = ch
	s.transferLock.Unlock()

	return ch, func() {
		s.transferLock.Lock()
		defer s.transferLock.Unlock()
		delete(s.downloadChannels, key)
		// satisfiedDownloads is deliberately left in place: responses still
		// in flight must be discarded, not written over the copy the caller
		// is at this moment reading.
		s.pruneSatisfiedLocked()
	}
}

// pruneSatisfiedLocked drops expired duplicate-response markers. Callers must
// hold transferLock.
func (s *FileServer) pruneSatisfiedLocked() {
	for key, at := range s.satisfiedDownloads {
		if time.Since(at) > duplicateResponseGrace {
			delete(s.satisfiedDownloads, key)
		}
	}
}

func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	fmt.Printf("[%s] Received request to serve file '%s'\n", s.Transport.Address(), msg.Key)

	keyToRead := msg.Key

	// Files stored locally are keyed by their original name, while the
	// request carries the hash, so the name is looked up by index rather than
	// by scanning every file on every request.
	if s.DB != nil {
		f, err := s.DB.FindFileByHash(context.Background(), msg.Key)
		if err != nil {
			log.Printf("[%s] Failed to look up hash '%s': %v", s.Transport.Address(), msg.Key, err)
		} else if f != nil {
			fmt.Printf("[%s] Found original key '%s' for hash '%s'\n", s.Transport.Address(), f.Name, msg.Key)
			keyToRead = f.Name
		}
	}

	if !s.store.Has(keyToRead) {
		return fmt.Errorf("[%s] Do not have file %s", s.Transport.Address(), keyToRead)
	}

	plaintextSize, fileReader, err := s.store.ReadDecrypt(s.EncryptionKey, keyToRead)
	if err != nil {
		return fmt.Errorf("[%s] Failed to read file %s: %w", s.Transport.Address(), keyToRead, err)
	}
	if rc, ok := fileReader.(io.Closer); ok {
		defer rc.Close()
	}

	peer, ok := s.peer(from)
	if !ok {
		return fmt.Errorf("peer %s not found in peer list", from)
	}

	storeMsg := Message{
		Payload: MessageStoreFile{
			Key:  msg.Key,
			Size: plaintextSize,
		},
	}

	if err := sendMessage(peer, &storeMsg); err != nil {
		return err
	}

	// No sleep is needed between the header and the body: both travel over
	// the same connection and the receiver decodes frames in order.
	if err := peer.Send([]byte{p2p.IncomingStream}); err != nil {
		return err
	}

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
		f, err := s.DB.FindFileByHash(context.Background(), msg.Key)
		if err != nil {
			log.Printf("[%s] Failed to look up hash '%s': %v", s.Transport.Address(), msg.Key, err)
		} else if f != nil {
			originalKey = f.Name
		}
	}

	if s.DB != nil {
		if err := s.DB.DeleteFile(context.Background(), msg.Key); err != nil {
			// Stop before touching the disk. Removing the bytes while the
			// metadata still points at them is the inconsistency worth
			// avoiding; leaving both in place is recoverable.
			return fmt.Errorf("[%s] Failed to delete hash '%s' from database, leaving the file on disk: %w", s.Transport.Address(), msg.Key, err)
		}
		fmt.Printf("[%s] Deleted file with hash '%s' from database\n", s.Transport.Address(), msg.Key)
	}

	// A replica is stored under the hash, an original under its name, so both
	// are attempted. Delete is idempotent, so a miss is not an error.
	for _, key := range []string{originalKey, msg.Key} {
		if key == "" || !s.store.Has(key) {
			continue
		}
		if err := s.store.Delete(key); err != nil {
			return fmt.Errorf("[%s] Error deleting file '%s': %w", s.Transport.Address(), key, err)
		}
		fmt.Printf("[%s] Deleted file '%s' from local storage\n", s.Transport.Address(), key)
		return nil
	}

	fmt.Printf("[%s] File with hash '%s' does not exist locally, skipping deletion\n", s.Transport.Address(), msg.Key)
	return nil
}

// sendMessage writes msg to peer as a single frame.
//
// Tag, length and payload go out in one write so that a concurrent send on
// the same connection cannot land between them and corrupt the frame.
func sendMessage(peer p2p.Peer, msg *Message) error {
	payload := new(bytes.Buffer)
	if err := gob.NewEncoder(payload).Encode(msg); err != nil {
		return err
	}
	if payload.Len() > p2p.MaxMessageSize {
		return fmt.Errorf("message of %d bytes exceeds the %d byte limit", payload.Len(), p2p.MaxMessageSize)
	}

	frame := new(bytes.Buffer)
	frame.WriteByte(p2p.IncomingMessage)
	if err := binary.Write(frame, binary.LittleEndian, int64(payload.Len())); err != nil {
		return err
	}
	frame.Write(payload.Bytes())

	return peer.Send(frame.Bytes())
}

// connectedPeers returns a snapshot of the current peers.
//
// Callers must not hold peersLock while writing to the network: a peer that
// stops reading would otherwise block every other operation on the node.
func (s *FileServer) connectedPeers() (map[string]p2p.Peer, []string) {
	s.peersLock.Lock()
	defer s.peersLock.Unlock()

	peers := make(map[string]p2p.Peer, len(s.peers))
	addrs := make([]string, 0, len(s.peers))
	for addr, peer := range s.peers {
		peers[addr] = peer
		addrs = append(addrs, addr)
	}
	return peers, addrs
}

func (s *FileServer) peer(addr string) (p2p.Peer, bool) {
	s.peersLock.Lock()
	defer s.peersLock.Unlock()
	p, ok := s.peers[addr]
	return p, ok
}

func (s *FileServer) peerCount() int {
	s.peersLock.Lock()
	defer s.peersLock.Unlock()
	return len(s.peers)
}

// broadcast sends msg to every connected peer. A failure to reach one peer is
// logged and does not prevent delivery to the others.
func (s *FileServer) broadcast(msg *Message) error {
	peers, addrs := s.connectedPeers()

	var lastErr error
	for _, addr := range addrs {
		fmt.Printf("[%s] Sending message to peer %s\n", s.Transport.Address(), addr)
		if err := sendMessage(peers[addr], msg); err != nil {
			fmt.Printf("[%s] Error sending message to peer %s: %v\n", s.Transport.Address(), addr, err)
			lastErr = err
		}
	}

	return lastErr
}

func (s *FileServer) Get(key string) (int64, io.Reader, error) {
	if s.store.Has(key) {
		fmt.Printf("[%s] File '%s' found locally! Serving file from disk...\n", s.Transport.Address(), key)
		return s.store.ReadDecrypt(s.EncryptionKey, key)
	}

	fmt.Printf("[%s] Did not find file '%s' locally, searching on network...\n", s.Transport.Address(), key)

	hash := hashKey(key)

	// Register before broadcasting, so a fast peer cannot answer before this
	// node is ready to notice.
	ch, release := s.awaitDownload(hash)
	defer release()

	msg := Message{
		Payload: MessageGetFile{
			Key: hash,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return 0, nil, err
	}

	select {
	case <-ch:
		fmt.Printf("[%s] File downloaded successfully!\n", s.Transport.Address())
		// The file was downloaded and stored using the hash
		return s.store.ReadDecrypt(s.EncryptionKey, hash)
	case <-time.After(downloadTimeout):
		return 0, nil, fmt.Errorf("timeout waiting for file download of %q", key)
	case <-s.quitch:
		return 0, nil, fmt.Errorf("server stopped while waiting for %q", key)
	}
}

func (s *FileServer) Store(key string, r io.Reader) error {

	// 1. Write Encrypted to disk.
	n, err := s.store.WriteEncrypt(s.EncryptionKey, key, r)
	if err != nil {
		return err
	}

	plaintextSize := n - ivSize

	if s.DB != nil {
		if err := s.DB.InsertFileWithKey(context.Background(), dbpkg.File{
			ID:        hashKey(key),
			Name:      key,
			Hash:      hashKey(key),
			Size:      plaintextSize,
			LocalPath: s.store.FullPathForKey(key),
		}, "default"); err != nil {
			// The bytes are on disk but unrecorded, so surface it rather than
			// reporting a store that later commands cannot see.
			return fmt.Errorf("recording file %q: %w", key, err)
		}
	}

	peers, addrs := s.connectedPeers()

	msg := Message{
		Payload: MessageStoreFile{
			Key:  hashKey(key),
			Size: plaintextSize,
		},
	}

	// 2. Announce the file, then stream it to the peers that accepted the
	// announcement. A peer that fails the announcement is skipped, so its
	// connection never receives a body it is not expecting.
	replicated := make([]string, 0, len(addrs))
	writers := make([]io.Writer, 0, len(addrs))
	for _, addr := range addrs {
		fmt.Printf("[%s] Sending message to peer %s\n", s.Transport.Address(), addr)
		if err := sendMessage(peers[addr], &msg); err != nil {
			fmt.Printf("[%s] Error sending message to peer %s: %v\n", s.Transport.Address(), addr, err)
			continue
		}
		replicated = append(replicated, addr)
		writers = append(writers, peers[addr])
	}

	_, fileReader, err := s.store.ReadDecrypt(s.EncryptionKey, key)
	if err != nil {
		return err
	}
	if rc, ok := fileReader.(io.Closer); ok {
		defer rc.Close()
	}

	var written int64
	if len(writers) > 0 {
		mw := io.MultiWriter(writers...)
		if _, err := mw.Write([]byte{p2p.IncomingStream}); err != nil {
			return err
		}

		written, err = io.Copy(mw, fileReader)
		if err != nil {
			return err
		}
	}

	if s.DB != nil {
		fileID := hashKey(key)
		for _, addr := range replicated {
			shareID := hashKey(fileID + addr + "outgoing")
			if err := s.DB.InsertShare(context.Background(), dbpkg.Share{
				ID:        shareID,
				FileID:    fileID,
				PeerID:    addr,
				Direction: "outgoing",
			}); err != nil {
				log.Printf("[%s] Failed to record outgoing share to %s: %v", s.Transport.Address(), addr, err)
			}
		}
	}

	fmt.Printf("[%s] Received and written %d bytes to disk (encrypted), sent %d bytes (plaintext) to %d peer(s)\n",
		s.Transport.Address(), n, written, len(replicated))

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
			return fmt.Errorf("[%s] Failed to delete file '%s' from database: %w. File not deleted from disk to maintain consistency", s.Transport.Address(), key, err)
		}
		fmt.Printf("[%s] Deleted file '%s' from database\n", s.Transport.Address(), key)
	}

	// Delete from local storage. Delete is idempotent, so an absent file is
	// not an error.
	if err := s.store.Delete(key); err != nil {
		return err
	}
	fmt.Printf("[%s] Deleted file '%s' from local storage\n", s.Transport.Address(), key)

	// Connect to peers that have the file (from shares table)
	if len(sharePeers) > 0 {
		fmt.Printf("[%s] Connecting to %d peer(s) from shares: %v\n", s.Transport.Address(), len(sharePeers), sharePeers)

		var wg sync.WaitGroup
		for _, addr := range sharePeers {
			if _, already := s.peer(addr); already {
				continue
			}
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				if err := s.Transport.Dial(addr); err != nil {
					fmt.Printf("[%s] Could not connect to share peer %s: %v\n", s.Transport.Address(), addr, err)
				}
			}(addr)
		}
		wg.Wait()

		// Dial returns once the socket is up, but the handshake that
		// registers the peer completes asynchronously.
		s.waitForPeers(2 * time.Second)
	}

	peerCount := s.peerCount()
	fmt.Printf("[%s] Connected to %d peer(s)\n", s.Transport.Address(), peerCount)

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

// Stop shuts the server down. It is safe to call more than once.
func (s *FileServer) Stop() {
	s.stopOnce.Do(func() {
		close(s.quitch)
		if err := s.Transport.Close(); err != nil {
			log.Printf("[%s] Error closing transport: %v", s.Transport.Address(), err)
		}

		// Closing the listener only stops new connections. Established ones
		// must be closed too, otherwise peers keep this node in their peer
		// set and go on broadcasting to a process that has exited.
		peers, _ := s.connectedPeers()
		for _, p := range peers {
			p.Close()
		}
	})
}

func (s *FileServer) OnPeer(p p2p.Peer) error {
	peerAddr := p.RemoteAddr().String()

	s.peersLock.Lock()
	s.peers[peerAddr] = p
	s.peersLock.Unlock()

	fmt.Printf("[%s] Connected with remote %s\n", s.Transport.Address(), peerAddr)

	if s.DB != nil {
		now := time.Now()
		if err := s.DB.UpsertPeer(context.Background(), dbpkg.Peer{
			ID:       peerAddr,
			Address:  peerAddr,
			Status:   "connected",
			LastSeen: &now,
		}); err != nil {
			log.Printf("[%s] Failed to record peer %s: %v", s.Transport.Address(), peerAddr, err)
		}
	}

	go s.announceTo(peerAddr)

	return nil
}

// OnPeerDisconnect drops a peer whose connection has ended. Without it the
// node keeps broadcasting to a closed socket and reports stale peer counts.
func (s *FileServer) OnPeerDisconnect(p p2p.Peer) {
	peerAddr := p.RemoteAddr().String()

	s.peersLock.Lock()
	delete(s.peers, peerAddr)
	s.peersLock.Unlock()

	fmt.Printf("[%s] Disconnected from %s\n", s.Transport.Address(), peerAddr)

	if s.DB != nil {
		now := time.Now()
		if err := s.DB.UpsertPeer(context.Background(), dbpkg.Peer{
			ID:       peerAddr,
			Address:  peerAddr,
			Status:   "disconnected",
			LastSeen: &now,
		}); err != nil {
			log.Printf("[%s] Failed to record peer %s: %v", s.Transport.Address(), peerAddr, err)
		}
	}
}

// announceTo sends this node's peer list to a newly connected peer, retrying
// briefly while the connection settles.
func (s *FileServer) announceTo(peerAddr string) {
	const attempts = 5

	select {
	case <-time.After(500 * time.Millisecond):
	case <-s.quitch:
		return
	}

	for i := range attempts {
		if err := s.sendPeerExchange(peerAddr); err == nil {
			return
		} else {
			fmt.Printf("[%s] Error sending peer exchange to %s: %v (attempt %d/%d)\n",
				s.Transport.Address(), peerAddr, err, i+1, attempts)
		}

		select {
		case <-time.After(1 * time.Second):
		case <-s.quitch:
			return
		}
	}
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
		if remoteHS.ListenAddr == "" {
			return fmt.Errorf("peer advertised an empty listen address")
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
		if s.peerCount() > 0 {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
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

	store    *Store
	quitch   chan struct{}
	stopOnce sync.Once

	// transferLock guards all three transfer maps below. They are written by
	// the caller's goroutine (Get) and by the server loop, so they cannot be
	// touched without it.
	transferLock         sync.Mutex
	pendingFileTransfers map[string]MessageStoreFile
	downloadChannels     map[string]chan struct{}
	satisfiedDownloads   map[string]time.Time
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
		satisfiedDownloads:   make(map[string]time.Time),
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
