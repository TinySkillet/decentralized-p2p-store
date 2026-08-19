package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"context"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

// downloadTimeout bounds how long Get waits for a peer to answer.
const downloadTimeout = 10 * time.Second

// offerTimeout bounds how long Get waits for peers to say whether they hold
// a key. It is short because an offer is a single small message.
const offerTimeout = 5 * time.Second

// peerRecency is how far back a peer counts as active when the per-host
// identity limit is applied.
const peerRecency = 30 * time.Minute

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
	case MessageFileOffer:
		return s.handleMessageFileOffer(from, v)
	case MessageFetchFile:
		return s.handleMessageFetchFile(from, v)
	case MessageDeleteFile:
		return s.handleMessageDeleteFile(from, v)
	case MessagePeerExchange:
		return s.handleMessagePeerExchange(from, v)
	}
	return nil
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	fmt.Printf("[%s] Received StoreFile message from %s for %s. Expecting stream...\n", s.Transport.Address(), from, short(msg.Digest))

	s.transferLock.Lock()
	s.pendingFileTransfers[from] = msg
	s.transferLock.Unlock()

	return nil
}

func (s *FileServer) handleStream(from string) (err error) {
	s.transferLock.Lock()
	msg, announced := s.pendingFileTransfers[from]
	delete(s.pendingFileTransfers, from)
	s.transferLock.Unlock()

	peer, found := s.peer(from)

	// The transport blocks this peer's read loop until CloseStream is called.
	// Returning early without it wedges the connection permanently, so it is
	// deferred rather than called on the success path only.
	if found {
		defer peer.CloseStream()
	}

	if !announced {
		return fmt.Errorf("peer %s sent a stream but announced no transfer", from)
	}
	if !found {
		return fmt.Errorf("peer (%s) could not be found in the peers list", from)
	}

	body := io.LimitReader(peer, msg.Size)

	// The contents are hashed as they are written and the file is only moved
	// into place if it matches what the sender announced, so data that fails
	// verification never becomes readable at all.
	size, err := s.store.WriteContentExpecting(s.EncryptionKey, msg.Digest, body)
	if err != nil {
		s.failRequest(msg.RequestID, err)
		return fmt.Errorf("receiving %s from %s: %w", msg.Digest, from, err)
	}
	if size != msg.Size {
		// A peer that dies mid-transfer ends the body early. The digest would
		// normally catch that; this reports the more specific cause.
		err := fmt.Errorf("truncated transfer of %s from %s: got %d bytes, announced %d", msg.Digest, from, size, msg.Size)
		s.failRequest(msg.RequestID, err)
		return err
	}

	fmt.Printf("[%s] Received %d bytes of %s from %s\n", s.Transport.Address(), size, short(msg.Digest), from)

	if s.DB != nil {
		if derr := s.recordFile(msg.Name, msg.Digest, size); derr != nil {
			log.Printf("[%s] Failed to record %s: %v", s.Transport.Address(), short(msg.Digest), derr)
		}

		shareID := contentKey([]byte(msg.Digest + from + "incoming"))
		if derr := s.DB.InsertShare(context.Background(), dbpkg.Share{
			ID:        shareID,
			FileID:    nameKey(msg.Name),
			PeerID:    from,
			Direction: "incoming",
		}); derr != nil {
			log.Printf("[%s] Failed to record incoming share for %s: %v", s.Transport.Address(), short(msg.Digest), derr)
		}
	}

	s.completeRequest(msg.RequestID)

	return nil
}

// recordFile maps a name to the contents now stored under it.
func (s *FileServer) recordFile(name, digest string, size int64) error {
	if name == "" {
		name = digest
	}
	return s.DB.InsertFileWithKey(context.Background(), dbpkg.File{
		ID:        nameKey(name),
		Name:      name,
		Hash:      digest,
		Size:      size,
		LocalPath: s.store.FullPathForKey(digest),
	}, "default")
}

// short abbreviates a digest for log output.
func short(digest string) string {
	if len(digest) > 12 {
		return digest[:12]
	}
	return digest
}

// fileRequest tracks one outstanding Get.
type fileRequest struct {
	key string

	// offers carries one entry per peer that answered. It is buffered to the
	// number of peers asked so a reply never blocks the server loop.
	offers chan peerOffer

	// done is closed when the file has been received, or closed with err set
	// when the chosen peer failed to deliver it.
	done chan struct{}
	once sync.Once
	err  error
}

// peerOffer is one peer's answer to a MessageGetFile.
type peerOffer struct {
	from  string
	offer MessageFileOffer
}

// newRequest registers an outstanding request and returns it with a function
// that deregisters it.
func (s *FileServer) newRequest(id, key string, expectedReplies int) (*fileRequest, func()) {
	req := &fileRequest{
		key:    key,
		offers: make(chan peerOffer, expectedReplies),
		done:   make(chan struct{}),
	}

	s.transferLock.Lock()
	s.requests[id] = req
	s.transferLock.Unlock()

	return req, func() {
		s.transferLock.Lock()
		delete(s.requests, id)
		s.transferLock.Unlock()
	}
}

func (s *FileServer) request(id string) (*fileRequest, bool) {
	s.transferLock.Lock()
	defer s.transferLock.Unlock()
	req, ok := s.requests[id]
	return req, ok
}

// completeRequest wakes the Get waiting on id, if any. sync.Once makes a
// repeated completion a no-op rather than a close of a closed channel.
func (s *FileServer) completeRequest(id string) {
	if id == "" {
		return
	}
	if req, ok := s.request(id); ok {
		req.once.Do(func() { close(req.done) })
	}
}

// failRequest wakes the Get waiting on id with an error, so a rejected
// transfer fails immediately instead of waiting out the timeout.
func (s *FileServer) failRequest(id string, err error) {
	if id == "" {
		return
	}
	if req, ok := s.request(id); ok {
		req.once.Do(func() {
			req.err = err
			close(req.done)
		})
	}
}

// handleMessageGetFile answers a peer asking whether this node holds a name,
// resolving it to the contents stored under it. Peers that hold nothing
// answer too, so the requester learns the answer is no without waiting for a
// timeout.
func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	fmt.Printf("[%s] Received availability query for '%s' from %s\n", s.Transport.Address(), msg.Name, from)

	peer, ok := s.peer(from)
	if !ok {
		return fmt.Errorf("peer %s not found in peer list", from)
	}

	offer := MessageFileOffer{RequestID: msg.RequestID, Name: msg.Name}

	local, err := s.resolve(msg.Name)
	if err != nil {
		log.Printf("[%s] Failed to look up '%s': %v", s.Transport.Address(), msg.Name, err)
	} else if local != nil {
		offer.Have = true
		offer.Digest = local.digest
		offer.Size = local.size
	}

	return sendMessage(peer, &Message{Payload: offer})
}

// handleMessageFileOffer records a peer's answer for the Get that is waiting.
func (s *FileServer) handleMessageFileOffer(from string, msg MessageFileOffer) error {
	req, ok := s.request(msg.RequestID)
	if !ok {
		// The request timed out or was already satisfied.
		return nil
	}

	select {
	case req.offers <- peerOffer{from: from, offer: msg}:
	default:
		// More replies than peers asked; drop rather than block the loop.
	}
	return nil
}

// localContent describes contents this node can serve.
type localContent struct {
	digest string
	size   int64
}

// resolve maps a name to the contents this node holds for it, or nil.
//
// The name is looked up in the database, which is what maps a name to the
// content-addressed hash. A name that is itself a digest is also accepted, so
// contents can be asked for directly.
func (s *FileServer) resolve(name string) (*localContent, error) {
	if s.DB != nil {
		f, err := s.DB.FindFileByName(context.Background(), name)
		if err != nil {
			return nil, err
		}
		if f != nil && s.store.Has(f.Hash) {
			return &localContent{digest: f.Hash, size: f.Size}, nil
		}
	}

	if isDigest(name) && s.store.Has(name) {
		size, r, err := s.store.Read(name)
		if err != nil {
			return nil, err
		}
		r.Close()
		return &localContent{digest: name, size: size - ivSize}, nil
	}

	return nil, nil
}

// handleMessageFetchFile streams contents to the one peer that asked for them.
func (s *FileServer) handleMessageFetchFile(from string, msg MessageFetchFile) error {
	fmt.Printf("[%s] Received request to serve %s\n", s.Transport.Address(), short(msg.Digest))

	if !s.store.Has(msg.Digest) {
		return fmt.Errorf("[%s] Do not have %s", s.Transport.Address(), short(msg.Digest))
	}

	plaintextSize, fileReader, err := s.store.ReadDecrypt(s.EncryptionKey, msg.Digest)
	if err != nil {
		return fmt.Errorf("[%s] Failed to read %s: %w", s.Transport.Address(), short(msg.Digest), err)
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
			RequestID: msg.RequestID,
			Name:      msg.Name,
			Digest:    msg.Digest,
			Size:      plaintextSize,
		},
	}

	// The announcement and the body go out under one lock, so a concurrent
	// transfer on this connection cannot be spliced between them.
	n, err := sendFile(peer, &storeMsg, fileReader)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] Written %d bytes (plaintext) over the network to %s\n", s.Transport.Address(), n, from)
	return nil
}

func (s *FileServer) handleMessageDeleteFile(from string, msg MessageDeleteFile) error {
	fmt.Printf("[%s] Received delete request for '%s' from %s\n", s.Transport.Address(), msg.Name, from)

	if err := s.forget(msg.Name); err != nil {
		return fmt.Errorf("[%s] %w", s.Transport.Address(), err)
	}
	return nil
}

// forget removes a name and, if nothing else refers to them, the contents
// behind it.
//
// Deduplication means one blob can back several names, so the bytes outlive
// any single name that pointed at them.
func (s *FileServer) forget(name string) error {
	if s.DB == nil {
		// Without a database there is no name mapping, so the name can only
		// be the contents themselves.
		if err := s.store.Delete(name); err != nil {
			return fmt.Errorf("deleting %q: %w", name, err)
		}
		return nil
	}

	hash, orphaned, err := s.DB.DeleteFileByName(context.Background(), name)
	if err != nil {
		// Stop before touching the disk. Removing the bytes while the
		// metadata still points at them is the inconsistency worth avoiding;
		// leaving both in place is recoverable.
		return fmt.Errorf("deleting %q from the database, leaving the file on disk: %w", name, err)
	}

	if hash == "" {
		fmt.Printf("[%s] Nothing stored under '%s', skipping deletion\n", s.Transport.Address(), name)
		return nil
	}

	fmt.Printf("[%s] Removed name '%s' from the database\n", s.Transport.Address(), name)

	if !orphaned {
		fmt.Printf("[%s] Contents %s are still referenced by another name, keeping them\n", s.Transport.Address(), short(hash))
		return nil
	}

	if err := s.store.Delete(hash); err != nil {
		return fmt.Errorf("deleting contents %s: %w", short(hash), err)
	}
	fmt.Printf("[%s] Deleted contents %s from local storage\n", s.Transport.Address(), short(hash))
	return nil
}

// encodeMessage renders msg as a single wire frame.
//
// Tag, length and payload are emitted together so that a concurrent send on
// the same connection cannot land between them and corrupt the frame.
func encodeMessage(msg *Message) ([]byte, error) {
	payload := new(bytes.Buffer)
	if err := gob.NewEncoder(payload).Encode(msg); err != nil {
		return nil, err
	}
	if payload.Len() > p2p.MaxMessageSize {
		return nil, fmt.Errorf("message of %d bytes exceeds the %d byte limit", payload.Len(), p2p.MaxMessageSize)
	}

	frame := new(bytes.Buffer)
	frame.WriteByte(p2p.IncomingMessage)
	if err := binary.Write(frame, binary.LittleEndian, int64(payload.Len())); err != nil {
		return nil, err
	}
	frame.Write(payload.Bytes())

	return frame.Bytes(), nil
}

// sendMessage writes msg to peer as a single frame.
func sendMessage(peer p2p.Peer, msg *Message) error {
	frame, err := encodeMessage(msg)
	if err != nil {
		return err
	}
	return peer.Send(frame)
}

// sendFile announces a file and streams its body as one indivisible transfer.
func sendFile(peer p2p.Peer, msg *Message, body io.Reader) (int64, error) {
	frame, err := encodeMessage(msg)
	if err != nil {
		return 0, err
	}
	return peer.SendStream(frame, body)
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

// hasPeerWithNodeID reports whether a peer with this identity is already
// connected, possibly on a different address.
func (s *FileServer) hasPeerWithNodeID(nodeID string) bool {
	if nodeID == "" {
		return false
	}

	s.peersLock.Lock()
	defer s.peersLock.Unlock()
	for _, p := range s.peers {
		if p.ID() == nodeID {
			return true
		}
	}
	return false
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

// Get returns a file, fetching it from a peer when it is not held locally.
//
// The exchange runs in two rounds. First every peer is asked whether it holds
// the name and answers either way, resolving the name to a content digest, so
// a name nobody has fails as soon as the last peer has spoken. Then exactly
// one peer that answered yes is asked for those contents, which is what stops
// several peers streaming the same file at once.
//
// Because the second round asks for a digest rather than a name, the reply is
// self-verifying: the bytes either hash to what was asked for or they are
// discarded. A peer cannot substitute different contents.
func (s *FileServer) Get(name string) (int64, io.Reader, error) {
	if local, err := s.resolve(name); err != nil {
		return 0, nil, err
	} else if local != nil {
		fmt.Printf("[%s] File '%s' found locally! Serving file from disk...\n", s.Transport.Address(), name)
		return s.store.ReadDecrypt(s.EncryptionKey, local.digest)
	}

	fmt.Printf("[%s] Did not find file '%s' locally, searching on network...\n", s.Transport.Address(), name)

	requestID, err := newRequestID()
	if err != nil {
		return 0, nil, err
	}

	peers, addrs := s.connectedPeers()
	if len(addrs) == 0 {
		return 0, nil, fmt.Errorf("no peers connected to ask for %q", name)
	}

	// Register before broadcasting, so a fast peer cannot answer before this
	// node is ready to notice.
	req, release := s.newRequest(requestID, name, len(addrs))
	defer release()

	query := Message{Payload: MessageGetFile{RequestID: requestID, Name: name}}
	asked := 0
	for _, addr := range addrs {
		if err := sendMessage(peers[addr], &query); err != nil {
			fmt.Printf("[%s] Error asking peer %s: %v\n", s.Transport.Address(), addr, err)
			continue
		}
		asked++
	}
	if asked == 0 {
		return 0, nil, fmt.Errorf("could not reach any peer to ask for %q", name)
	}

	holder, ok := s.awaitOffer(req, asked)
	if !ok {
		return 0, nil, fmt.Errorf("no peer has %q", name)
	}

	peer, ok := s.peer(holder.from)
	if !ok {
		return 0, nil, fmt.Errorf("peer %s went away before it could send %q", holder.from, name)
	}

	digest := holder.offer.Digest
	fmt.Printf("[%s] Fetching '%s' (%s) from %s\n", s.Transport.Address(), name, short(digest), holder.from)

	fetch := Message{Payload: MessageFetchFile{RequestID: requestID, Name: name, Digest: digest}}
	if err := sendMessage(peer, &fetch); err != nil {
		return 0, nil, err
	}

	select {
	case <-req.done:
		if req.err != nil {
			return 0, nil, fmt.Errorf("fetching %q from %s: %w", name, holder.from, req.err)
		}
		fmt.Printf("[%s] File downloaded successfully!\n", s.Transport.Address())
		return s.store.ReadDecrypt(s.EncryptionKey, digest)
	case <-time.After(downloadTimeout):
		return 0, nil, fmt.Errorf("timeout waiting for %q from %s", name, holder.from)
	case <-s.quitch:
		return 0, nil, fmt.Errorf("server stopped while waiting for %q", name)
	}
}

// awaitOffer collects replies until a peer says it holds the file, every peer
// asked has answered, or the offer window closes.
func (s *FileServer) awaitOffer(req *fileRequest, asked int) (peerOffer, bool) {
	deadline := time.After(offerTimeout)

	for replies := 0; replies < asked; replies++ {
		select {
		case reply := <-req.offers:
			if reply.offer.Have && reply.offer.Digest != "" {
				return reply, true
			}
		case <-deadline:
			return peerOffer{}, false
		case <-s.quitch:
			return peerOffer{}, false
		}
	}

	// Every peer answered and none of them holds it.
	return peerOffer{}, false
}

// Store writes a file and pushes it to every connected peer.
//
// The file is stored under the digest of its own contents, so storing the
// same bytes twice under different names keeps one copy on disk with two
// names pointing at it.
func (s *FileServer) Store(name string, r io.Reader) error {
	digest, size, err := s.store.WriteContent(s.EncryptionKey, r)
	if err != nil {
		return err
	}

	if s.DB != nil {
		if err := s.recordFile(name, digest, size); err != nil {
			// The bytes are on disk but unrecorded, so surface it rather than
			// reporting a store that later commands cannot see.
			return fmt.Errorf("recording file %q: %w", name, err)
		}
	}

	msg := Message{
		Payload: MessageStoreFile{
			Name:   name,
			Digest: digest,
			Size:   size,
		},
	}

	replicated, written := s.replicate(digest, &msg)

	if s.DB != nil {
		for _, addr := range replicated {
			shareID := contentKey([]byte(digest + addr + "outgoing"))
			if err := s.DB.InsertShare(context.Background(), dbpkg.Share{
				ID:        shareID,
				FileID:    nameKey(name),
				PeerID:    addr,
				Direction: "outgoing",
			}); err != nil {
				log.Printf("[%s] Failed to record outgoing share to %s: %v", s.Transport.Address(), addr, err)
			}
		}
	}

	fmt.Printf("[%s] Stored '%s' as %s (%d bytes), sent %d bytes to %d peer(s)\n",
		s.Transport.Address(), name, short(digest), size, written, len(replicated))

	return nil
}

// replicate pushes a stored file to every connected peer and reports which
// peers accepted it, along with the total bytes sent.
//
// Each peer is served by its own goroutine reading its own copy of the file.
// A single shared reader would have to hold every peer's connection lock at
// once to keep the transfer indivisible, and one slow peer would stall
// delivery to all the others.
func (s *FileServer) replicate(digest string, msg *Message) ([]string, int64) {
	peers, addrs := s.connectedPeers()

	type result struct {
		addr    string
		written int64
	}
	results := make(chan result, len(addrs))

	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Add(1)
		go func(addr string, peer p2p.Peer) {
			defer wg.Done()

			_, body, err := s.store.ReadDecrypt(s.EncryptionKey, digest)
			if err != nil {
				fmt.Printf("[%s] Could not read %s to send to %s: %v\n", s.Transport.Address(), short(digest), addr, err)
				return
			}
			if rc, ok := body.(io.Closer); ok {
				defer rc.Close()
			}

			fmt.Printf("[%s] Sending message to peer %s\n", s.Transport.Address(), addr)

			n, err := sendFile(peer, msg, body)
			if err != nil {
				fmt.Printf("[%s] Error sending %s to peer %s: %v\n", s.Transport.Address(), short(digest), addr, err)
				return
			}
			results <- result{addr: addr, written: n}
		}(addr, peers[addr])
	}

	wg.Wait()
	close(results)

	replicated := make([]string, 0, len(addrs))
	var written int64
	for r := range results {
		replicated = append(replicated, r.addr)
		written += r.written
	}
	return replicated, written
}

// Delete removes a name locally and asks every peer to do the same.
func (s *FileServer) Delete(name string) error {
	// Query the peers holding this file before the metadata that names them
	// is removed.
	var sharePeers []string
	if s.DB != nil {
		peers, err := s.DB.GetOutgoingSharePeers(context.Background(), nameKey(name))
		if err != nil {
			fmt.Printf("[%s] Warning: could not query share peers: %v\n", s.Transport.Address(), err)
		} else {
			sharePeers = peers
		}
	}

	if err := s.forget(name); err != nil {
		return fmt.Errorf("[%s] %w", s.Transport.Address(), err)
	}

	// Reconnect to peers that were sent a copy but are not currently
	// connected, so the deletion reaches them too.
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
	if peerCount == 0 {
		fmt.Printf("[%s] No peers connected, cannot broadcast delete message\n", s.Transport.Address())
		return nil
	}

	msg := Message{Payload: MessageDeleteFile{Name: name}}
	if err := s.broadcast(&msg); err != nil {
		return err
	}

	fmt.Printf("[%s] Broadcasted delete request for '%s' to %d peer(s)\n", s.Transport.Address(), name, peerCount)
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

	if err := s.admit(peerAddr, p.ID()); err != nil {
		return err
	}

	s.peersLock.Lock()
	s.peers[peerAddr] = p
	s.peersLock.Unlock()

	fmt.Printf("[%s] Connected with remote %s\n", s.Transport.Address(), peerAddr)

	if s.DB != nil {
		now := time.Now()
		if err := s.DB.UpsertPeer(context.Background(), dbpkg.Peer{
			ID:       peerAddr,
			Address:  peerAddr,
			NodeID:   p.ID(),
			Status:   "connected",
			LastSeen: &now,
		}); err != nil {
			log.Printf("[%s] Failed to record peer %s: %v", s.Transport.Address(), peerAddr, err)
		}
	}

	go s.announceTo(peerAddr)

	return nil
}

// admit decides whether a peer may join this node's peer set.
//
// One machine presenting many identities is the shape a Sybil attack takes,
// so the number of identities accepted from a single address is capped. The
// network is scoped to trusted groups reached through a known bootstrap node,
// and this limits the damage a single compromised host can do to the peer
// tables that get gossiped onward.
//
// Loopback is exempt: several nodes on one machine is the normal local
// testing arrangement, not an attack.
func (s *FileServer) admit(peerAddr, nodeID string) error {
	if s.DB == nil || nodeID == "" {
		return nil
	}

	host := dbpkg.HostOf(peerAddr)
	if dbpkg.IsLoopbackHost(host) {
		return nil
	}

	// An identity already known at this address is a reconnection, not a new
	// claim on the budget.
	known, err := s.DB.CountActivePeersForHost(context.Background(), host, peerRecency)
	if err != nil {
		log.Printf("[%s] Could not check peer count for %s: %v", s.Transport.Address(), host, err)
		return nil
	}
	if s.hasPeerWithNodeID(nodeID) || known < s.MaxPeersPerHost {
		return nil
	}

	return fmt.Errorf("refusing peer %s: host %s already has %d identities, limit is %d",
		nodeID, host, known, s.MaxPeersPerHost)
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
			NodeID:   p.ID(),
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

	for i := range attempts {
		if err := s.sendPeerExchange(peerAddr); err == nil {
			return
		} else {
			fmt.Printf("[%s] Error sending peer exchange to %s: %v (attempt %d/%d)\n",
				s.Transport.Address(), peerAddr, err, i+1, attempts)
		}

		select {
		case <-time.After(250 * time.Millisecond):
		case <-s.quitch:
			return
		}
	}
}

// Handshake is the first thing exchanged on a new connection.
type Handshake struct {
	Version uint32
	NodeID  string

	// ListenPort is the port this node accepts connections on. Only the port
	// is sent: the receiver already knows which address the connection came
	// from and pairs the two, so a node configured with a bare ":3000" is
	// still reachable by peers on other machines.
	ListenPort string
}

// portSource reports the port this node accepts connections on. It is read at
// handshake time rather than captured up front, so a node configured with
// port 0 advertises the port it actually bound.
type portSource interface {
	Address() string
	BoundAddr() string
}

// localListenPort returns the port peers should use to reach this node.
func localListenPort(src portSource) string {
	for _, candidate := range []string{src.BoundAddr(), src.Address()} {
		if candidate == "" {
			continue
		}
		if _, port, err := net.SplitHostPort(candidate); err == nil && port != "" && port != "0" {
			return port
		}
	}
	return ""
}

// GetHandshakeFunc builds the handshake run on every new connection.
//
// It settles three things before any file traffic is allowed: that both sides
// speak the same protocol version, that the peer is not this node reached by
// a roundabout route, and what address other nodes should use to reach it.
func GetHandshakeFunc(nodeID string, src portSource) p2p.HandshakeFunc {
	return func(p any) error {
		peer, ok := p.(*p2p.TCPPeer)
		if !ok {
			return fmt.Errorf("invalid peer type for TCP handshake")
		}

		hs := Handshake{
			Version:    p2p.ProtocolVersion,
			NodeID:     nodeID,
			ListenPort: localListenPort(src),
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

		if remoteHS.Version != p2p.ProtocolVersion {
			return fmt.Errorf("protocol version mismatch: peer speaks %d, this node speaks %d",
				remoteHS.Version, p2p.ProtocolVersion)
		}
		if remoteHS.NodeID == "" {
			return fmt.Errorf("peer did not identify itself")
		}
		if remoteHS.NodeID == nodeID {
			// Gossip hands out addresses, and one of them is eventually our
			// own. Identity is what tells the difference reliably; comparing
			// addresses cannot, because a node has several.
			return fmt.Errorf("refusing to connect to self")
		}
		if remoteHS.ListenPort == "" {
			return fmt.Errorf("peer %s did not advertise a listen port", remoteHS.NodeID)
		}

		// Pair the port the peer advertises with the address the connection
		// actually came from. A peer configured with a bare ":3000" would
		// otherwise hand out an address that resolves to the wrong host.
		host := peer.ObservedHost()
		if host == "" {
			return fmt.Errorf("could not determine the address peer %s connected from", remoteHS.NodeID)
		}

		peer.NodeID = remoteHS.NodeID
		peer.FullAddr = net.JoinHostPort(host, remoteHS.ListenPort)

		fmt.Printf("[%s] Handshake successful with %s at %s\n", src.Address(), remoteHS.NodeID, peer.FullAddr)

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

// waitForPeerDiscovery waits until the peer set stops growing, and returns
// the number of peers connected.
//
// Gossip reaches further than the bootstrap list names, so a command that
// acted the moment the first peer connected would replicate to a fraction of
// the network it could have reached. Waiting for the count to hold steady
// adapts to how long discovery actually takes, where a fixed sleep is either
// too short on a slow network or wasted time on a fast one.
func (s *FileServer) waitForPeerDiscovery(quiet, max time.Duration) int {
	const poll = 25 * time.Millisecond

	deadline := time.Now().Add(max)
	last := s.peerCount()
	stableSince := time.Now()

	for time.Now().Before(deadline) {
		select {
		case <-time.After(poll):
		case <-s.quitch:
			return s.peerCount()
		}

		count := s.peerCount()
		if count != last {
			last = count
			stableSince = time.Now()
			continue
		}
		if count > 0 && time.Since(stableSince) >= quiet {
			return count
		}
	}

	return s.peerCount()
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
	gob.Register(MessageFileOffer{})
	gob.Register(MessageFetchFile{})
	gob.Register(MessageDeleteFile{})
	gob.Register(MessagePeerExchange{})
	gob.Register(PeerInfo{})
	gob.Register(Handshake{})
}

type FileServerOpts struct {
	NodeID string

	// MaxPeersPerHost caps how many identities may be accepted from one
	// address. Zero means the package default.
	MaxPeersPerHost int

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

	// transferLock guards the transfer maps below. They are written by the
	// caller's goroutine (Get) and by the server loop, so they cannot be
	// touched without it.
	transferLock sync.Mutex

	// pendingFileTransfers holds the announcement a peer sent just before the
	// stream that follows it, keyed by peer address.
	pendingFileTransfers map[string]MessageStoreFile

	// requests holds outstanding Gets, keyed by request id.
	requests map[string]*fileRequest
}

func NewFileServer(opts FileServerOpts) *FileServer {
	if opts.MaxPeersPerHost <= 0 {
		opts.MaxPeersPerHost = dbpkg.DefaultMaxPeersPerHost
	}
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
		requests:             make(map[string]*fileRequest),
	}
}

type Message struct {
	Payload any
}

// MessageStoreFile announces that a file stream follows immediately on the
// same connection. RequestID is empty when the file is pushed unsolicited by
// a Store, and echoes the requester's id when it answers a fetch.
//
// Digest identifies the contents; Name travels with it so the receiving node
// can list what it holds under the name its owner gave it.
type MessageStoreFile struct {
	RequestID string
	Name      string
	Digest    string
	Size      int64
}

// MessageGetFile asks every peer whether it holds anything under Name.
type MessageGetFile struct {
	RequestID string
	Name      string
}

// MessageFileOffer answers a MessageGetFile, resolving the name to the
// contents the answering peer holds for it. Peers that hold nothing answer
// too, with Have false, so the requester can give up as soon as every peer
// has spoken instead of waiting out the timeout.
type MessageFileOffer struct {
	RequestID string
	Name      string
	Have      bool
	Digest    string
	Size      int64
}

// MessageFetchFile asks the single peer chosen from the offers to send the
// contents. It names a digest rather than a file name: the requester knows
// exactly which bytes it expects, and can reject anything else.
type MessageFetchFile struct {
	RequestID string
	Name      string
	Digest    string
}

// MessageDeleteFile asks peers to forget a name. The contents behind it are
// removed only once no name on that peer still refers to them.
type MessageDeleteFile struct {
	Name string
}

type MessagePeerExchange struct {
	Peers []PeerInfo
}

type PeerInfo struct {
	Address  string
	NodeID   string
	LastSeen time.Time
}
