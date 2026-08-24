// Moving files between nodes: the fetch exchange and the transfers it drives.
package node

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"

	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// downloadTimeout bounds how long Get waits for a peer to answer.
const downloadTimeout = 10 * time.Second

// offerTimeout bounds how long Get waits for peers to say whether they hold
// a key. It is short because an offer is a single small message.
const offerTimeout = 5 * time.Second

// peerRecency is how far back a peer counts as active. It bounds both the
// per-host identity limit and the peer list this node gossips onward, which
// are the same question asked twice.
const peerRecency = 30 * time.Minute

// Listen binds the transport and starts connecting to bootstrap nodes. It
// returns as soon as the socket is bound, so callers can report a bind
// failure directly instead of discovering it later on a background goroutine.
func (s *FileServer) Listen() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	if s.OwnsDatabase && s.DB != nil {
		// Record who owns this storage, so commands opening the same database
		// can tell that their copy of a file is really this node's copy.
		if err := s.DB.PutSetting(context.Background(), dbpkg.ServingAddressSetting, s.Transport.Address()); err != nil {
			log.Printf("[%s] Could not record the serving address: %v", s.Transport.Address(), err)
		}
	}

	if len(s.BootstrapNodes) != 0 {
		s.bootstrapNetwork()
	}

	return nil
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	fmt.Printf("[%s] Received StoreFile message from %s for %s. Expecting stream...\n", s.Transport.Address(), from, storage.Short(msg.Digest))

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

	// An unsolicited push from a peer this node has not approved is refused.
	// A transfer with a request id is one this node asked for, and is allowed
	// whoever answers: the request named a digest and the contents are
	// verified against it, so an untrusted peer cannot substitute anything.
	// What it must not do is put a file here that was never asked for.
	if msg.RequestID == "" && s.TrustEnforced() && !s.Trusts(from) {
		// The body is already in flight, so it has to be read off the
		// connection even though it is discarded. Skipping this, or skipping
		// the deferred CloseStream, wedges the connection permanently.
		if _, cerr := io.Copy(io.Discard, body); cerr != nil {
			return cerr
		}
		fmt.Printf("[%s] Refused a push of '%s' (%s) from untrusted peer %s\n",
			s.Transport.Address(), msg.Name, storage.Short(msg.Digest), storage.Short(from))

		s.publish(Event{
			Kind: EventPushRefused, Name: msg.Name, Digest: msg.Digest,
			Size: msg.Size, Node: from,
			Err: "the sending peer is not trusted",
		})
		return nil
	}

	// A peer that missed a deletion still holds the file and its repair cycle
	// will offer it back. The tombstone is what stops the deletion being
	// undone; the sender is told so it stops holding the file too.
	if s.DB != nil {
		deleted, derr := s.DB.IsDeleted(context.Background(), msg.Name, msg.Digest)
		if derr != nil {
			log.Printf("[%s] Could not check deletions for %s: %v", s.Transport.Address(), storage.Short(msg.Digest), derr)
		} else if deleted {
			// The body is already on its way, so it has to be read off the
			// connection even though it is being discarded.
			if _, cerr := io.Copy(io.Discard, body); cerr != nil {
				return cerr
			}
			fmt.Printf("[%s] Refused '%s' (%s) from %s: it was deleted here\n",
				s.Transport.Address(), msg.Name, storage.Short(msg.Digest), from)

			s.notifyDeleted(from, msg.Name, msg.Digest)
			s.failRequest(msg.RequestID, fmt.Errorf("%q was deleted", msg.Name))
			return nil
		}
	}

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

	fmt.Printf("[%s] Received %d bytes of %s from %s\n", s.Transport.Address(), size, storage.Short(msg.Digest), from)

	if s.DB != nil {
		if derr := s.recordReplica(msg.Name, msg.Digest, size, msg.Owner); derr != nil {
			log.Printf("[%s] Failed to record %s: %v", s.Transport.Address(), storage.Short(msg.Digest), derr)
		}

		shareID := storage.ContentKey([]byte(msg.Digest + from + "incoming"))
		if derr := s.DB.InsertShare(context.Background(), dbpkg.Share{
			ID:        shareID,
			FileID:    nameKey(msg.Name),
			PeerID:    from,
			Direction: "incoming",
		}); derr != nil {
			log.Printf("[%s] Failed to record incoming share for %s: %v", s.Transport.Address(), storage.Short(msg.Digest), derr)
		}
	}

	// A transfer we asked for and one pushed at us are different events: the
	// second is the one the trust rules care about, and the UI reports them
	// differently.
	kind := EventFileReceived
	if msg.RequestID != "" {
		kind = EventFileFetched
	}
	s.publish(Event{Kind: kind, Name: msg.Name, Digest: msg.Digest, Size: size, Node: from})

	s.completeRequest(msg.RequestID)

	return nil
}

// recordFile maps a name to the contents now stored under it, owned by owner.
func (s *FileServer) recordFile(name, digest string, size int64, owner string) error {
	if name == "" {
		name = digest
	}
	return s.DB.InsertFileWithKey(context.Background(), dbpkg.File{
		ID:        nameKey(name),
		Name:      name,
		Hash:      digest,
		Size:      size,
		LocalPath: s.store.FullPathForKey(digest),
		Owner:     owner,
	}, "default")
}

// recordReplica records a copy received from a peer.
//
// A replica is held on behalf of the network and must not take over a name
// this node already uses for something else: otherwise any peer could store a
// file called "notes" and silently repoint this node's own "notes" at it.
func (s *FileServer) recordReplica(name, digest string, size int64, owner string) error {
	if name == "" {
		name = digest
	}

	stored, err := s.DB.InsertReplica(context.Background(), dbpkg.File{
		ID:        nameKey(name),
		Name:      name,
		Hash:      digest,
		Size:      size,
		LocalPath: s.store.FullPathForKey(digest),
		Owner:     owner,
	}, "default")
	if err != nil {
		return err
	}

	if stored != name {
		fmt.Printf("[%s] '%s' already refers to other contents here, filing the copy under %s instead\n",
			s.Transport.Address(), name, storage.Short(digest))
	}
	return nil
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

	if storage.IsDigest(name) && s.store.Has(name) {
		size, r, err := s.store.Read(name)
		if err != nil {
			return nil, err
		}
		r.Close()
		return &localContent{digest: name, size: size - storage.IVSize}, nil
	}

	return nil, nil
}

// handleMessageFetchFile streams contents to the one peer that asked for them.
func (s *FileServer) handleMessageFetchFile(from string, msg MessageFetchFile) error {
	fmt.Printf("[%s] Received request to serve %s\n", s.Transport.Address(), storage.Short(msg.Digest))

	if !s.store.Has(msg.Digest) {
		return fmt.Errorf("[%s] Do not have %s", s.Transport.Address(), storage.Short(msg.Digest))
	}

	plaintextSize, fileReader, err := s.store.ReadDecrypt(s.EncryptionKey, msg.Digest)
	if err != nil {
		return fmt.Errorf("[%s] Failed to read %s: %w", s.Transport.Address(), storage.Short(msg.Digest), err)
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
	for _, nodeID := range addrs {
		if err := sendMessage(peers[nodeID], &query); err != nil {
			fmt.Printf("[%s] Error asking peer %s: %v\n", s.Transport.Address(), storage.Short(nodeID), err)
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
	fmt.Printf("[%s] Fetching '%s' (%s) from %s\n", s.Transport.Address(), name, storage.Short(digest), holder.from)

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
		// Storing a file again is a deliberate act, so any tombstone from an
		// earlier deletion is cleared rather than blocking it.
		if err := s.DB.ClearDeletion(context.Background(), name, digest); err != nil {
			log.Printf("[%s] Could not clear the deletion record for %q: %v", s.Transport.Address(), name, err)
		}
		if err := s.recordFile(name, digest, size, s.OwnerID()); err != nil {
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
			Owner:  s.OwnerID(),
		},
	}

	replicated, written := s.replicate(digest, &msg)

	if s.DB != nil {
		for _, nodeID := range replicated {
			shareID := storage.ContentKey([]byte(digest + nodeID + "outgoing"))
			if err := s.DB.InsertShare(context.Background(), dbpkg.Share{
				ID:        shareID,
				FileID:    nameKey(name),
				PeerID:    nodeID,
				Direction: "outgoing",
			}); err != nil {
				log.Printf("[%s] Failed to record outgoing share to %s: %v", s.Transport.Address(), storage.Short(nodeID), err)
			}
		}
	}

	fmt.Printf("[%s] Stored '%s' as %s (%d bytes), sent %d bytes to %d peer(s)\n",
		s.Transport.Address(), name, storage.Short(digest), size, written, len(replicated))

	// Storing is itself a measurement: the peers that accepted a copy are
	// known holders, so record it rather than waiting for a repair cycle.
	s.recordHealth(FileHealth{
		Name: name, Digest: digest, Size: size,
		Copies: 1 + len(replicated), Target: s.ReplicationFactor,
		Holders: replicated,
	})

	s.publish(Event{
		Kind: EventFileStored, Name: name, Digest: digest,
		Size: size, Count: len(replicated),
	})

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
	// Trusted rather than merely connected: sending a copy hands the contents
	// over, so it goes only to peers the operator approved.
	peers, addrs := s.trustedPeers()

	type result struct {
		addr    string
		written int64
	}
	results := make(chan result, len(addrs))

	var wg sync.WaitGroup
	for _, nodeID := range addrs {
		wg.Add(1)
		go func(nodeID string, peer p2p.Peer) {
			defer wg.Done()

			_, body, err := s.store.ReadDecrypt(s.EncryptionKey, digest)
			if err != nil {
				fmt.Printf("[%s] Could not read %s to send to %s: %v\n", s.Transport.Address(), storage.Short(digest), storage.Short(nodeID), err)
				return
			}
			if rc, ok := body.(io.Closer); ok {
				defer rc.Close()
			}

			fmt.Printf("[%s] Sending message to peer %s\n", s.Transport.Address(), storage.Short(nodeID))

			n, err := sendFile(peer, msg, body)
			if err != nil {
				fmt.Printf("[%s] Error sending %s to peer %s: %v\n", s.Transport.Address(), storage.Short(digest), storage.Short(nodeID), err)
				return
			}
			results <- result{addr: nodeID, written: n}
		}(nodeID, peers[nodeID])
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
