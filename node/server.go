package node

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"

	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// Serve processes incoming messages until Stop is called.
//
// Repair runs alongside it: a file replicated when it was stored stays
// replicated only if something notices when its holders leave.
func (s *FileServer) Serve() {
	if s.RepairInterval > 0 {
		go s.repairLoop()
	}
	if s.SweepInterval > 0 {
		go s.sweepLoop()
	}
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

// notifyDeleted tells a peer that what it just offered has been deleted here,
// so a deletion reaches nodes that were offline when it was broadcast.
//
// The original authorisation is replayed with it. The receiving peer verifies
// it against its own record of who owns the file, so this node relaying the
// message does not need to be trusted.
func (s *FileServer) notifyDeleted(addr, name, digest string) {
	peer, ok := s.peer(addr)
	if !ok {
		return
	}

	owner, signature := s.deletionAuthorisation(name, digest)

	msg := Message{Payload: MessageDeleteFile{
		Name:      name,
		Digest:    digest,
		Owner:     owner,
		Signature: signature,
	}}
	if err := sendMessage(peer, &msg); err != nil {
		log.Printf("[%s] Could not tell %s that %q was deleted: %v", s.Transport.Address(), addr, name, err)
	}
}

// deletionAuthorisation returns the authorisation recorded when a file was
// deleted here, so it can be replayed to a peer that still holds it.
func (s *FileServer) deletionAuthorisation(name, digest string) (owner string, signature []byte) {
	if s.DB == nil {
		return "", nil
	}
	owner, signature, ok, err := s.DB.GetDeletion(context.Background(), name, digest)
	if err != nil {
		log.Printf("[%s] Could not read the deletion record for %q: %v", s.Transport.Address(), name, err)
		return "", nil
	}
	if !ok {
		return "", nil
	}
	return owner, signature
}

// storageOwnerID returns the identity of the node whose storage this one is
// borrowing, or "" when this node holds its files in its own right.
//
// The identity persisted in a database belongs to the long-lived node that
// owns it; a command opening the same database runs under a fresh identity.
// Identity is compared rather than address because a node advertises the port
// it was configured with, while its peers know it by the address the
// connection came from.
// NodeID returns this node's network identifier.
func (s *FileServer) NodeID() string { return s.Identity.NodeID() }

// OwnerID returns the identity that owns the files in this node's database.
func (s *FileServer) OwnerID() string { return s.OwnerIdentity.NodeID() }

func (s *FileServer) storageOwnerID() string {
	if s.OwnsDatabase || s.DB == nil {
		return ""
	}

	// Only meaningful when a serving node has actually claimed the database.
	if _, ok, err := s.DB.GetSetting(context.Background(), dbpkg.ServingAddressSetting); err != nil || !ok {
		return ""
	}

	id, ok, err := s.DB.StoredNodeID(context.Background())
	if err != nil || !ok {
		return ""
	}
	return id
}

// isStorageOwner reports whether a peer is the node whose storage this one is
// borrowing. Peers are identified by node id, so this is a direct comparison.
func (s *FileServer) isStorageOwner(nodeID, ownerID string) bool {
	return ownerID != "" && nodeID == ownerID
}

func (s *FileServer) handleMessageDeleteFile(from string, msg MessageDeleteFile) error {
	fmt.Printf("[%s] Received delete request for '%s' from %s\n", s.Transport.Address(), msg.Name, from)

	if err := s.authorizeDelete(msg); err != nil {
		return fmt.Errorf("[%s] Refusing to delete '%s' for %s: %w", s.Transport.Address(), msg.Name, from, err)
	}

	if err := s.forget(msg.Name, msg.Digest, msg.Owner, msg.Signature); err != nil {
		return fmt.Errorf("[%s] %w", s.Transport.Address(), err)
	}
	return nil
}

// authorizeDelete checks that a deletion request came from the node entitled
// to make it.
//
// The file records who stored it, and only that identity can authorise its
// removal. Files stored before ownership was recorded have no owner, and are
// accepted unsigned so that upgrading a network does not strand data nobody
// can delete.
func (s *FileServer) authorizeDelete(msg MessageDeleteFile) error {
	if s.DB == nil {
		return nil
	}

	f, err := s.DB.FindFileByName(context.Background(), msg.Name)
	if err != nil {
		return fmt.Errorf("looking up the file: %w", err)
	}
	if f == nil {
		// Nothing here to delete, so nothing to authorise.
		return nil
	}
	if msg.Digest != "" && f.Hash != msg.Digest {
		// A different file of the same name; forget() will decline it.
		return nil
	}

	if f.Owner == "" {
		// Stored before ownership was recorded.
		return nil
	}
	if msg.Owner != f.Owner {
		return fmt.Errorf("it is owned by %s, not %s", storage.Short(f.Owner), storage.Short(msg.Owner))
	}
	if !verifyByNode(msg.Owner, deleteTranscript(msg.Name, f.Hash), msg.Signature) {
		return fmt.Errorf("the authorisation does not verify against %s", storage.Short(msg.Owner))
	}

	// The authorisation is genuine, and still may not apply.
	//
	// As the file's owner we would hold a tombstone if we wanted it gone, so
	// holding the file without one means it was deliberately stored again. An
	// authorisation stays valid for ever, and a peer that kept the old
	// tombstone replays it when it refuses the new copy — which would destroy
	// the copy we had just chosen to make.
	//
	// A genuine deletion here never reaches this path: Delete records the
	// tombstone locally before it broadcasts.
	if f.Owner == s.OwnerID() {
		deleted, err := s.DB.IsDeleted(context.Background(), msg.Name, f.Hash)
		if err != nil {
			return fmt.Errorf("checking our own deletion record: %w", err)
		}
		if !deleted {
			return fmt.Errorf("we own %q and hold no deletion record for it, so it was stored again deliberately", msg.Name)
		}
	}

	return nil
}

// forget removes a name.
//
// The contents are not unlinked here. Deduplication means one blob can back
// several names, and deciding it is unreferenced and then unlinking it are two
// steps: a name recorded in between would be left pointing at data that had
// just been deleted. Instead the name mapping is the single source of truth
// and unreachable contents are reclaimed by the sweep, which makes that
// decision and acts on it together.
func (s *FileServer) forget(name, digest, owner string, signature []byte) error {
	if s.DB == nil {
		// Without a database there is no name mapping, so the name can only
		// be the contents themselves.
		if err := s.store.Delete(name); err != nil {
			return fmt.Errorf("deleting %q: %w", name, err)
		}
		return nil
	}

	hash, orphaned, err := s.DB.DeleteFileByName(context.Background(), name, digest, owner, signature)
	if err != nil {
		// Stop before touching the disk. Removing the bytes while the
		// metadata still points at them is the inconsistency worth avoiding;
		// leaving both in place is recoverable.
		return fmt.Errorf("deleting %q from the database: %w", name, err)
	}

	if hash == "" {
		fmt.Printf("[%s] Nothing matching '%s' stored here, skipping deletion\n", s.Transport.Address(), name)
		return nil
	}

	fmt.Printf("[%s] Removed name '%s' from the database\n", s.Transport.Address(), name)

	s.publish(Event{Kind: EventFileDeleted, Name: name, Digest: hash})

	if orphaned {
		reclaimed, rerr := s.reclaim(hash, DefaultOrphanGrace)
		switch {
		case rerr != nil:
			log.Printf("[%s] Could not reclaim %s: %v", s.Transport.Address(), storage.Short(hash), rerr)
		case reclaimed:
			fmt.Printf("[%s] Deleted contents %s from local storage\n", s.Transport.Address(), storage.Short(hash))
		default:
			fmt.Printf("[%s] Contents %s are now unreferenced and will be reclaimed shortly\n", s.Transport.Address(), storage.Short(hash))
		}
	} else {
		fmt.Printf("[%s] Contents %s are still referenced by another name, keeping them\n", s.Transport.Address(), storage.Short(hash))
	}
	return nil
}

// Delete removes a name locally and asks every peer to do the same.
func (s *FileServer) Delete(name string) error {
	// The digest is needed before the name mapping goes, so peers can tell
	// this deletion applies to their copy and not to a different file of the
	// same name.
	var digest, owner string
	var signature []byte
	if local, err := s.resolve(name); err != nil {
		return err
	} else if local != nil {
		digest = local.digest

		// Peers verify the authorisation against the file's recorded owner, so
		// it has to be produced here while the record still exists.
		if s.DB != nil {
			f, err := s.DB.FindFileByName(context.Background(), name)
			if err != nil {
				return err
			}
			if f != nil {
				owner = f.Owner
			}
		}
		if owner == s.OwnerID() {
			signature = s.OwnerIdentity.Sign(deleteTranscript(name, digest))
		} else if owner != "" {
			return fmt.Errorf("'%s' is owned by %s, so this node cannot authorise deleting it", name, storage.Short(owner))
		}
	}

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

	if err := s.forget(name, digest, owner, signature); err != nil {
		return fmt.Errorf("[%s] %w", s.Transport.Address(), err)
	}

	// Reconnect to peers that were sent a copy but are not currently
	// connected, so the deletion reaches them too.
	//
	// The share records name identities, which have to be turned back into
	// somewhere to dial. A peer whose address is unknown is skipped: the
	// deletion reaches it when it next connects, which is already how the
	// system treats a peer that is simply offline.
	if len(sharePeers) > 0 {
		fmt.Printf("[%s] Reconnecting to %d peer(s) from shares\n", s.Transport.Address(), len(sharePeers))

		addresses, err := s.DB.AddressesForNodes(context.Background(), sharePeers)
		if err != nil {
			fmt.Printf("[%s] Warning: could not resolve share peer addresses: %v\n", s.Transport.Address(), err)
			addresses = nil
		}

		var wg sync.WaitGroup
		for _, nodeID := range sharePeers {
			if _, already := s.peer(nodeID); already {
				continue
			}
			addr := addresses[nodeID]
			if addr == "" {
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
		s.WaitForPeers(2 * time.Second)
	}

	peerCount := s.peerCount()
	if peerCount == 0 {
		fmt.Printf("[%s] No peers connected, cannot broadcast delete message\n", s.Transport.Address())
		return nil
	}

	msg := Message{Payload: MessageDeleteFile{
		Name:      name,
		Digest:    digest,
		Owner:     owner,
		Signature: signature,
	}}
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
		if s.controlListener != nil {
			s.controlListener.Close()
		}

		peers, _ := s.connectedPeers()
		for _, p := range peers {
			p.Close()
		}
	})
}

type FileServerOpts struct {
	// Identity is the signing key that names this node on the network, used
	// for the handshake.
	Identity Identity

	// OwnerIdentity signs ownership of stored files.
	//
	// It is the identity persisted in the database, which is not always this
	// process's network identity: a one-shot command joins the network under a
	// throwaway key so that the node it borrows the database from does not
	// refuse the connection as one to itself. Files must still be owned by the
	// database's own identity, or a command would store files that nobody,
	// itself included, could ever delete.
	OwnerIdentity Identity

	// MaxPeersPerHost caps how many identities may be accepted from one
	// address. Zero means the package default.
	MaxPeersPerHost int

	// ReplicationFactor is how many copies of a file the network aims to
	// hold, counting the node that stored it. Zero means the default.
	ReplicationFactor int

	// RepairInterval is how often under-replicated files are restored. Zero
	// means the default; a negative value disables repair entirely.
	RepairInterval time.Duration

	// SweepInterval is how often unreachable data is reclaimed. Zero means
	// the default; a negative value disables the sweep.
	SweepInterval time.Duration

	// OwnsDatabase marks the long-lived node that the database and storage
	// root belong to. Commands run against the same database with this unset,
	// because their copy of a file is that node's copy, not a second one.
	OwnsDatabase bool

	EncryptionKey     []byte
	StorageRoot       string
	PathTransformFunc storage.PathTransformFunc
	Transport         p2p.Transport
	BootstrapNodes    []string
	DB                *dbpkg.DB
}

type FileServer struct {
	FileServerOpts

	peersLock sync.Mutex
	peers     map[string]p2p.Peer

	store    *storage.Store
	quitch   chan struct{}
	stopOnce sync.Once

	// controlListener accepts commands from the CLI, when this node is the one
	// that owns the database.
	controlListener net.Listener

	// events fans node activity out to whoever is watching.
	events *eventBus

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
	if !opts.OwnerIdentity.Valid() {
		opts.OwnerIdentity = opts.Identity
	}
	if opts.MaxPeersPerHost <= 0 {
		opts.MaxPeersPerHost = dbpkg.DefaultMaxPeersPerHost
	}
	if opts.ReplicationFactor <= 0 {
		opts.ReplicationFactor = DefaultReplicationFactor
	}
	if opts.RepairInterval == 0 {
		opts.RepairInterval = DefaultRepairInterval
	}
	if opts.SweepInterval == 0 {
		opts.SweepInterval = DefaultSweepInterval
	}
	storeOpts := storage.StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}
	return &FileServer{
		FileServerOpts:       opts,
		events:               newEventBus(),
		store:                storage.NewStore(storeOpts),
		quitch:               make(chan struct{}),
		peers:                make(map[string]p2p.Peer),
		pendingFileTransfers: make(map[string]MessageStoreFile),
		requests:             make(map[string]*fileRequest),
	}
}
