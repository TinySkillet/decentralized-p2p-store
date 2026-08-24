package node

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"sort"
	"strings"
	"sync"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// Trust is approval: a peer this node is willing to accept files and deletion
// requests from. Discovery makes peers visible; trust decides what they may do.
//
// **Trust is not checked when a peer connects.** Three reasons, any one of them
// sufficient. A UI could never show a peer awaiting approval if unapproved
// peers were refused at the door. Gossip travels over admitted connections, so
// refusing untrusted peers would partition a network in which nobody is
// trusted yet, and trust could never be bootstrapped. And nobody can approve a
// peer they have never seen. admit() keeps its own job, which is the per-host
// identity cap; enforcement here is per operation instead.
//
// The decision path reads an in-memory set rather than the database. The
// database allows one connection at a time, so querying trust on every stored
// file and every incoming stream would serialise those against every other
// query the node makes.

// trustSet is the in-memory view of which peers are approved.
type trustSet struct {
	mu      sync.RWMutex
	trusted map[string]bool

	// mode is the database's trust_mode. Held here for the same reason as the
	// set itself: it is read on every enforced operation.
	mode string
}

func newTrustSet() *trustSet {
	return &trustSet{trusted: make(map[string]bool), mode: dbpkg.TrustModeOpen}
}

func (t *trustSet) has(nodeID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.trusted[nodeID]
}

func (t *trustSet) add(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.trusted[nodeID] = true
}

func (t *trustSet) remove(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.trusted, nodeID)
}

func (t *trustSet) enforcing() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mode == dbpkg.TrustModeEnforcing
}

func (t *trustSet) currentMode() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mode
}

func (t *trustSet) setMode(mode string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mode = mode
}

func (t *trustSet) replace(ids []string, mode string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.trusted = make(map[string]bool, len(ids))
	for _, id := range ids {
		t.trusted[id] = true
	}
	t.mode = mode
}

// loadTrust reads the approved peers and the trust mode into memory.
func (s *FileServer) loadTrust() error {
	if s.DB == nil {
		return nil
	}

	ctx := context.Background()
	peers, err := s.DB.ListTrustedPeers(ctx)
	if err != nil {
		return fmt.Errorf("loading trusted peers: %w", err)
	}

	ids := make([]string, 0, len(peers))
	for _, p := range peers {
		ids = append(ids, p.NodeID)
	}

	mode := dbpkg.TrustModeOpen
	if stored, ok, err := s.DB.GetSetting(ctx, dbpkg.TrustModeSetting); err != nil {
		return fmt.Errorf("reading the trust mode: %w", err)
	} else if ok && stored != "" {
		mode = stored
	}

	s.trust.replace(ids, mode)
	return nil
}

// Trusts reports whether a peer is approved.
//
// This node always trusts itself: a command borrowing the database acts as
// this node, and refusing that would make the node unable to accept its own
// work.
func (s *FileServer) Trusts(nodeID string) bool {
	if nodeID == "" {
		return false
	}
	if nodeID == s.NodeID() || nodeID == s.OwnerID() {
		return true
	}
	return s.trust.has(nodeID)
}

// TrustEnforced reports whether refusals are active.
func (s *FileServer) TrustEnforced() bool { return s.trust.enforcing() }

// TrustMode reports the current mode.
func (s *FileServer) TrustMode() string { return s.trust.currentMode() }

// ResolvePeerID turns what a person typed into a full node identity.
//
// Node ids are 64 hex characters and every display abbreviates them, so an
// operator approving a peer has only the short form in front of them. Taking
// the abbreviation literally would record trust for an identity that does not
// exist — approval that silently never applies, which is the worst possible
// outcome for a security control.
//
// A full identity is used as given, so a peer can be approved before it has
// ever connected. Anything shorter must match exactly one known peer.
func (s *FileServer) ResolvePeerID(ctx context.Context, given string) (string, error) {
	if given == "" {
		return "", fmt.Errorf("no peer given")
	}
	if isFullNodeID(given) {
		return given, nil
	}

	seen := make(map[string]bool)
	var matches []string

	consider := func(id string) {
		if id == "" || seen[id] || !strings.HasPrefix(id, given) {
			return
		}
		seen[id] = true
		matches = append(matches, id)
	}

	_, live := s.connectedPeers()
	for _, id := range live {
		consider(id)
	}
	if s.DB != nil {
		known, err := s.DB.ListKnownPeers(ctx)
		if err != nil {
			return "", fmt.Errorf("looking up peers: %w", err)
		}
		for _, k := range known {
			consider(k.NodeID)
		}
		approved, err := s.DB.ListTrustedPeers(ctx)
		if err != nil {
			return "", fmt.Errorf("looking up approved peers: %w", err)
		}
		for _, a := range approved {
			consider(a.NodeID)
		}
	}

	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return "", fmt.Errorf("no known peer starts with %q; give the full 64-character identity to approve a peer this node has not met", given)
	default:
		sort.Strings(matches)
		short := make([]string, 0, len(matches))
		for _, m := range matches {
			short = append(short, storage.Short(m))
		}
		return "", fmt.Errorf("%q matches %d peers (%s); use more characters", given, len(matches), strings.Join(short, ", "))
	}
}

// nodeIDLength is how many characters a full identity has: an Ed25519 public
// key, hex encoded.
const nodeIDLength = ed25519.PublicKeySize * 2

// isFullNodeID reports whether s is a complete hex-encoded identity.
func isFullNodeID(s string) bool {
	if len(s) != nodeIDLength {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f', r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return true
}

// Trust approves a peer so it may push files and request deletions here.
func (s *FileServer) Trust(nodeID, label string) error {
	if s.DB == nil {
		return fmt.Errorf("no database to record trust in")
	}
	if nodeID == "" {
		return fmt.Errorf("a peer cannot be trusted without an identity")
	}

	ctx := context.Background()
	nodeID, err := s.ResolvePeerID(ctx, nodeID)
	if err != nil {
		return err
	}

	if err := s.DB.TrustPeer(ctx, nodeID, label); err != nil {
		return err
	}
	s.trust.add(nodeID)

	fmt.Printf("[%s] Trusting %s\n", s.Transport.Address(), storage.Short(nodeID))
	s.publish(Event{Kind: EventPeerTrusted, Node: nodeID})
	return nil
}

// Untrust withdraws approval, and reports whether the peer had it.
func (s *FileServer) Untrust(nodeID string) (bool, error) {
	if s.DB == nil {
		return false, fmt.Errorf("no database to record trust in")
	}
	ctx := context.Background()
	nodeID, err := s.ResolvePeerID(ctx, nodeID)
	if err != nil {
		return false, err
	}

	had, err := s.DB.UntrustPeer(ctx, nodeID)
	if err != nil {
		return false, err
	}
	s.trust.remove(nodeID)

	if had {
		fmt.Printf("[%s] No longer trusting %s\n", s.Transport.Address(), storage.Short(nodeID))
		s.publish(Event{Kind: EventPeerUntrusted, Node: nodeID})
	}
	return had, nil
}

// SetTrustMode switches enforcement on or off.
func (s *FileServer) SetTrustMode(mode string) error {
	if s.DB == nil {
		return fmt.Errorf("no database to record the trust mode in")
	}
	switch mode {
	case dbpkg.TrustModeOpen, dbpkg.TrustModeEnforcing:
	default:
		return fmt.Errorf("unknown trust mode %q, want %q or %q",
			mode, dbpkg.TrustModeOpen, dbpkg.TrustModeEnforcing)
	}

	if err := s.DB.PutSetting(context.Background(), dbpkg.TrustModeSetting, mode); err != nil {
		return err
	}
	s.trust.setMode(mode)

	fmt.Printf("[%s] Trust mode is now %s\n", s.Transport.Address(), mode)
	return nil
}

// TrustedPeerView is one approved peer, with whether it is connected now.
type TrustedPeerView struct {
	dbpkg.TrustedPeer
	Online bool
}

// TrustedPeers reports the approved peers.
func (s *FileServer) TrustedPeers(ctx context.Context) ([]TrustedPeerView, error) {
	if s.DB == nil {
		return nil, nil
	}

	approved, err := s.DB.ListTrustedPeers(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]TrustedPeerView, 0, len(approved))
	for _, p := range approved {
		out = append(out, TrustedPeerView{TrustedPeer: p, Online: s.hasPeerWithNodeID(p.NodeID)})
	}
	return out, nil
}

// trustedPeers returns the connected peers that are approved to receive a copy
// of a file from this node.
//
// Sits beside connectedPeers because replication must not hand data to a peer
// the operator has not approved, while queries and gossip still may.
func (s *FileServer) trustedPeers() (map[string]p2p.Peer, []string) {
	peers, ids := s.connectedPeers()
	if !s.TrustEnforced() {
		return peers, ids
	}

	kept := make(map[string]p2p.Peer, len(peers))
	keptIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		if s.Trusts(id) {
			kept[id] = peers[id]
			keptIDs = append(keptIDs, id)
		}
	}
	sort.Strings(keptIDs)
	return kept, keptIDs
}
