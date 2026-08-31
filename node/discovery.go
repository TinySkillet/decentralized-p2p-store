// Local-network peer discovery: noticing peers without configured addresses.
package node

import (
	"context"
	"fmt"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// discoveryQuiet is how long a discovered peer stays quiet before being
// announced again. mDNS re-broadcasts every few seconds; without this every
// broadcast would be another database write and another event in the UI.
const discoveryQuiet = time.Minute

// EnableDiscovery starts local-network discovery, on transports that can.
//
// Discovery only makes peers visible. A discovered peer is recorded and shown
// for approval; it is dialled only if it is already approved — approval is
// the gate, discovery is just the doorbell. This keeps the trust model intact
// on a network full of strangers: they appear, nothing more.
func (s *FileServer) EnableDiscovery() error {
	d, ok := s.Transport.(p2p.Discoverer)
	if !ok {
		return fmt.Errorf("this transport cannot discover peers; discovery needs the libp2p transport")
	}
	return d.Discover(s.onPeerDiscovered)
}

// onPeerDiscovered handles one discovery report. It must not block: the
// transport calls it from its discovery loop.
func (s *FileServer) onPeerDiscovered(nodeID string, addrs []string) {
	if nodeID == "" || nodeID == s.NodeID() || len(addrs) == 0 {
		return
	}
	// Already connected: nothing to record or announce, and certainly
	// nothing to dial.
	if s.hasPeerWithNodeID(nodeID) {
		return
	}
	if !s.discoveryDebounce(nodeID) {
		return
	}

	fmt.Printf("[%s] Discovered %s on the local network\n", s.Transport.Address(), storage.Short(nodeID))

	if s.DB != nil {
		now := time.Now()
		if err := s.DB.UpsertPeer(context.Background(), dbpkg.Peer{
			NodeID:   nodeID,
			Address:  addrs[0],
			Addrs:    addrs,
			Status:   "discovered",
			LastSeen: &now,
		}); err != nil {
			fmt.Printf("[%s] Could not record discovered peer %s: %v\n", s.Transport.Address(), storage.Short(nodeID), err)
		}
	}

	s.publish(Event{Kind: EventPeerDiscovered, Node: nodeID, Peer: addrs[0]})

	// Approval is the gate. An approved peer reappearing on the network is
	// the reconnection this feature exists for; a stranger stays a listing.
	// Checked against the trust list in every mode: open mode not enforcing
	// pushes is no reason to start connections nobody asked for.
	if !s.trust.has(nodeID) {
		return
	}

	go func() {
		if err := s.Transport.Dial(p2p.Addr{NodeID: nodeID, Addrs: addrs}); err != nil {
			fmt.Printf("[%s] Could not connect to discovered peer %s: %v\n", s.Transport.Address(), storage.Short(nodeID), err)
		}
	}()
}

// discoveryDebounce reports whether this discovery is worth acting on, and
// records that it was acted on.
func (s *FileServer) discoveryDebounce(nodeID string) bool {
	s.discoveredMu.Lock()
	defer s.discoveredMu.Unlock()
	if s.discoveredAt == nil {
		s.discoveredAt = make(map[string]time.Time)
	}
	if last, ok := s.discoveredAt[nodeID]; ok && time.Since(last) < discoveryQuiet {
		return false
	}
	s.discoveredAt[nodeID] = time.Now()
	return true
}
