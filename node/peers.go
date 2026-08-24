// Tracking which peers this node is connected to, and admitting new ones.
package node

import (
	"context"
	"fmt"
	"log"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"

	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// connectedPeers returns a snapshot of the current peers.
//
// Callers must not hold peersLock while writing to the network: a peer that
// stops reading would otherwise block every other operation on the node.
func (s *FileServer) connectedPeers() (map[string]p2p.Peer, []string) {
	s.peersLock.Lock()
	defer s.peersLock.Unlock()

	peers := make(map[string]p2p.Peer, len(s.peers))
	ids := make([]string, 0, len(s.peers))
	for id, peer := range s.peers {
		peers[id] = peer
		ids = append(ids, id)
	}
	return peers, ids
}

func (s *FileServer) peer(nodeID string) (p2p.Peer, bool) {
	s.peersLock.Lock()
	defer s.peersLock.Unlock()
	p, ok := s.peers[nodeID]
	return p, ok
}

// peerAddress returns an address this peer can be reached at, or "" when its
// transport does not deal in addresses.
//
// Only the admission limit and the peer records need this; everything else
// reasons about identity, which every transport has.
func peerAddress(p p2p.Peer) string {
	located, ok := p.(p2p.Located)
	if !ok {
		return ""
	}
	addrs := located.AdvertisedAddrs()
	if len(addrs) == 0 {
		return ""
	}
	return addrs[0]
}

// advertisedAddrs returns every address a peer says it can be reached at.
func advertisedAddrs(p p2p.Peer) []string {
	located, ok := p.(p2p.Located)
	if !ok {
		return nil
	}
	return located.AdvertisedAddrs()
}

// hasPeerWithNodeID reports whether a peer with this identity is already
// connected, possibly at a different address.
func (s *FileServer) hasPeerWithNodeID(nodeID string) bool {
	if nodeID == "" {
		return false
	}
	_, ok := s.peer(nodeID)
	return ok
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
	for _, id := range addrs {
		fmt.Printf("[%s] Sending message to peer %s\n", s.Transport.Address(), storage.Short(id))
		if err := sendMessage(peers[id], msg); err != nil {
			fmt.Printf("[%s] Error sending message to peer %s: %v\n", s.Transport.Address(), storage.Short(id), err)
			lastErr = err
		}
	}

	return lastErr
}

func (s *FileServer) OnPeer(p p2p.Peer) error {
	nodeID := p.ID()
	peerAddr := peerAddress(p)

	if err := s.admit(peerAddr, nodeID); err != nil {
		return err
	}

	// Keyed by identity. The address is recorded alongside as a location hint,
	// but it is not what the peer is: it changes when the node moves network,
	// and a peer may be reachable at several.
	s.peersLock.Lock()
	s.peers[nodeID] = p
	s.peersLock.Unlock()

	fmt.Printf("[%s] Connected with %s at %s\n", s.Transport.Address(), storage.Short(nodeID), peerAddr)

	if s.DB != nil {
		now := time.Now()
		if err := s.DB.UpsertPeer(context.Background(), dbpkg.Peer{
			NodeID:   nodeID,
			Address:  peerAddr,
			Addrs:    advertisedAddrs(p),
			Status:   "connected",
			LastSeen: &now,
		}); err != nil {
			log.Printf("[%s] Failed to record peer %s: %v", s.Transport.Address(), peerAddr, err)
		}
	}

	// Published after the lock is released and after the peer is registered,
	// so a subscriber reacting to this can already see it in the peer set.
	s.publish(Event{Kind: EventPeerUp, Node: nodeID, Peer: peerAddr})

	go s.announceTo(nodeID)

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

	err = fmt.Errorf("refusing peer %s: host %s already has %d identities, limit is %d",
		nodeID, host, known, s.MaxPeersPerHost)

	// Surfaced as an event so a peer bouncing off the limit is visible rather
	// than buried in the log.
	s.publish(Event{Kind: EventPeerRefused, Node: nodeID, Peer: peerAddr, Count: known, Err: err.Error()})

	return err
}

// OnPeerDisconnect drops a peer whose connection has ended. Without it the
// node keeps broadcasting to a closed socket and reports stale peer counts.
func (s *FileServer) OnPeerDisconnect(p p2p.Peer) {
	nodeID := p.ID()
	peerAddr := peerAddress(p)

	s.peersLock.Lock()
	delete(s.peers, nodeID)
	s.peersLock.Unlock()

	fmt.Printf("[%s] Disconnected from %s\n", s.Transport.Address(), storage.Short(nodeID))

	s.publish(Event{Kind: EventPeerDown, Node: nodeID, Peer: peerAddr})

	if s.DB != nil {
		now := time.Now()
		if err := s.DB.UpsertPeer(context.Background(), dbpkg.Peer{
			NodeID:   nodeID,
			Address:  peerAddr,
			Addrs:    advertisedAddrs(p),
			Status:   "disconnected",
			LastSeen: &now,
		}); err != nil {
			log.Printf("[%s] Failed to record peer %s: %v", s.Transport.Address(), peerAddr, err)
		}
	}
}

// announceTo sends this node's peer list to a newly connected peer, retrying
// briefly while the connection settles.
func (s *FileServer) announceTo(nodeID string) {
	const attempts = 5

	for i := range attempts {
		if err := s.sendPeerExchange(nodeID); err == nil {
			return
		} else {
			fmt.Printf("[%s] Error sending peer exchange to %s: %v (attempt %d/%d)\n",
				s.Transport.Address(), storage.Short(nodeID), err, i+1, attempts)
		}

		select {
		case <-time.After(250 * time.Millisecond):
		case <-s.quitch:
			return
		}
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

// WaitForPeerDiscovery waits until the peer set stops growing, and returns
// the number of peers connected.
//
// Gossip reaches further than the bootstrap list names, so a command that
// acted the moment the first peer connected would replicate to a fraction of
// the network it could have reached. Waiting for the count to hold steady
// adapts to how long discovery actually takes, where a fixed sleep is either
// too short on a slow network or wasted time on a fast one.
func (s *FileServer) WaitForPeerDiscovery(quiet, max time.Duration) int {
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

// WaitForPeers waits for at least one peer connection, with a timeout
func (s *FileServer) WaitForPeers(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.peerCount() > 0 {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for peer connections")
}
