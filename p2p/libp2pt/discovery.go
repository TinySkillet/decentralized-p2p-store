package libp2pt

import (
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

// mdnsServiceTag names this system on the local network. It carries the
// protocol version for the same reason protocolID does: nodes speaking
// different versions cannot talk, so they should not find each other either.
var mdnsServiceTag = fmt.Sprintf("p2pstorage-v%d", p2p.ProtocolVersion)

// Discover implements p2p.Discoverer with multicast DNS: every node running
// discovery on the same local network announces itself and notices the
// others, with no addresses configured anywhere.
//
// Discovery only reports peers. Whether a discovered peer is dialled is the
// caller's decision, which is the point: peers on a coffee-shop network
// should become visible and approvable, not connected.
func (t *Transport) Discover(found func(nodeID string, addrs []string)) error {
	if t.host == nil {
		return errors.New("transport is not started")
	}
	if found == nil {
		return errors.New("discovery needs a callback to report peers to")
	}

	t.mu.Lock()
	if t.found != nil {
		t.mu.Unlock()
		return errors.New("discovery is already running")
	}
	t.found = found
	t.mu.Unlock()

	svc := mdns.NewMdnsService(t.host, mdnsServiceTag, notifee{t})
	if err := svc.Start(); err != nil {
		return err
	}

	t.mu.Lock()
	t.mdns = svc
	t.mu.Unlock()
	return nil
}

// notifee receives raw mDNS results. It is a named type because the mdns
// package wants an interface, not a func.
type notifee struct{ t *Transport }

func (n notifee) HandlePeerFound(info peer.AddrInfo) { n.t.peerFound(info) }

// peerFound translates one mDNS result into the terms the rest of the system
// speaks: a canonical node id and complete dial targets.
func (t *Transport) peerFound(info peer.AddrInfo) {
	if info.ID == t.host.ID() {
		return
	}
	nodeID, err := nodeIDForPeer(info.ID)
	if err != nil {
		// Not an identity this system can express (an RSA peer, say). The
		// gater would refuse the connection anyway; refusing here keeps it
		// out of the discovered list too.
		return
	}

	suffix, err := ma.NewMultiaddr("/p2p/" + info.ID.String())
	if err != nil {
		return
	}
	addrs := make([]string, 0, len(info.Addrs))
	for _, addr := range info.Addrs {
		addrs = append(addrs, addr.Encapsulate(suffix).String())
	}
	if len(addrs) == 0 {
		return
	}

	t.mu.Lock()
	found := t.found
	t.mu.Unlock()
	if found != nil {
		found(nodeID, addrs)
	}
}
