// A libp2p-backed implementation of p2p.Transport.
//
// TCP + Noise + yamux, one long-lived stream per peer. The tagged framing,
// the message size limit and the decoder are the ones the custom TCP
// transport uses, reused verbatim, so everything above the transport is
// unchanged. The two transports are nonetheless not interoperable: they
// differ in wire security and connection setup, so which one a network runs
// is a per-network deployment choice.
package libp2pt

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	tcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

// protocolID names the wire format spoken over the peer stream. It carries
// p2p.ProtocolVersion so that a node running a different version fails to
// open a stream at all, rather than connecting and then misreading frames.
var protocolID = protocol.ID(fmt.Sprintf("/p2pstorage/gob/%d", p2p.ProtocolVersion))

const dialTimeout = 10 * time.Second

type Opts struct {
	// ListenAddr is either a multiaddr ("/ip4/0.0.0.0/tcp/3000") or the
	// "host:port" form the rest of the system uses.
	ListenAddr string

	// Key is this node's Ed25519 private key in its 64-byte form — the same
	// bytes the keys table persists. The transport needs it because identity
	// is proven by the Noise handshake here, not by an application-level
	// challenge afterwards.
	Key []byte

	OnPeer func(p2p.Peer) error

	// OnPeerDisconnect is called once for every peer that completed OnPeer,
	// when its stream ends.
	OnPeerDisconnect func(p2p.Peer)
}

type Transport struct {
	Opts

	host    host.Host
	rpcChan chan p2p.RPC

	// mu guards peers. adoptMu additionally serialises adoption and
	// replacement end to end, so OnPeer and OnPeerDisconnect for one remote
	// node can never run out of order.
	mu      sync.Mutex
	adoptMu sync.Mutex
	peers   map[peer.ID]*Peer

	// found and mdns exist once Discover has been called.
	found func(nodeID string, addrs []string)
	mdns  io.Closer

	closed bool
}

var _ p2p.Transport = (*Transport)(nil)

func New(opts Opts) (*Transport, error) {
	if len(opts.Key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("identity key is %d bytes, want %d", len(opts.Key), ed25519.PrivateKeySize)
	}
	return &Transport{
		Opts:    opts,
		rpcChan: make(chan p2p.RPC, 1024),
		peers:   make(map[peer.ID]*Peer),
	}, nil
}

// Address returns the configured listen address, which is what log lines are
// prefixed with throughout the node.
func (t *Transport) Address() string { return t.ListenAddr }

// BoundAddr returns the "host:port" the listener actually bound, which
// differs from ListenAddr when the configured port is 0.
func (t *Transport) BoundAddr() string {
	if t.host == nil {
		return ""
	}
	for _, addr := range t.host.Network().ListenAddresses() {
		host, err1 := addr.ValueForProtocol(ma.P_IP4)
		port, err2 := addr.ValueForProtocol(ma.P_TCP)
		if err1 == nil && err2 == nil {
			return net.JoinHostPort(host, port)
		}
	}
	return ""
}

func (t *Transport) Consume() <-chan p2p.RPC { return t.rpcChan }

func (t *Transport) ListenAndAccept() error {
	listens, err := listenMultiaddrs(t.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen address %q: %w", t.ListenAddr, err)
	}

	key, err := libp2pKey(t.Key)
	if err != nil {
		return err
	}

	// Explicitly TCP + Noise, with relay off: what this transport does is
	// pinned down rather than inherited from libp2p's defaults, and the
	// pieces that need infrastructure this project lacks stay out until they
	// are deliberately added.
	h, err := libp2p.New(
		libp2p.Identity(key),
		libp2p.ListenAddrs(listens...),
		// Without this, an outbound dial originates from the listen port
		// (SO_REUSEPORT), so two nodes dialling each other at once produce
		// mirrored 4-tuples that the kernel merges into one TCP
		// simultaneous-open connection — and then both ends run the Noise
		// handshake as initiator and fail. Ephemeral source ports keep the
		// two connections distinct; the stream-level tie-break in adopt then
		// picks one. Costs reuseport's NAT friendliness, which matters only
		// when hole punching lands.
		libp2p.Transport(tcp.NewTCPTransport, tcp.DisableReuseport()),
		libp2p.Transport(quic.NewTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.ConnectionGater(ed25519Gate{}),
		// NAT traversal, in the two forms that need no outside help: ask the
		// router for a port mapping (UPnP/NAT-PMP), and answer other peers'
		// dial-back probes so they can learn their own reachability. Relay
		// and hole punching stay out: they need relay servers granting
		// reservations, an operational dependency this project lacks.
		libp2p.NATPortMap(),
		libp2p.EnableNATService(),
		libp2p.DisableRelay(),
	)
	if err != nil {
		return err
	}
	t.host = h

	h.SetStreamHandler(protocolID, func(s network.Stream) {
		t.adopt(s, false)
	})

	log.Printf("Listening via libp2p at %v\n", h.Network().ListenAddresses())
	return nil
}

// Dial connects to the peer addr names. Unlike the TCP transport, a location
// alone is not enough: libp2p cannot dial an address without knowing whose it
// is, so addr must carry a node id or at least one address must end in a
// /p2p/ component. This is why bare "host:port" bootstrap entries do not work
// on this transport.
func (t *Transport) Dial(addr p2p.Addr) error {
	if t.host == nil {
		return errors.New("transport is not started")
	}

	pid, locations, err := t.dialTarget(addr)
	if err != nil {
		return err
	}

	if t.hasPeer(pid) {
		return nil
	}

	t.host.Peerstore().AddAddrs(pid, locations, peerstore.TempAddrTTL)

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	s, err := t.host.NewStream(ctx, pid, protocolID)
	if err != nil {
		return err
	}

	go t.adopt(s, true)
	return nil
}

// dialTarget resolves addr to the peer to dial and the locations to try.
// Every location may name the peer itself via a /p2p/ component; they must
// all agree, and with addr.NodeID when that is set too.
func (t *Transport) dialTarget(addr p2p.Addr) (peer.ID, []ma.Multiaddr, error) {
	var pid peer.ID
	if addr.NodeID != "" {
		var err error
		if pid, err = peerIDForNode(addr.NodeID); err != nil {
			return "", nil, err
		}
	}

	var locations []ma.Multiaddr
	for _, raw := range addr.Addrs {
		m, err := dialMultiaddrFrom(raw)
		if err != nil {
			return "", nil, fmt.Errorf("address %q: %w", raw, err)
		}
		if info, err := peer.AddrInfoFromP2pAddr(m); err == nil {
			if pid != "" && info.ID != pid {
				return "", nil, fmt.Errorf("address %q names peer %s, but the dial expects %s", raw, info.ID, pid)
			}
			pid = info.ID
			locations = append(locations, info.Addrs...)
			continue
		}
		locations = append(locations, m)
	}

	if pid == "" {
		return "", nil, errors.New("nothing names the peer to dial: no node id, and no address with a /p2p/ component")
	}
	if len(locations) == 0 {
		return "", nil, errors.New("no address to dial")
	}
	return pid, locations, nil
}

func (t *Transport) hasPeer(pid peer.ID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.peers[pid]
	return ok
}

// adopt takes ownership of a new stream: it decides whether the stream
// becomes the peer's live stream, tells the owner, and starts the read loop.
func (t *Transport) adopt(s network.Stream, outbound bool) {
	pid := s.Conn().RemotePeer()

	// The gater already refused anything non-Ed25519; this is the backstop
	// that keeps the invariant local.
	nodeID, err := nodeIDForPeer(pid)
	if err != nil {
		s.Reset()
		return
	}

	p := &Peer{
		stream:   s,
		t:        t,
		nodeID:   nodeID,
		pid:      pid,
		outbound: outbound,
		wg:       &sync.WaitGroup{},
	}

	t.adoptMu.Lock()
	defer t.adoptMu.Unlock()

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		s.Reset()
		return
	}
	existing := t.peers[pid]
	t.mu.Unlock()

	if existing != nil {
		if !newStreamWins(t.host.ID(), pid, existing.outbound, outbound) {
			s.Reset()
			return
		}
		t.drop(existing, fmt.Errorf("replaced by a newer stream"))
	}

	if t.OnPeer != nil {
		if err := t.OnPeer(p); err != nil {
			s.Reset()
			return
		}
	}
	p.registered = true

	t.mu.Lock()
	t.peers[pid] = p
	t.mu.Unlock()

	go t.readLoop(p)
}

// newStreamWins is the simultaneous-open tie-break. When both sides dial each
// other at once, each ends up holding two streams to the same peer, and both
// must independently pick the same survivor or the connection dissolves
// entirely. The rule: the stream opened by the side with the smaller peer id
// survives. A duplicate in the same direction is a reconnect, and the newer
// stream wins.
func newStreamWins(local, remote peer.ID, existingOutbound, newOutbound bool) bool {
	if existingOutbound == newOutbound {
		return true
	}
	localIsSmaller := local < remote
	return newOutbound == localIsSmaller
}

// drop cleans a peer up exactly once, however many paths race to it.
func (t *Transport) drop(p *Peer, reason error) {
	p.teardown.Do(func() {
		fmt.Printf("[%s] Dropping peer connection: %v\n", t.ListenAddr, reason)
		p.stream.Reset()

		t.mu.Lock()
		if t.peers[p.pid] == p {
			delete(t.peers, p.pid)
		}
		t.mu.Unlock()

		if p.registered && t.OnPeerDisconnect != nil {
			t.OnPeerDisconnect(p)
		}
	})
}

// readLoop mirrors the TCP transport's: decode frames into RPCs, and pause
// while the owner consumes a stream body directly from the peer.
func (t *Transport) readLoop(p *Peer) {
	decoder := p2p.DefaultDecoder{}
	for {
		rpc := p2p.RPC{}
		if err := decoder.Decode(p, &rpc); err != nil {
			t.drop(p, err)
			return
		}

		rpc.From = p.nodeID

		if rpc.Stream {
			p.wg.Add(1)
			t.rpcChan <- rpc
			p.wg.Wait()
			continue
		}

		t.rpcChan <- rpc
	}
}

func (t *Transport) Close() error {
	t.mu.Lock()
	t.closed = true
	mdnsSvc := t.mdns
	t.mu.Unlock()

	if mdnsSvc != nil {
		mdnsSvc.Close()
	}
	if t.host == nil {
		return nil
	}
	// Closing the host resets every stream, which ends each read loop; each
	// read loop then runs its own drop.
	return t.host.Close()
}

// libp2pKey wraps the persisted 64-byte Ed25519 private key. The node keeps
// its identity across the transport switch because libp2p accepts exactly
// the form the keys table stores.
func libp2pKey(key []byte) (crypto.PrivKey, error) {
	return crypto.UnmarshalEd25519PrivateKey(key)
}

// listenMultiaddrs expands one configured listen address into everything the
// node listens on. A "host:port" form yields both TCP and QUIC on that port —
// QUIC is a separate UDP socket, so the same number serves both. An explicit
// multiaddr is taken as exactly what the operator chose.
func listenMultiaddrs(s string) ([]ma.Multiaddr, error) {
	tcpAddr, err := multiaddrFrom(s)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(s, "/") {
		return []ma.Multiaddr{tcpAddr}, nil
	}
	quicAddr, err := ma.NewMultiaddr(strings.Replace(tcpAddr.String(), "/tcp/", "/udp/", 1) + "/quic-v1")
	if err != nil {
		return nil, err
	}
	return []ma.Multiaddr{tcpAddr, quicAddr}, nil
}

// dialMultiaddrFrom is multiaddrFrom for dial targets, where an address with
// no host (":3000") means this machine rather than every interface.
func dialMultiaddrFrom(s string) (ma.Multiaddr, error) {
	if !strings.HasPrefix(s, "/") {
		if host, port, err := net.SplitHostPort(s); err == nil && host == "" {
			s = net.JoinHostPort("127.0.0.1", port)
		}
	}
	return multiaddrFrom(s)
}

// multiaddrFrom accepts either a multiaddr or the "host:port" form the rest
// of the system uses, so addresses recorded by either transport stay usable.
func multiaddrFrom(s string) (ma.Multiaddr, error) {
	if strings.HasPrefix(s, "/") {
		return ma.NewMultiaddr(s)
	}
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return nil, err
	}
	if host == "" {
		host = "0.0.0.0"
	}
	ip := net.ParseIP(host)
	switch {
	case ip == nil:
		return ma.NewMultiaddr(fmt.Sprintf("/dns4/%s/tcp/%s", host, port))
	case ip.To4() != nil:
		return ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s", ip, port))
	default:
		return ma.NewMultiaddr(fmt.Sprintf("/ip6/%s/tcp/%s", ip, port))
	}
}
