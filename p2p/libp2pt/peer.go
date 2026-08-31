package libp2pt

import (
	"io"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

// Peer is one connected node, reached over a single long-lived libp2p stream.
//
// One stream per peer, not one per transfer, is what preserves the p2p.Peer
// contract the node was built against: messages and file bodies share an
// ordered byte stream, and yamux's per-stream flow control means a blocked
// reader back-pressures the sender exactly as a TCP socket does.
type Peer struct {
	stream network.Stream
	t      *Transport

	// nodeID is the canonical identity, derived from the peer id that the
	// Noise handshake proved. There is no separate application handshake on
	// this transport: the connection cannot exist without the proof.
	nodeID   string
	pid      peer.ID
	outbound bool

	wg *sync.WaitGroup

	// writeLock serialises writes exactly as the TCP transport does: several
	// goroutines send to the same peer, and interleaved writes would corrupt
	// the frame stream.
	writeLock sync.Mutex

	// registered is set once OnPeer accepted the peer, before the read loop
	// starts. OnPeerDisconnect fires only for registered peers.
	registered bool

	// teardown makes drop idempotent: a replaced peer and a failed read race
	// to clean up the same stream.
	teardown sync.Once
}

var _ p2p.Peer = (*Peer)(nil)
var _ p2p.Located = (*Peer)(nil)

func (p *Peer) ID() string { return p.nodeID }

func (p *Peer) Read(b []byte) (int, error) { return p.stream.Read(b) }

func (p *Peer) Write(b []byte) (int, error) {
	p.writeLock.Lock()
	defer p.writeLock.Unlock()
	return p.stream.Write(b)
}

func (p *Peer) Close() error { return p.stream.Close() }

func (p *Peer) Send(b []byte) error {
	p.writeLock.Lock()
	defer p.writeLock.Unlock()
	_, err := p.stream.Write(b)
	return err
}

// SendStream writes header, the stream tag, and then body while holding the
// write lock, so the transfer is indivisible from the receiver's point of
// view.
func (p *Peer) SendStream(header []byte, body io.Reader) (int64, error) {
	p.writeLock.Lock()
	defer p.writeLock.Unlock()

	if _, err := p.stream.Write(header); err != nil {
		return 0, err
	}
	if _, err := p.stream.Write([]byte{p2p.IncomingStream}); err != nil {
		return 0, err
	}
	return io.Copy(p.stream, body)
}

func (p *Peer) CloseStream() { p.wg.Done() }

// RemoteHost implements p2p.Located: the host the connection actually came
// from, which is what the per-host admission limit counts against.
func (p *Peer) RemoteHost() string {
	remote := p.stream.Conn().RemoteMultiaddr()
	for _, proto := range []int{ma.P_IP4, ma.P_IP6, ma.P_DNS, ma.P_DNS4, ma.P_DNS6} {
		if host, err := remote.ValueForProtocol(proto); err == nil {
			return host
		}
	}
	return ""
}

// AdvertisedAddrs implements p2p.Located. The addresses are full multiaddrs
// including the /p2p/ component, so they are complete dial targets in the
// form this transport's Dial accepts.
func (p *Peer) AdvertisedAddrs() []string {
	suffix, err := ma.NewMultiaddr("/p2p/" + p.pid.String())
	if err != nil {
		return nil
	}
	var out []string
	for _, addr := range p.t.host.Peerstore().Addrs(p.pid) {
		out = append(out, addr.Encapsulate(suffix).String())
	}
	return out
}
