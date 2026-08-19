package p2p

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

func (t *TCPTransport) Close() error {
	if t.listener == nil {
		return nil
	}
	return t.listener.Close()
}

// Address returns the address this node advertises to peers. It is the
// configured ListenAddr, not the bound socket address, because that is the
// value exchanged during the handshake and stored by remote peers.
func (t *TCPTransport) Address() string {
	return t.ListenAddr
}

// BoundAddr returns the address the listener actually bound, which differs
// from ListenAddr when the configured port is 0.
func (t *TCPTransport) BoundAddr() string {
	return t.boundAddr
}

func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcChan
}

func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	go t.handleConn(conn, true)
	return nil
}

func (t *TCPTransport) ListenAndAccept() error {
	ln, err := net.Listen("tcp", t.ListenAddr)
	if err != nil {
		return err
	}
	t.listener = ln
	t.boundAddr = ln.Addr().String()

	go t.startAcceptLoop()
	return nil
}

func (t *TCPTransport) startAcceptLoop() {
	log.Printf("Listening on TCP at %s\n", t.ListenAddr)
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			// Must continue rather than fall through: on a transient error
			// conn is nil, and touching it panics the whole node.
			fmt.Printf("[%s] TCP accept error: %v\n", t.ListenAddr, err)
			continue
		}

		fmt.Printf("[%s] New Incoming Connection: %+v\n", t.ListenAddr, conn.RemoteAddr().String())
		go t.handleConn(conn, false)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	var err error

	peer := NewTCPPeer(conn, outbound)
	registered := false

	defer func() {
		if err != nil {
			fmt.Printf("[%s] Dropping peer connection: %v\n", t.ListenAddr, err)
		} else {
			fmt.Printf("[%s] Closing peer connection to %s\n", t.ListenAddr, peer.RemoteAddr())
		}
		conn.Close()

		// Tell the owner the peer is gone. Without this the server keeps
		// broadcasting to a dead connection forever.
		if registered && t.OnPeerDisconnect != nil {
			t.OnPeerDisconnect(peer)
		}
	}()

	if err = t.HandshakeFunc(peer); err != nil {
		return
	}

	if t.OnPeer != nil {
		if err = t.OnPeer(peer); err != nil {
			return
		}
	}
	registered = true

	for {
		rpc := RPC{}
		err = t.Decoder.Decode(conn, &rpc)
		if err != nil {
			return
		}

		rpc.From = peer.RemoteAddr().String()

		if rpc.Stream {
			peer.wg.Add(1)
			fmt.Printf("[%s] Incoming stream from [%s], waiting till stream is done...\n", t.ListenAddr, conn.RemoteAddr().String())

			t.rpcChan <- rpc

			peer.wg.Wait()
			fmt.Printf("[%s] Stream from [%s] closed. Resuming normal read loop.\n", t.ListenAddr, conn.RemoteAddr().String())

			continue
		}

		t.rpcChan <- rpc
	}
}

type TCPPeer struct {
	net.Conn

	outbound bool

	wg *sync.WaitGroup

	// writeLock serialises writes to the connection. Several goroutines send
	// to the same peer (broadcasts, stream replies, peer exchange), and
	// without this their bytes interleave and corrupt the frame stream.
	writeLock sync.Mutex

	// FullAddr is the address other nodes can reach this peer on. The
	// handshake fills it in by pairing the port the peer advertises with the
	// source address the connection actually arrived from, so it stays
	// routable even when the peer was configured with a bare ":3000".
	FullAddr string

	// NodeID is the peer's stable identifier, learned during the handshake.
	NodeID string
}

// ID returns the peer's node identifier.
func (p *TCPPeer) ID() string { return p.NodeID }

// ObservedHost returns the host half of the address this connection came
// from. The handshake pairs it with the port the peer advertises.
func (p *TCPPeer) ObservedHost() string {
	host, _, err := net.SplitHostPort(p.Conn.RemoteAddr().String())
	if err != nil {
		return ""
	}
	return host
}

func (p *TCPPeer) RemoteAddr() net.Addr {
	if p.FullAddr != "" {
		addr, err := net.ResolveTCPAddr("tcp", p.FullAddr)
		if err == nil {
			return addr
		}
	}
	return p.Conn.RemoteAddr()
}

func (p *TCPPeer) Send(b []byte) error {
	p.writeLock.Lock()
	defer p.writeLock.Unlock()
	_, err := p.Conn.Write(b)
	return err
}

// Write satisfies net.Conn and takes the same lock as Send, so callers
// streaming a file body cannot interleave with a concurrent Send.
func (p *TCPPeer) Write(b []byte) (int, error) {
	p.writeLock.Lock()
	defer p.writeLock.Unlock()
	return p.Conn.Write(b)
}

// SendStream writes header, the stream tag, and then body, holding the
// connection's write lock for the whole transfer so that nothing else can be
// written into the middle of it.
func (p *TCPPeer) SendStream(header []byte, body io.Reader) (int64, error) {
	p.writeLock.Lock()
	defer p.writeLock.Unlock()

	// Writes go to the embedded connection directly: the lock is already
	// held, and Send and Write would try to take it again.
	if _, err := p.Conn.Write(header); err != nil {
		return 0, err
	}
	if _, err := p.Conn.Write([]byte{IncomingStream}); err != nil {
		return 0, err
	}
	return io.Copy(p.Conn, body)
}

func (p *TCPPeer) CloseStream() {
	p.wg.Done()
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		Conn:     conn,
		outbound: outbound,
		wg:       &sync.WaitGroup{},
	}
}

type TCPTransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakeFunc
	Decoder       Decoder
	OnPeer        func(Peer) error

	// OnPeerDisconnect is called once for every peer that completed OnPeer,
	// when its connection ends.
	OnPeerDisconnect func(Peer)
}

type TCPTransport struct {
	TCPTransportOpts
	listener  net.Listener
	boundAddr string
	rpcChan   chan RPC
}

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcChan:          make(chan RPC, 1024),
	}
}
