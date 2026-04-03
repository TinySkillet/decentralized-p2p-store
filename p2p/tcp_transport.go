package p2p

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

func (t TCPTransport) Close() error {
	return t.listener.Close()
}

func (t *TCPTransport) Address() string {
	return t.ListenAddr
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

	go t.startAcceptLoop()
	return nil
}

func (t *TCPTransport) startAcceptLoop() {
	log.Printf("Listening on TCP at PORT %s\n", t.ListenAddr)
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			fmt.Printf("[%s] TCP accept error: %v\n", t.ListenAddr, err)
		}

		fmt.Printf("[%s] New Incoming Connection: %+v\n", t.ListenAddr, conn.RemoteAddr().String())
		go t.handleConn(conn, false)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	var err error

	defer func() {
		fmt.Printf("[%s] Dropping peer connection: %v\n", t.ListenAddr, err)
		conn.Close()
	}()

	peer := NewTCPPeer(conn, outbound)

	if err = t.HandshakeFunc(peer); err != nil {
		return
	}

	if t.OnPeer != nil {
		if err = t.OnPeer(peer); err != nil {
			return
		}
	}

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

	// FullAddr is the verified listening address of the peer
	FullAddr string
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
	_, err := p.Conn.Write(b)
	return err
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
}

type TCPTransport struct {
	TCPTransportOpts
	listener net.Listener
	rpcChan  chan RPC
}

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcChan:          make(chan RPC, 1024),
	}
}
