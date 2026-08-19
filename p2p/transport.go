package p2p

import (
	"io"
	"net"
)

type Peer interface {
	net.Conn
	Send([]byte) error

	// SendStream writes an announcement frame and the file body that follows
	// it as one indivisible transfer. Sending them as separate writes lets a
	// concurrent transfer interleave between the two, and the receiver then
	// pairs one file's announcement with another file's bytes.
	SendStream(header []byte, body io.Reader) (int64, error)

	CloseStream()

	// ID returns the remote node's identifier, established during the
	// handshake. Addresses change as nodes move between networks, so
	// identity is tracked separately from location.
	ID() string
}

type Transport interface {
	Address() string
	Dial(string) error
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
}
