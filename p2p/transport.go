package p2p

import "io"

// Peer is one connected node.
//
// It is deliberately not a net.Conn. Nothing here needs addresses from the
// connection itself, and requiring them would exclude transports whose
// connections have none: a libp2p stream, for instance, has Read, Write,
// Close and deadlines, but no RemoteAddr. Where an address genuinely is
// needed, Located provides it.
type Peer interface {
	io.ReadWriteCloser

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

// Located is implemented by peers whose transport knows a network address for
// them.
//
// It is separate from Peer because addressing is a property of the transport,
// not of a connected node: identity is what the rest of the system reasons
// about. The per-host admission limit is the one place that genuinely needs
// an address, and it asks for this capability rather than assuming it.
type Located interface {
	// RemoteHost returns the host the connection arrived from, or "" when the
	// transport cannot say.
	RemoteHost() string

	// AdvertisedAddrs returns addresses other nodes can use to reach this
	// peer, in whatever form this transport's Dial accepts.
	AdvertisedAddrs() []string
}

type Transport interface {
	Address() string
	Dial(string) error
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
}
