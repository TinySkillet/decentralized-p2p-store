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

// Addr names a peer to dial: who it is, and where it might be reached.
//
// Identity and location are separate on purpose. A node keeps its identity as
// it moves between networks, so the same peer may be reachable at several
// addresses, and an address alone says nothing about who will answer.
type Addr struct {
	// NodeID is the identity expected to answer, or "" when it is not known
	// — a configured bootstrap address is just a location. When set, a
	// connection that handshakes as anyone else is dropped before it is
	// registered.
	NodeID string

	// Addrs are candidate locations for the peer, tried in order until one
	// connects, in whatever form this transport accepts.
	Addrs []string
}

// Discoverer is implemented by transports that can find peers on the local
// network without being given any address.
//
// A capability interface like Located, and for the same reason: discovery is
// a property of the transport, not of the node. The custom TCP transport has
// no discovery; asking for the capability keeps that a checkable fact rather
// than a silent no-op.
type Discoverer interface {
	// Discover starts announcing this node on the local network and calls
	// found for every peer noticed there, with its node id and addresses in
	// whatever form this transport's Dial accepts. It requires the transport
	// to be listening already, announces until the transport closes, and may
	// report the same peer repeatedly. found must not block.
	Discover(found func(nodeID string, addrs []string)) error
}

type Transport interface {
	Address() string
	Dial(Addr) error
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
}
