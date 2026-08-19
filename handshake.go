// The handshake that opens every connection, and the identity it proves.
package main

import (
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"net"

	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

// Handshake is the first thing exchanged on a new connection.
type Handshake struct {
	Version uint32

	// PublicKey names the node. Peers verify its signatures against this, so
	// a claim to be a particular node can be checked rather than trusted.
	PublicKey []byte

	// ListenPort is the port this node accepts connections on. Only the port
	// is sent: the receiver already knows which address the connection came
	// from and pairs the two, so a node configured with a bare ":3000" is
	// still reachable by peers on other machines.
	ListenPort string

	// Challenge is a fresh random value the peer must sign, which is what
	// makes a captured handshake useless on another connection.
	Challenge []byte
}

// HandshakeProof answers a peer's challenge.
type HandshakeProof struct {
	Signature []byte
}

// portSource reports the port this node accepts connections on. It is read at
// handshake time rather than captured up front, so a node configured with
// port 0 advertises the port it actually bound.
type portSource interface {
	Address() string
	BoundAddr() string
}

// localListenPort returns the port peers should use to reach this node.
func localListenPort(src portSource) string {
	for _, candidate := range []string{src.BoundAddr(), src.Address()} {
		if candidate == "" {
			continue
		}
		if _, port, err := net.SplitHostPort(candidate); err == nil && port != "" && port != "0" {
			return port
		}
	}
	return ""
}

// GetHandshakeFunc builds the handshake run on every new connection.
//
// It settles four things before any file traffic is allowed: that both sides
// speak the same protocol version, that the peer holds the private key for the
// identity it claims, that it is not this node reached by a roundabout route,
// and what address other nodes should use to reach it.
//
// The proof of identity matters because everything downstream leans on it. A
// node id used to be an unverified assertion, so a peer could claim to be any
// node it liked; deciding what a peer is allowed to do would have been
// meaningless on top of that.
func GetHandshakeFunc(identity Identity, src portSource) p2p.HandshakeFunc {
	return func(p any) error {
		peer, ok := p.(*p2p.TCPPeer)
		if !ok {
			return fmt.Errorf("invalid peer type for TCP handshake")
		}
		if !identity.Valid() {
			return fmt.Errorf("this node has no identity key")
		}

		// One encoder and decoder for the whole exchange: a second decoder on
		// the same stream could discard bytes the first had buffered.
		enc := gob.NewEncoder(peer)
		dec := gob.NewDecoder(peer)

		challenge, err := newChallenge()
		if err != nil {
			return err
		}

		// 1. Announce who we are and what we want signed.
		if err := enc.Encode(Handshake{
			Version:    p2p.ProtocolVersion,
			PublicKey:  identity.PublicKey(),
			ListenPort: localListenPort(src),
			Challenge:  challenge,
		}); err != nil {
			return err
		}

		var remote Handshake
		if err := dec.Decode(&remote); err != nil {
			return err
		}

		if remote.Version != p2p.ProtocolVersion {
			return fmt.Errorf("protocol version mismatch: peer speaks %d, this node speaks %d",
				remote.Version, p2p.ProtocolVersion)
		}

		remoteID := hex.EncodeToString(remote.PublicKey)
		if _, err := publicKeyForNode(remoteID); err != nil {
			return fmt.Errorf("peer did not present a usable identity: %w", err)
		}
		if remoteID == identity.NodeID() {
			// Gossip hands out addresses, and one of them is eventually our
			// own. Identity is what tells the difference reliably; comparing
			// addresses cannot, because a node has several.
			return fmt.Errorf("refusing to connect to self")
		}
		if len(remote.Challenge) != challengeSize {
			return fmt.Errorf("peer %s sent a %d byte challenge, want %d", remoteID, len(remote.Challenge), challengeSize)
		}
		if remote.ListenPort == "" {
			return fmt.Errorf("peer %s did not advertise a listen port", remoteID)
		}

		// 2. Prove we hold the key for the identity we claimed, and check
		// that they can do the same.
		if err := enc.Encode(HandshakeProof{
			Signature: identity.Sign(handshakeTranscript(identity.PublicKey(), remote.PublicKey, remote.Challenge)),
		}); err != nil {
			return err
		}

		var remoteProof HandshakeProof
		if err := dec.Decode(&remoteProof); err != nil {
			return err
		}

		if !verifyByNode(remoteID, handshakeTranscript(remote.PublicKey, identity.PublicKey(), challenge), remoteProof.Signature) {
			return fmt.Errorf("peer %s could not prove it holds that identity", remoteID)
		}

		// Pair the port the peer advertises with the address the connection
		// actually came from. A peer configured with a bare ":3000" would
		// otherwise hand out an address that resolves to the wrong host.
		host := peer.ObservedHost()
		if host == "" {
			return fmt.Errorf("could not determine the address peer %s connected from", remoteID)
		}

		peer.NodeID = remoteID
		peer.FullAddr = net.JoinHostPort(host, remote.ListenPort)

		fmt.Printf("[%s] Handshake successful with %s at %s\n", src.Address(), short(remoteID), peer.FullAddr)

		return nil
	}
}
