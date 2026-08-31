// Converting between this project's node ids and libp2p's peer ids, and the
// gate that keeps the conversion total.
package libp2pt

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// The hex Ed25519 public key stays the canonical node id everywhere outside
// this package. It is not a style choice: files.owner and deletions.owner
// feed signature verification through hex.DecodeString, so changing the
// canonical form would make every previously stored file undeletable.

// nodeIDForPeer derives the canonical node id from a libp2p peer id.
//
// It only works for Ed25519 peers: a peer id embeds its public key only when
// the key is short enough, and an RSA peer id is a hash from which no key can
// be recovered. ed25519Gate refuses those connections outright, because
// without a recoverable key the peer would connect fine and then fail
// signature checks much later, which reads as data corruption rather than as
// an unsupported peer.
func nodeIDForPeer(pid peer.ID) (string, error) {
	pub, err := pid.ExtractPublicKey()
	if err != nil {
		return "", fmt.Errorf("peer id %s does not embed its public key: %w", pid, err)
	}
	if pub.Type() != crypto.Ed25519 {
		return "", fmt.Errorf("peer %s uses a %s key, only Ed25519 peers are supported", pid, pub.Type())
	}
	raw, err := pub.Raw()
	if err != nil {
		return "", err
	}
	if len(raw) != ed25519.PublicKeySize {
		return "", fmt.Errorf("peer %s key is %d bytes, want %d", pid, len(raw), ed25519.PublicKeySize)
	}
	return hex.EncodeToString(raw), nil
}

// peerIDForNode converts a canonical node id to the peer id libp2p dials.
func peerIDForNode(nodeID string) (peer.ID, error) {
	raw, err := hex.DecodeString(nodeID)
	if err != nil {
		return "", fmt.Errorf("node id is not hex: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return "", fmt.Errorf("node id decodes to %d bytes, want %d", len(raw), ed25519.PublicKeySize)
	}
	pub, err := crypto.UnmarshalEd25519PublicKey(raw)
	if err != nil {
		return "", err
	}
	return peer.IDFromPublicKey(pub)
}

// ed25519Gate refuses connections from peers whose identity this system
// cannot express. Enforced here rather than documented: see nodeIDForPeer.
type ed25519Gate struct{}

var _ connmgr.ConnectionGater = ed25519Gate{}

func (ed25519Gate) InterceptPeerDial(pid peer.ID) bool {
	_, err := nodeIDForPeer(pid)
	return err == nil
}

func (ed25519Gate) InterceptAddrDial(peer.ID, ma.Multiaddr) bool { return true }

func (ed25519Gate) InterceptAccept(network.ConnMultiaddrs) bool { return true }

// InterceptSecured runs once the security handshake has proven the peer id,
// which is the earliest point an inbound peer's key type is known.
func (ed25519Gate) InterceptSecured(_ network.Direction, pid peer.ID, _ network.ConnMultiaddrs) bool {
	_, err := nodeIDForPeer(pid)
	return err == nil
}

func (ed25519Gate) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) {
	return true, 0
}
