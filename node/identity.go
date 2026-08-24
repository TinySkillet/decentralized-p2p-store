package node

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
)

// handshakeDomain separates handshake signatures from every other thing this
// node might ever sign, so a signature made for one purpose cannot be
// presented as evidence for another.
const handshakeDomain = "p2pstorage/handshake/v1"

// challengeSize is the length of the random value a node asks its peer to sign.
const challengeSize = 32

// Identity is a node's cryptographic identity.
//
// A node is named by its public key rather than by a random string, so a claim
// to be a particular node can be checked instead of taken on trust.
type Identity struct {
	priv ed25519.PrivateKey
}

// newIdentity generates a fresh identity.
func newIdentity() (Identity, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Identity{}, fmt.Errorf("generating identity: %w", err)
	}
	return Identity{priv: priv}, nil
}

// identityFromKey reconstructs an identity from its stored private key.
func identityFromKey(key []byte) (Identity, error) {
	if len(key) != ed25519.PrivateKeySize {
		return Identity{}, fmt.Errorf("identity key is %d bytes, want %d", len(key), ed25519.PrivateKeySize)
	}
	return Identity{priv: ed25519.PrivateKey(key)}, nil
}

// PrivateKey returns the bytes to persist.
func (i Identity) PrivateKey() []byte { return i.priv }

// PublicKey returns the key peers verify signatures against.
func (i Identity) PublicKey() []byte {
	return i.priv.Public().(ed25519.PublicKey)
}

// NodeID is the hex-encoded public key, and is how peers refer to this node.
func (i Identity) NodeID() string {
	return hex.EncodeToString(i.PublicKey())
}

// Sign returns a signature over msg.
func (i Identity) Sign(msg []byte) []byte {
	return ed25519.Sign(i.priv, msg)
}

// Valid reports whether the identity holds usable key material.
func (i Identity) Valid() bool { return len(i.priv) == ed25519.PrivateKeySize }

// publicKeyForNode decodes a node id back into the key that verifies its
// signatures.
func publicKeyForNode(nodeID string) (ed25519.PublicKey, error) {
	raw, err := hex.DecodeString(nodeID)
	if err != nil {
		return nil, fmt.Errorf("node id is not hex: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("node id decodes to %d bytes, want %d", len(raw), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(raw), nil
}

// verifyByNode checks a signature made by the node with this id.
func verifyByNode(nodeID string, msg, sig []byte) bool {
	pub, err := publicKeyForNode(nodeID)
	if err != nil {
		return false
	}
	return ed25519.Verify(pub, msg, sig)
}

// newChallenge returns a fresh random value for a peer to sign.
func newChallenge() ([]byte, error) {
	buf := make([]byte, challengeSize)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return nil, fmt.Errorf("generating challenge: %w", err)
	}
	return buf, nil
}

// handshakeTranscript is the exact bytes a node signs to prove it holds the
// private key for signerPub.
//
// Both public keys and the verifier's challenge are included: the challenge
// makes a captured signature useless on another connection, and naming both
// parties stops a signature made towards one peer being replayed towards
// another.
func handshakeTranscript(signerPub, peerPub, challenge []byte) []byte {
	msg := make([]byte, 0, len(handshakeDomain)+len(signerPub)+len(peerPub)+len(challenge))
	msg = append(msg, handshakeDomain...)
	msg = append(msg, signerPub...)
	msg = append(msg, peerPub...)
	msg = append(msg, challenge...)
	return msg
}

// deleteDomain separates deletion authorisations from handshake proofs, so a
// signature obtained in one exchange cannot be presented as the other.
const deleteDomain = "p2pstorage/delete/v1"

// deleteTranscript is the exact bytes an owner signs to authorise removing a
// file.
//
// Both the name and the contents are named, so an authorisation to delete one
// file cannot be replayed against another. A replay of the same authorisation
// is harmless: deletion is idempotent and already recorded as a tombstone.
func deleteTranscript(name, digest string) []byte {
	msg := make([]byte, 0, len(deleteDomain)+len(name)+len(digest)+2)
	msg = append(msg, deleteDomain...)
	msg = append(msg, 0)
	msg = append(msg, name...)
	msg = append(msg, 0)
	msg = append(msg, digest...)
	return msg
}
