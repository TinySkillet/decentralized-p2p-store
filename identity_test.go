package main

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"
)

func mustIdentity(t *testing.T) Identity {
	t.Helper()
	id, err := newIdentity()
	if err != nil {
		t.Fatalf("newIdentity: %v", err)
	}
	return id
}

func TestIdentityIsNamedByItsPublicKey(t *testing.T) {
	id := mustIdentity(t)

	if !id.Valid() {
		t.Fatal("a freshly generated identity reports itself invalid")
	}
	// The node id must be exactly the public key, so a peer can derive the
	// verifying key from the id alone.
	pub, err := publicKeyForNode(id.NodeID())
	if err != nil {
		t.Fatalf("publicKeyForNode: %v", err)
	}
	if !bytes.Equal(pub, id.PublicKey()) {
		t.Error("the node id does not decode back to the public key")
	}

	if other := mustIdentity(t); other.NodeID() == id.NodeID() {
		t.Error("two identities share an id")
	}
}

func TestIdentityRoundTripsThroughItsKey(t *testing.T) {
	id := mustIdentity(t)

	restored, err := identityFromKey(id.PrivateKey())
	if err != nil {
		t.Fatalf("identityFromKey: %v", err)
	}
	if restored.NodeID() != id.NodeID() {
		t.Error("an identity restored from its key has a different id")
	}

	// A signature from the restored identity must verify against the original.
	msg := []byte("something to sign")
	if !verifyByNode(id.NodeID(), msg, restored.Sign(msg)) {
		t.Error("the restored identity cannot sign as itself")
	}
}

func TestIdentityFromKeyRejectsBadInput(t *testing.T) {
	for name, key := range map[string][]byte{
		"empty":     nil,
		"too short": make([]byte, 8),
		"too long":  make([]byte, ed25519.PrivateKeySize+1),
	} {
		if _, err := identityFromKey(key); err == nil {
			t.Errorf("identityFromKey accepted a %s key", name)
		}
	}

	// The zero identity must not be usable for signing.
	if (Identity{}).Valid() {
		t.Error("the zero identity reports itself valid")
	}
}

func TestVerifyByNodeRejectsForgeries(t *testing.T) {
	signer := mustIdentity(t)
	other := mustIdentity(t)
	msg := []byte("authorise this")
	sig := signer.Sign(msg)

	if !verifyByNode(signer.NodeID(), msg, sig) {
		t.Fatal("a genuine signature did not verify")
	}
	if verifyByNode(other.NodeID(), msg, sig) {
		t.Error("a signature verified against the wrong identity")
	}
	if verifyByNode(signer.NodeID(), []byte("authorise something else"), sig) {
		t.Error("a signature verified over different content")
	}
	if verifyByNode(signer.NodeID(), msg, make([]byte, ed25519.SignatureSize)) {
		t.Error("a signature of zeroes verified")
	}
}

func TestPublicKeyForNodeRejectsMalformedIDs(t *testing.T) {
	for name, id := range map[string]string{
		"empty":        "",
		"not hex":      strings.Repeat("z", 64),
		"wrong length": "abcd",
	} {
		if _, err := publicKeyForNode(id); err == nil {
			t.Errorf("publicKeyForNode accepted a %s id", name)
		}
	}
}

func TestNewChallengeIsFreshAndSized(t *testing.T) {
	first, err := newChallenge()
	if err != nil {
		t.Fatalf("newChallenge: %v", err)
	}
	if len(first) != challengeSize {
		t.Fatalf("challenge is %d bytes, want %d", len(first), challengeSize)
	}

	second, err := newChallenge()
	if err != nil {
		t.Fatalf("newChallenge: %v", err)
	}
	// A repeated challenge would make a captured handshake replayable.
	if bytes.Equal(first, second) {
		t.Error("two challenges were identical")
	}
}

// TestHandshakeTranscriptBindsBothPartiesAndChallenge covers what stops a
// captured proof being reused: the signed bytes name who signed, who they
// signed towards, and the specific challenge they answered.
func TestHandshakeTranscriptBindsBothPartiesAndChallenge(t *testing.T) {
	alice := mustIdentity(t)
	bob := mustIdentity(t)
	carol := mustIdentity(t)

	challenge, err := newChallenge()
	if err != nil {
		t.Fatalf("newChallenge: %v", err)
	}
	other, err := newChallenge()
	if err != nil {
		t.Fatalf("newChallenge: %v", err)
	}

	base := handshakeTranscript(alice.PublicKey(), bob.PublicKey(), challenge)

	for name, variant := range map[string][]byte{
		"a different challenge": handshakeTranscript(alice.PublicKey(), bob.PublicKey(), other),
		"a different peer":      handshakeTranscript(alice.PublicKey(), carol.PublicKey(), challenge),
		"a different signer":    handshakeTranscript(carol.PublicKey(), bob.PublicKey(), challenge),
		"the parties swapped":   handshakeTranscript(bob.PublicKey(), alice.PublicKey(), challenge),
	} {
		if bytes.Equal(base, variant) {
			t.Errorf("the transcript does not distinguish %s", name)
		}
	}

	// And it must be stable, or a proof would never verify.
	if !bytes.Equal(base, handshakeTranscript(alice.PublicKey(), bob.PublicKey(), challenge)) {
		t.Error("the transcript is not deterministic")
	}

	// The domain separator keeps handshake proofs out of other contexts.
	if !bytes.HasPrefix(base, []byte(handshakeDomain)) {
		t.Error("the transcript is not domain separated")
	}
}
