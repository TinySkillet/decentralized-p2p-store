package node

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// nameKey derives a stable row identifier for a file name.
//
// This is an internal identifier, not a content address: the contents are
// identified by their own SHA-256 digest. The same hash function is used here
// so the codebase has one rather than a weaker second one.
func nameKey(name string) string {
	return storage.ContentKey([]byte(name))
}

// newRequestID returns an identifier that correlates a reply with the request
// that caused it.
func newRequestID() (string, error) {
	buf := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return "", fmt.Errorf("generating request id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
