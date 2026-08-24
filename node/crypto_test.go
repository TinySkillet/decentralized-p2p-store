package node

import (
	"testing"

	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

func TestNameKeyIsStable(t *testing.T) {
	if nameKey("hello") != nameKey("hello") {
		t.Fatal("nameKey is not deterministic")
	}
	if nameKey("hello") == nameKey("world") {
		t.Fatal("distinct names collided")
	}
	if len(nameKey("hello")) != storage.DigestSize {
		t.Fatalf("nameKey length = %d, want %d hex chars", len(nameKey("hello")), storage.DigestSize)
	}
}

func TestNewRequestIDIsUnique(t *testing.T) {
	// Request ids correlate a reply with the request that caused it, so a
	// repeat would let one request's answer satisfy another.
	seen := make(map[string]bool, 100)
	for range 100 {
		id, err := newRequestID()
		if err != nil {
			t.Fatalf("newRequestID: %v", err)
		}
		if id == "" {
			t.Fatal("newRequestID returned an empty id")
		}
		if seen[id] {
			t.Fatalf("request id %q was issued twice", id)
		}
		seen[id] = true
	}
}
