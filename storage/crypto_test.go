package storage

import (
	"bytes"
	"crypto/rand"
	"io"
	"strings"
	"testing"
)

func mustKey(t *testing.T) []byte {
	t.Helper()
	key, err := NewEncryptionKey()
	if err != nil {
		t.Fatalf("newEncryptionKey: %v", err)
	}
	return key
}

func TestNewEncryptionKey(t *testing.T) {
	k1 := mustKey(t)
	if len(k1) != 32 {
		t.Fatalf("key length = %d, want 32", len(k1))
	}
	if bytes.Equal(k1, make([]byte, 32)) {
		t.Fatal("key is all zeroes")
	}
	if k2 := mustKey(t); bytes.Equal(k1, k2) {
		t.Fatal("two successive keys are identical")
	}
}

func TestCopyEncryptDecryptRoundTrip(t *testing.T) {
	// Sizes either side of the 32KiB copy buffer, so the multi-read path in
	// copyStream is exercised as well as the single-read path.
	sizes := []int{0, 1, 15, 16, 17, 1024, 32 * 1024, 32*1024 + 1, 200 * 1024}

	for _, size := range sizes {
		key := mustKey(t)
		plaintext := make([]byte, size)
		if _, err := io.ReadFull(rand.Reader, plaintext); err != nil {
			t.Fatalf("generating plaintext: %v", err)
		}

		var ciphertext bytes.Buffer
		n, err := copyEncrypt(key, bytes.NewReader(plaintext), &ciphertext)
		if err != nil {
			t.Fatalf("size %d: copyEncrypt: %v", size, err)
		}

		// The encrypted stream is the IV followed by the ciphertext, and the
		// reported count must match what actually landed in the buffer. The
		// server relies on this to derive the plaintext size.
		if want := size + IVSize; n != want {
			t.Errorf("size %d: copyEncrypt returned %d, want %d", size, n, want)
		}
		if ciphertext.Len() != n {
			t.Errorf("size %d: wrote %d bytes but reported %d", size, ciphertext.Len(), n)
		}

		var decrypted bytes.Buffer
		dn, err := copyDecrypt(key, bytes.NewReader(ciphertext.Bytes()), &decrypted)
		if err != nil {
			t.Fatalf("size %d: copyDecrypt: %v", size, err)
		}
		if dn != size {
			t.Errorf("size %d: copyDecrypt returned %d, want %d", size, dn, size)
		}
		if !bytes.Equal(decrypted.Bytes(), plaintext) {
			t.Errorf("size %d: round trip did not preserve contents", size)
		}
	}
}

func TestCopyEncryptDoesNotEmitPlaintext(t *testing.T) {
	key := mustKey(t)
	plaintext := bytes.Repeat([]byte("sensitive"), 512)

	var ciphertext bytes.Buffer
	if _, err := copyEncrypt(key, bytes.NewReader(plaintext), &ciphertext); err != nil {
		t.Fatalf("copyEncrypt: %v", err)
	}
	if bytes.Contains(ciphertext.Bytes(), []byte("sensitive")) {
		t.Fatal("ciphertext contains plaintext")
	}
}

func TestCopyEncryptUsesFreshIV(t *testing.T) {
	key := mustKey(t)
	plaintext := []byte("the same message every time")

	var first, second bytes.Buffer
	if _, err := copyEncrypt(key, bytes.NewReader(plaintext), &first); err != nil {
		t.Fatalf("copyEncrypt: %v", err)
	}
	if _, err := copyEncrypt(key, bytes.NewReader(plaintext), &second); err != nil {
		t.Fatalf("copyEncrypt: %v", err)
	}

	// Reusing an IV under CTR leaks the XOR of the two plaintexts.
	if bytes.Equal(first.Bytes()[:IVSize], second.Bytes()[:IVSize]) {
		t.Fatal("IV reused across encryptions")
	}
	if bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("identical plaintext produced identical ciphertext")
	}
}

func TestCopyDecryptWrongKeyDoesNotReturnPlaintext(t *testing.T) {
	plaintext := []byte("attack at dawn")

	var ciphertext bytes.Buffer
	if _, err := copyEncrypt(mustKey(t), bytes.NewReader(plaintext), &ciphertext); err != nil {
		t.Fatalf("copyEncrypt: %v", err)
	}

	// CTR mode has no authentication tag, so decrypting under the wrong key
	// succeeds and yields garbage rather than an error. This test pins that
	// behaviour so it is a deliberate, documented property.
	var decrypted bytes.Buffer
	if _, err := copyDecrypt(mustKey(t), bytes.NewReader(ciphertext.Bytes()), &decrypted); err != nil {
		t.Fatalf("copyDecrypt: %v", err)
	}
	if bytes.Equal(decrypted.Bytes(), plaintext) {
		t.Fatal("wrong key recovered the plaintext")
	}
}

// shortReader returns one byte at a time, the way a slow network connection
// does. copyDecrypt must not mistake a partial read for a complete IV.
type shortReader struct{ data []byte }

func (r *shortReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	return 1, nil
}

func TestCopyDecryptHandlesShortReads(t *testing.T) {
	key := mustKey(t)
	plaintext := bytes.Repeat([]byte("chunked"), 100)

	var ciphertext bytes.Buffer
	if _, err := copyEncrypt(key, bytes.NewReader(plaintext), &ciphertext); err != nil {
		t.Fatalf("copyEncrypt: %v", err)
	}

	var decrypted bytes.Buffer
	if _, err := copyDecrypt(key, &shortReader{data: ciphertext.Bytes()}, &decrypted); err != nil {
		t.Fatalf("copyDecrypt: %v", err)
	}
	if !bytes.Equal(decrypted.Bytes(), plaintext) {
		t.Fatal("short reads corrupted the plaintext")
	}
}

func TestCopyDecryptTruncatedIV(t *testing.T) {
	// Fewer bytes than an IV must be a reported error, not a silent
	// decryption under a partially-zeroed IV.
	var out bytes.Buffer
	_, err := copyDecrypt(mustKey(t), bytes.NewReader(make([]byte, IVSize-1)), &out)
	if err == nil {
		t.Fatal("expected an error for a truncated IV, got nil")
	}
	if !strings.Contains(err.Error(), "iv") {
		t.Errorf("error %q does not mention the iv", err)
	}
}

func TestCopyEncryptRejectsBadKeyLength(t *testing.T) {
	var out bytes.Buffer
	if _, err := copyEncrypt([]byte("too short"), bytes.NewReader(nil), &out); err == nil {
		t.Fatal("expected an error for an invalid key length, got nil")
	}
}

func TestContentKeyIdentifiesContents(t *testing.T) {
	payload := []byte("the same bytes")

	// The whole point of content addressing: identity follows the bytes, not
	// the name they happen to be stored under.
	if ContentKey(payload) != ContentKey([]byte("the same bytes")) {
		t.Fatal("identical contents produced different keys")
	}
	if ContentKey(payload) == ContentKey([]byte("different bytes")) {
		t.Fatal("different contents produced the same key")
	}
	if len(ContentKey(payload)) != DigestSize {
		t.Fatalf("contentKey length = %d, want %d hex chars", len(ContentKey(payload)), DigestSize)
	}

	// A known vector, so a change of hash function is caught rather than
	// silently accepted.
	const wantEmpty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got := ContentKey(nil); got != wantEmpty {
		t.Errorf("contentKey of empty input = %s, want the SHA-256 of the empty string %s", got, wantEmpty)
	}
}

func TestDigesterMatchesContentKey(t *testing.T) {
	payload := []byte("streamed through a digester")

	dg := newDigester()
	if _, err := io.Copy(io.Discard, dg.tee(bytes.NewReader(payload))); err != nil {
		t.Fatalf("copy: %v", err)
	}

	// The streaming digest and the one-shot digest must agree, or a file
	// would fail verification against its own recorded hash.
	if dg.sum() != ContentKey(payload) {
		t.Errorf("digester gave %s, contentKey gave %s", dg.sum(), ContentKey(payload))
	}
}
