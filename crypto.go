package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
)

// ivSize is the length of the initialisation vector prefixed to every
// encrypted file on disk. It equals the AES block size.
const ivSize = aes.BlockSize

// newEncryptionKey returns a fresh 256 bit AES key.
//
// An error from the system RNG must not be ignored: silently falling back to
// the zero value would produce an all-zero key and encrypt every file with it.
func newEncryptionKey() ([]byte, error) {
	keyBuf := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, keyBuf); err != nil {
		return nil, fmt.Errorf("generating encryption key: %w", err)
	}
	return keyBuf, nil
}

// nameKey derives a stable row identifier for a file name.
//
// This is an internal identifier, not a content address: the contents are
// identified by their own SHA-256 digest. SHA-256 is used here too so the
// codebase has a single hash function rather than a weaker second one.
func nameKey(name string) string {
	return contentKey([]byte(name))
}

// copyDecrypt reads an IV-prefixed ciphertext from src and writes the
// plaintext to dest. It returns the number of plaintext bytes written.
func copyDecrypt(key []byte, src io.Reader, dest io.Writer) (int, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return 0, err
	}

	// io.ReadFull, not Read: a short read would leave the tail of the IV
	// zeroed and decrypt the whole file to garbage without reporting an error.
	iv := make([]byte, block.BlockSize())
	if _, err := io.ReadFull(src, iv); err != nil {
		return 0, fmt.Errorf("reading iv: %w", err)
	}

	stream := cipher.NewCTR(block, iv)
	return copyStream(stream, 0, src, dest)
}

// copyEncrypt writes a random IV followed by the ciphertext of src to dest.
// It returns the total number of bytes written, including the IV.
func copyEncrypt(key []byte, src io.Reader, dest io.Writer) (int, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return 0, err
	}

	iv := make([]byte, block.BlockSize())
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return 0, fmt.Errorf("generating iv: %w", err)
	}

	if _, err := dest.Write(iv); err != nil {
		return 0, err
	}

	stream := cipher.NewCTR(block, iv)
	return copyStream(stream, block.BlockSize(), src, dest)
}

// copyStream applies stream to src and writes the result to dest. written is
// the number of bytes already emitted to dest by the caller (the IV, when
// encrypting) and is included in the returned count.
func copyStream(stream cipher.Stream, written int, src io.Reader, dest io.Writer) (int, error) {
	buf := make([]byte, 32*1024)
	nw := written
	for {
		n, err := src.Read(buf)
		if n > 0 {
			stream.XORKeyStream(buf[:n], buf[:n])
			c, err := dest.Write(buf[:n])
			if err != nil {
				return nw, err
			}
			nw += c
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nw, err
		}
	}
	return nw, nil
}

// digestSize is the hex-encoded length of a SHA-256 digest.
const digestSize = sha256.Size * 2

// newNodeID returns a fresh random node identifier.
func newNodeID() (string, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return "", fmt.Errorf("generating node id: %w", err)
	}
	return hex.EncodeToString(buf), nil
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

// contentKey returns the hex SHA-256 of b, the identifier a file is stored
// and requested under.
func contentKey(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// digester accumulates a SHA-256 digest of everything read through it.
type digester struct {
	hash hash.Hash
}

func newDigester() *digester {
	return &digester{hash: sha256.New()}
}

// tee wraps r so that everything read from it also feeds the digest.
func (d *digester) tee(r io.Reader) io.Reader {
	return io.TeeReader(r, d.hash)
}

// sum returns the hex-encoded digest of everything seen so far.
func (d *digester) sum() string {
	return hex.EncodeToString(d.hash.Sum(nil))
}
