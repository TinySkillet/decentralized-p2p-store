// Package storage holds content-addressed file storage and the encryption
// applied to it at rest.
//
// A file is identified by the SHA-256 of its own contents, so identical bytes
// are stored once however many names refer to them, and a transfer can be
// verified against what was asked for.
package storage

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

// IVSize is the length of the initialisation vector prefixed to every
// encrypted file on disk. It equals the AES block size.
const IVSize = aes.BlockSize

// NewEncryptionKey returns a fresh 256 bit AES key.
//
// An error from the system RNG must not be ignored: silently falling back to
// the zero value would produce an all-zero key and encrypt every file with it.
func NewEncryptionKey() ([]byte, error) {
	keyBuf := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, keyBuf); err != nil {
		return nil, fmt.Errorf("generating encryption key: %w", err)
	}
	return keyBuf, nil
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

// DigestSize is the hex-encoded length of a SHA-256 digest.
const DigestSize = sha256.Size * 2

// ContentKey returns the hex SHA-256 of b, the identifier a file is stored
// and requested under.
func ContentKey(b []byte) string {
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

// Short abbreviates a digest for log output and for tables, where a full
// 64 character hash would be unreadable.
func Short(digest string) string {
	if len(digest) > 12 {
		return digest[:12]
	}
	return digest
}
