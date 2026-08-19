package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const DEFAULT_ROOT_FOLDER = "p2pnetwork"

const (
	// dirPerm and filePerm keep stored data readable only by the node's own
	// user. Files on disk are encrypted, but the key lives in the sibling
	// SQLite database, so the directory should not be world readable.
	dirPerm  os.FileMode = 0o700
	filePerm os.FileMode = 0o600
)

// CASPathTransformFunc shards a content digest into a nested directory path.
//
// The key is already the hex SHA-256 of the file's contents, so it is split
// rather than hashed again: a digest of a1b2c3d4e5... becomes
// ROOT/a1b2c/3d4e5/a1b2c3d4e5..., spreading files across many directories so
// no single one grows large enough to slow lookups down.
//
// Because the path is derived from the contents, two files with identical
// bytes land on the same path and are stored once, whatever they are named.
func CASPathTransformFunc(key string) PathKey {
	const blockSize = 5

	// A key that is not a digest (an unhashed name, in tests or older data)
	// is hashed first, so every key still produces a valid sharded path.
	if !isDigest(key) {
		key = contentKey([]byte(key))
	}

	sliceLen := len(key) / blockSize
	paths := make([]string, sliceLen)
	for i := range sliceLen {
		from, to := i*blockSize, (i+1)*blockSize
		paths[i] = key[from:to]
	}

	return PathKey{
		Pathname: filepath.Join(paths...),
		Filename: key,
	}
}

// isDigest reports whether key is a hex-encoded SHA-256 digest.
func isDigest(key string) bool {
	if len(key) != digestSize {
		return false
	}
	for i := range len(key) {
		c := key[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (s *Store) Read(key string) (int64, io.ReadCloser, error) {
	return s.readStream(key)
}

func (s *Store) Write(key string, r io.Reader) (int64, error) {
	return s.writeStream(key, r)
}

func (s *Store) WriteEncrypt(encryptionKey []byte, key string, r io.Reader) (int64, error) {
	return s.atomicWrite(key, func(w io.Writer) (int64, error) {
		n, err := copyEncrypt(encryptionKey, r, w)
		return int64(n), err
	})
}

// WriteContent encrypts r to disk and files it under the digest of its own
// contents, which it returns along with the plaintext size.
//
// The digest is only known once the whole stream has been read, so the data
// is written to a temporary file and moved into place afterwards. Identical
// contents resolve to the same destination and are therefore stored once.
func (s *Store) WriteContent(encryptionKey []byte, r io.Reader) (digest string, size int64, err error) {
	return s.writeContent(encryptionKey, "", r)
}

// WriteContentExpecting is WriteContent for data received from a peer, which
// announced in advance what it was about to send.
//
// Contents that do not hash to want are rejected before they are moved into
// place, so a file that fails verification never becomes readable at all.
func (s *Store) WriteContentExpecting(encryptionKey []byte, want string, r io.Reader) (size int64, err error) {
	if want == "" {
		return 0, fmt.Errorf("no digest to verify against")
	}
	_, size, err = s.writeContent(encryptionKey, want, r)
	return size, err
}

func (s *Store) writeContent(encryptionKey []byte, want string, r io.Reader) (digest string, size int64, err error) {
	if err := os.MkdirAll(s.Root, dirPerm); err != nil {
		return "", 0, err
	}

	tmp, err := os.CreateTemp(s.Root, ".incoming-*")
	if err != nil {
		return "", 0, err
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName)
	}()

	if err := tmp.Chmod(filePerm); err != nil {
		return "", 0, err
	}

	dg := newDigester()
	written, err := copyEncrypt(encryptionKey, dg.tee(r), tmp)
	if err != nil {
		return "", 0, err
	}

	digest = dg.sum()
	size = int64(written) - ivSize

	if want != "" && digest != want {
		return "", 0, fmt.Errorf("content digest %s does not match the announced %s", digest, want)
	}

	if err := tmp.Sync(); err != nil {
		return "", 0, err
	}
	if err := tmp.Close(); err != nil {
		return "", 0, err
	}

	pathKey := s.PathTransformFunc(digest)
	dir := filepath.Join(s.Root, pathKey.Pathname)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return "", 0, err
	}

	if err := os.Rename(tmpName, filepath.Join(s.Root, pathKey.FullPath())); err != nil {
		return "", 0, err
	}

	return digest, size, nil
}

func (s *Store) ReadDecrypt(encryptionKey []byte, key string) (int64, io.Reader, error) {
	f, err := s.openFileForReading(key)
	if err != nil {
		return 0, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return 0, nil, err
	}

	// The file is an IV followed by the ciphertext, so the plaintext is
	// shorter than the file by exactly one IV.
	plaintextSize := info.Size() - ivSize
	if plaintextSize < 0 {
		f.Close()
		return 0, nil, fmt.Errorf("stored file %q is shorter than an iv", key)
	}

	pr, pw := io.Pipe()

	go func() {
		_, err := copyDecrypt(encryptionKey, f, pw)
		f.Close()
		pw.CloseWithError(err)
	}()

	return plaintextSize, pr, nil
}

func (s *Store) openFileForReading(key string) (*os.File, error) {
	return os.Open(s.FullPathForKey(key))
}

func (s *Store) writeStream(key string, r io.Reader) (int64, error) {
	return s.atomicWrite(key, func(w io.Writer) (int64, error) {
		return io.Copy(w, r)
	})
}

// atomicWrite stores the output of write under key by filling a temporary
// file in the destination directory and renaming it into place.
//
// Writing in place would mean a reader that already opened the file sees it
// truncated underneath them, and an interrupted transfer would leave a short
// file that looks complete. A rename is atomic, so the key either holds the
// previous contents or the new ones, never a mixture.
func (s *Store) atomicWrite(key string, write func(io.Writer) (int64, error)) (int64, error) {
	pathKey := s.PathTransformFunc(key)

	dir := filepath.Join(s.Root, pathKey.Pathname)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return 0, err
	}

	tmp, err := os.CreateTemp(dir, "."+pathKey.Filename+".tmp")
	if err != nil {
		return 0, err
	}
	tmpName := tmp.Name()

	// Both are no-ops once the file has been closed and renamed, and clean up
	// the partial file on every failure path.
	defer func() {
		tmp.Close()
		os.Remove(tmpName)
	}()

	if err := tmp.Chmod(filePerm); err != nil {
		return 0, err
	}

	n, err := write(tmp)
	if err != nil {
		return n, err
	}

	// Flush before the rename, so a crash cannot leave the key pointing at a
	// file whose contents never reached the disk.
	if err := tmp.Sync(); err != nil {
		return n, err
	}
	if err := tmp.Close(); err != nil {
		return n, err
	}

	if err := os.Rename(tmpName, filepath.Join(s.Root, pathKey.FullPath())); err != nil {
		return n, err
	}

	return n, nil
}

// Delete removes the file stored under key. It is idempotent: deleting a key
// that is not present is not an error, which matters because deletions are
// broadcast to peers that may never have held the file.
func (s *Store) Delete(key string) error {
	pathKey := s.PathTransformFunc(key)
	fullPath := filepath.Join(s.Root, pathKey.FullPath())

	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		log.Printf("Failed to delete [%s] from disk: %v\n", pathKey.Filename, err)
		return err
	}

	// Remove the directories that held the file, but only while they are
	// empty. Deleting the whole top-level prefix directory (as an earlier
	// version did) would also destroy unrelated files whose hash happens to
	// share the same leading characters.
	s.pruneEmptyDirs(filepath.Dir(fullPath))

	log.Printf("Deleted [%s] from disk\n", pathKey.Filename)
	return nil
}

// pruneEmptyDirs walks up from dir towards the store root, removing
// directories that have become empty. It stops at the first directory that
// still has entries, which os.Remove reports as an error.
func (s *Store) pruneEmptyDirs(dir string) {
	root := filepath.Clean(s.Root)
	for {
		cleaned := filepath.Clean(dir)
		if cleaned == root || cleaned == "." || cleaned == string(filepath.Separator) {
			return
		}
		if !strings.HasPrefix(cleaned, root+string(filepath.Separator)) {
			return
		}
		if err := os.Remove(cleaned); err != nil {
			return
		}
		dir = filepath.Dir(cleaned)
	}
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

// Has reports whether the key is readable from this store. Any error from the
// filesystem, not just "not found", counts as absent: reporting a file we
// cannot stat as present makes callers attempt reads that are certain to fail.
func (s *Store) Has(key string) bool {
	_, err := os.Stat(s.FullPathForKey(key))
	return err == nil
}

func (s *Store) readStream(key string) (int64, io.ReadCloser, error) {
	fullPath := s.FullPathForKey(key)

	file, err := os.Open(fullPath)
	if err != nil {
		return 0, nil, err
	}

	// Stat the open descriptor rather than the path, so the size cannot
	// belong to a different file than the one being read.
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return 0, nil, err
	}

	return fileInfo.Size(), file, nil
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	Pathname string
	Filename string
}

func (p PathKey) FullPath() string {
	return filepath.Join(p.Pathname, p.Filename)
}

func DefaultPathTransformFunc(key string) PathKey {
	return PathKey{
		Pathname: key,
		Filename: key,
	}
}

type Store struct {
	StoreOpts
}

type StoreOpts struct {
	Root              string
	PathTransformFunc PathTransformFunc
}

func NewStore(opts StoreOpts) *Store {

	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}
	if len(opts.Root) == 0 {
		opts.Root = DEFAULT_ROOT_FOLDER
	}
	return &Store{
		StoreOpts: opts,
	}
}

// FullPathForKey returns absolute path for key storage without filesystem access
func (s *Store) FullPathForKey(key string) string {
	pathKey := s.PathTransformFunc(key)
	return filepath.Join(s.Root, pathKey.FullPath())
}
