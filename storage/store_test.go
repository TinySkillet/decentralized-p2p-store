package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	return NewStore(StoreOpts{
		Root:              t.TempDir(),
		PathTransformFunc: CASPathTransformFunc,
	})
}

func TestCASPathTransformFunc(t *testing.T) {
	// The key is already a content digest, so the path shards it rather than
	// hashing it again: a digest of a1b2c... becomes a1b2c/....
	digest := ContentKey([]byte("some file contents"))

	pathKey := CASPathTransformFunc(digest)
	if pathKey.Filename != digest {
		t.Errorf("Filename = %q, want the digest %q", pathKey.Filename, digest)
	}

	dirs := strings.Split(pathKey.Pathname, string(filepath.Separator))
	if want := DigestSize / 5; len(dirs) != want {
		t.Errorf("got %d path segments, want %d", len(dirs), want)
	}
	for _, d := range dirs {
		if len(d) != 5 {
			t.Errorf("path segment %q has length %d, want 5", d, len(d))
		}
	}
	if !strings.HasPrefix(pathKey.FullPath(), digest[:5]) {
		t.Errorf("path %q does not begin with the digest prefix %q", pathKey.FullPath(), digest[:5])
	}

	if CASPathTransformFunc("a") == CASPathTransformFunc("b") {
		t.Error("distinct keys produced the same path")
	}
	if CASPathTransformFunc("a") != CASPathTransformFunc("a") {
		t.Error("transform is not deterministic")
	}
}

func TestCASPathTransformFuncHashesNonDigestKeys(t *testing.T) {
	// A key that is not a digest still has to produce a valid sharded path.
	pathKey := CASPathTransformFunc("just-a-name")

	if len(pathKey.Filename) != DigestSize {
		t.Errorf("Filename = %q, want a %d character digest", pathKey.Filename, DigestSize)
	}
	if !IsDigest(pathKey.Filename) {
		t.Errorf("Filename %q is not a hex digest", pathKey.Filename)
	}
}

func TestIsDigest(t *testing.T) {
	if !IsDigest(ContentKey([]byte("x"))) {
		t.Error("a real digest was not recognised")
	}
	for _, bad := range []string{"", "short", strings.Repeat("z", DigestSize), strings.Repeat("a", DigestSize-1), strings.ToUpper(ContentKey([]byte("x")))} {
		if IsDigest(bad) {
			t.Errorf("IsDigest(%q) = true, want false", bad)
		}
	}
}

// openFDCount reports how many file descriptors this process holds.
func openFDCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot count open descriptors on this platform: %v", err)
	}
	return len(entries)
}

// errReader fails after the readers before it are drained.
type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }

// mustWrite stores payload and returns the digest it landed under.
func mustWrite(t *testing.T, s *Store, key []byte, payload []byte) string {
	t.Helper()
	digest, size, err := s.WriteContent(key, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("WriteContent: %v", err)
	}
	if size != int64(len(payload)) {
		t.Fatalf("WriteContent reported %d bytes, want %d", size, len(payload))
	}
	return digest
}

func TestWriteContentIsAddressedByItsContents(t *testing.T) {
	s := newTestStore(t)
	key := mustKey(t)
	payload := bytes.Repeat([]byte("addressed by content "), 500)

	digest := mustWrite(t, s, key, payload)
	if digest != ContentKey(payload) {
		t.Errorf("digest = %q, want the SHA-256 of the contents", digest)
	}
	if !s.Has(digest) {
		t.Error("the contents are not readable under their digest")
	}

	// The file on disk must be ciphertext, and its size the plaintext plus one IV.
	onDisk, err := os.ReadFile(s.FullPathForKey(digest))
	if err != nil {
		t.Fatalf("reading the stored file: %v", err)
	}
	if bytes.Contains(onDisk, []byte("addressed by content")) {
		t.Error("plaintext found on disk")
	}
	if want := len(payload) + IVSize; len(onDisk) != want {
		t.Errorf("stored %d bytes, want %d", len(onDisk), want)
	}

	size, r, err := s.ReadDecrypt(key, digest)
	if err != nil {
		t.Fatalf("ReadDecrypt: %v", err)
	}
	if size != int64(len(payload)) {
		t.Errorf("ReadDecrypt size = %d, want %d", size, len(payload))
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("the round trip did not preserve the contents")
	}
}

func TestWriteContentStoresIdenticalBytesOnce(t *testing.T) {
	s := newTestStore(t)
	key := mustKey(t)
	payload := []byte("stored twice, kept once")

	first := mustWrite(t, s, key, payload)
	second := mustWrite(t, s, key, payload)

	if first != second {
		t.Errorf("identical contents gave different digests: %s and %s", Short(first), Short(second))
	}
	if n := countStoredFiles(t, s.Root); n != 1 {
		t.Errorf("%d files on disk, want 1", n)
	}
}

func TestStoreHas(t *testing.T) {
	s := newTestStore(t)

	if s.Has(ContentKey([]byte("never stored"))) {
		t.Error("Has reported contents that were never written")
	}
	if digest := mustWrite(t, s, mustKey(t), []byte("x")); !s.Has(digest) {
		t.Error("Has did not report contents that were written")
	}
}

// TestStoreHasReportsUnreadableContentAsAbsent is a regression test: reporting
// something we cannot stat as present makes callers attempt reads that must fail.
func TestStoreHasReportsUnreadableContentAsAbsent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission semantics differ on windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("running as root, permission bits are not enforced")
	}

	s := newTestStore(t)
	digest := mustWrite(t, s, mustKey(t), []byte("blocked"))

	dir := filepath.Dir(s.FullPathForKey(digest))
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, dirPerm) })

	if s.Has(digest) {
		t.Error("Has reported contents it cannot stat as present")
	}
}

// TestDeleteLeavesUnrelatedContents is a regression test. Delete used to
// remove the whole top-level prefix directory, destroying every other file
// whose path began with the same characters.
func TestDeleteLeavesUnrelatedContents(t *testing.T) {
	// A transform that files everything under one shared prefix, which is what
	// a hash prefix collision looks like.
	sharedPrefix := func(key string) PathKey {
		return PathKey{Pathname: filepath.Join("abcde", "fghij"), Filename: key}
	}
	s := NewStore(StoreOpts{Root: t.TempDir(), PathTransformFunc: sharedPrefix})
	key := mustKey(t)

	victim := mustWrite(t, s, key, []byte("delete me"))
	bystander := mustWrite(t, s, key, []byte("keep me"))

	if err := s.Delete(victim); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if s.Has(victim) {
		t.Error("the deleted contents survived")
	}
	if !s.Has(bystander) {
		t.Fatal("deleting one file destroyed an unrelated file sharing its prefix")
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	s := newTestStore(t)

	// Deletions are broadcast to peers that may never have held the file, so
	// deleting something absent must succeed rather than fail the request.
	if err := s.Delete(ContentKey([]byte("never stored"))); err != nil {
		t.Errorf("deleting absent contents returned %v, want nil", err)
	}

	digest := mustWrite(t, s, mustKey(t), []byte("x"))
	for i := range 2 {
		if err := s.Delete(digest); err != nil {
			t.Errorf("Delete call %d returned %v, want nil", i+1, err)
		}
	}
}

func TestDeletePrunesEmptyDirectories(t *testing.T) {
	s := newTestStore(t)

	digest := mustWrite(t, s, mustKey(t), []byte("lonely"))
	if err := s.Delete(digest); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	entries, err := os.ReadDir(s.Root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("the root still holds %d entries after deleting the only file", len(entries))
	}
	// Pruning must stop at the root and never remove it.
	if _, err := os.Stat(s.Root); err != nil {
		t.Errorf("the store root was removed: %v", err)
	}
}

func TestReadMissingContents(t *testing.T) {
	s := newTestStore(t)
	absent := ContentKey([]byte("nope"))

	if _, _, err := s.Read(absent); err == nil {
		t.Error("expected an error reading missing contents, got nil")
	}
	if _, _, err := s.ReadDecrypt(mustKey(t), absent); err == nil {
		t.Error("expected an error decrypting missing contents, got nil")
	}
}

// TestWriteIsAtomicForReaders checks the rename: a reader that has opened the
// contents must not see them replaced underneath it.
func TestWriteIsAtomicForReaders(t *testing.T) {
	s := newTestStore(t)
	key := mustKey(t)
	payload := bytes.Repeat([]byte("in flight"), 4096)

	digest := mustWrite(t, s, key, payload)

	_, reader, err := s.ReadDecrypt(key, digest)
	if err != nil {
		t.Fatalf("ReadDecrypt: %v", err)
	}

	// The same contents written again while the reader holds the old file.
	if _, _, err := s.WriteContent(key, bytes.NewReader(payload)); err != nil {
		t.Fatalf("second WriteContent: %v", err)
	}

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("a write during a read disturbed the reader")
	}
}

// TestFailedWriteLeavesNothingBehind pins that an interrupted transfer neither
// becomes readable nor leaves a partial file for the sweep to trip over.
func TestFailedWriteLeavesNothingBehind(t *testing.T) {
	s := newTestStore(t)
	key := mustKey(t)

	existing := mustWrite(t, s, key, []byte("already here"))

	failing := io.MultiReader(
		bytes.NewReader(bytes.Repeat([]byte("partial"), 1000)),
		&errReader{err: errors.New("connection reset")},
	)
	if _, _, err := s.WriteContent(key, failing); err == nil {
		t.Fatal("a failed write reported success")
	}

	if n := countStoredFiles(t, s.Root); n != 1 {
		t.Errorf("%d files on disk, want only the one already stored", n)
	}
	if !s.Has(existing) {
		t.Error("a failed write destroyed unrelated contents")
	}
}

// TestWriteContentExpectingRejectsMismatch covers the guarantee the fetch
// protocol relies on: contents that do not hash to what was asked for never
// become readable.
func TestWriteContentExpectingRejectsMismatch(t *testing.T) {
	s := newTestStore(t)
	key := mustKey(t)

	actual := []byte("the real bytes")
	wrong := ContentKey([]byte("something else entirely"))

	if _, err := s.WriteContentExpecting(key, wrong, bytes.NewReader(actual)); err == nil {
		t.Fatal("contents that did not match the announced digest were accepted")
	}
	if s.Has(ContentKey(actual)) {
		t.Error("the rejected contents were stored anyway")
	}
	if n := countStoredFiles(t, s.Root); n != 0 {
		t.Errorf("%d files left after a rejected write, want 0", n)
	}

	if _, err := s.WriteContentExpecting(key, ContentKey(actual), bytes.NewReader(actual)); err != nil {
		t.Fatalf("WriteContentExpecting: %v", err)
	}
	if !s.Has(ContentKey(actual)) {
		t.Error("matching contents were not stored")
	}
}

// TestWritesDoNotLeakDescriptors is a regression test: the write path used to
// return without closing its file, exhausting the process descriptor limit.
func TestWritesDoNotLeakDescriptors(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("descriptor counting relies on /proc")
	}
	s := newTestStore(t)
	key := mustKey(t)

	mustWrite(t, s, key, []byte("warmup"))

	before := openFDCount(t)
	for i := range 100 {
		if _, _, err := s.WriteContent(key, bytes.NewReader([]byte(fmt.Sprintf("payload %d", i)))); err != nil {
			t.Fatalf("WriteContent: %v", err)
		}
	}
	if after := openFDCount(t); after-before > 5 {
		t.Errorf("open descriptors grew from %d to %d across 100 writes", before, after)
	}
}

// countStoredFiles counts the regular files under root.
func countStoredFiles(t *testing.T, root string) int {
	t.Helper()
	n := 0
	if err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	}); err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return n
}
