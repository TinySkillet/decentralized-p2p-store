package main

import (
	"bytes"
	"errors"
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
	pathKey := CASPathTransformFunc("momsbestpicture")

	// sha1 of the key, hex encoded, split into five character directories.
	const wantFilename = "6804429f74181a63c50c3d81d733a12f14a353ff"
	if pathKey.Filename != wantFilename {
		t.Errorf("Filename = %q, want %q", pathKey.Filename, wantFilename)
	}

	dirs := strings.Split(pathKey.Pathname, string(filepath.Separator))
	if len(dirs) != 8 {
		t.Errorf("got %d path segments, want 8", len(dirs))
	}
	for _, d := range dirs {
		if len(d) != 5 {
			t.Errorf("path segment %q has length %d, want 5", d, len(d))
		}
	}

	if CASPathTransformFunc("a") == CASPathTransformFunc("b") {
		t.Error("distinct keys produced the same path")
	}
	if CASPathTransformFunc("a") != CASPathTransformFunc("a") {
		t.Error("transform is not deterministic")
	}
}

func TestStoreWriteReadRoundTrip(t *testing.T) {
	s := newTestStore(t)
	payload := []byte("some jpg bytes")

	n, err := s.Write("picture", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != int64(len(payload)) {
		t.Errorf("Write returned %d, want %d", n, len(payload))
	}

	size, r, err := s.Read("picture")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer r.Close()

	if size != int64(len(payload)) {
		t.Errorf("Read size = %d, want %d", size, len(payload))
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("read %q, want %q", got, payload)
	}
}

func TestStoreWriteEncryptReadDecrypt(t *testing.T) {
	s := newTestStore(t)
	key := mustKey(t)
	payload := bytes.Repeat([]byte("secret payload "), 5000)

	n, err := s.WriteEncrypt(key, "doc", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("WriteEncrypt: %v", err)
	}
	if want := int64(len(payload) + ivSize); n != want {
		t.Errorf("WriteEncrypt returned %d, want %d", n, want)
	}

	// What sits on disk must not be the plaintext.
	onDisk, err := os.ReadFile(s.FullPathForKey("doc"))
	if err != nil {
		t.Fatalf("reading stored file: %v", err)
	}
	if bytes.Contains(onDisk, []byte("secret payload")) {
		t.Error("plaintext found in the file on disk")
	}

	size, r, err := s.ReadDecrypt(key, "doc")
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
		t.Error("decrypted contents differ from the original")
	}
}

func TestStoreHas(t *testing.T) {
	s := newTestStore(t)

	if s.Has("absent") {
		t.Error("Has reported a key that was never written")
	}
	if _, err := s.Write("present", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !s.Has("present") {
		t.Error("Has did not report a key that was written")
	}
}

func TestStoreHasReportsUnreadableKeyAsAbsent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission semantics differ on windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("running as root, permission bits are not enforced")
	}

	s := newTestStore(t)
	if _, err := s.Write("blocked", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Make the containing directory unsearchable, so stat fails with a
	// permission error rather than "not found". Has must still say absent.
	dir := filepath.Dir(s.FullPathForKey("blocked"))
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, dirPerm) })

	if s.Has("blocked") {
		t.Error("Has reported a key it cannot stat as present")
	}
}

// TestStoreDeleteLeavesUnrelatedKeys is a regression test. Delete used to
// remove the entire top-level prefix directory, which destroyed every other
// file whose hash began with the same characters.
func TestStoreDeleteLeavesUnrelatedKeys(t *testing.T) {
	// A transform that deliberately places both keys under a shared prefix
	// directory, which is exactly what a hash prefix collision looks like.
	sharedPrefix := func(key string) PathKey {
		return PathKey{
			Pathname: filepath.Join("abcde", "fghij"),
			Filename: key,
		}
	}
	s := NewStore(StoreOpts{Root: t.TempDir(), PathTransformFunc: sharedPrefix})

	if _, err := s.Write("victim", bytes.NewReader([]byte("delete me"))); err != nil {
		t.Fatalf("Write victim: %v", err)
	}
	if _, err := s.Write("bystander", bytes.NewReader([]byte("keep me"))); err != nil {
		t.Fatalf("Write bystander: %v", err)
	}

	if err := s.Delete("victim"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if s.Has("victim") {
		t.Error("victim survived deletion")
	}
	if !s.Has("bystander") {
		t.Fatal("deleting one key also destroyed an unrelated key sharing its prefix")
	}

	got, err := os.ReadFile(s.FullPathForKey("bystander"))
	if err != nil {
		t.Fatalf("reading bystander: %v", err)
	}
	if string(got) != "keep me" {
		t.Errorf("bystander contents = %q, want %q", got, "keep me")
	}
}

func TestStoreDeleteIsIdempotent(t *testing.T) {
	s := newTestStore(t)

	// Deletions are broadcast to peers that may never have held the file, so
	// deleting an absent key must succeed rather than fail the whole request.
	if err := s.Delete("never-stored"); err != nil {
		t.Errorf("deleting an absent key returned %v, want nil", err)
	}

	if _, err := s.Write("once", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Delete("once"); err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	if err := s.Delete("once"); err != nil {
		t.Errorf("second Delete returned %v, want nil", err)
	}
}

func TestStoreDeletePrunesEmptyDirectories(t *testing.T) {
	s := newTestStore(t)
	root := s.Root

	if _, err := s.Write("lonely", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Delete("lonely"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("store root still holds %d entries after deleting the only file", len(entries))
	}

	// Pruning must stop at the root and never remove it.
	if _, err := os.Stat(root); err != nil {
		t.Errorf("store root was removed: %v", err)
	}
}

func TestStoreClear(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Write("a", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if s.Has("a") {
		t.Error("key survived Clear")
	}
}

func TestStoreReadMissingKey(t *testing.T) {
	s := newTestStore(t)
	if _, _, err := s.Read("nope"); err == nil {
		t.Error("expected an error reading a missing key, got nil")
	}
	if _, _, err := s.ReadDecrypt(mustKey(t), "nope"); err == nil {
		t.Error("expected an error decrypting a missing key, got nil")
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

// TestStoreWritesDoNotLeakDescriptors is a regression test: WriteEncrypt used
// to return without closing the file, exhausting the process descriptor limit
// after enough stores and eventually breaking accept() too.
func TestStoreWritesDoNotLeakDescriptors(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("descriptor counting relies on /proc")
	}
	s := newTestStore(t)
	key := mustKey(t)

	// Warm up first, so one-off allocations are not counted as leaks.
	if _, err := s.WriteEncrypt(key, "warmup", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("WriteEncrypt: %v", err)
	}

	before := openFDCount(t)
	for i := range 50 {
		if _, err := s.WriteEncrypt(key, string(rune('a'+i%26))+"-enc", bytes.NewReader([]byte("payload"))); err != nil {
			t.Fatalf("WriteEncrypt: %v", err)
		}
		if _, err := s.Write(string(rune('a'+i%26))+"-plain", bytes.NewReader([]byte("payload"))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	after := openFDCount(t)

	if after-before > 5 {
		t.Errorf("open descriptors grew from %d to %d across 100 writes", before, after)
	}
}

// TestStoreWriteIsAtomicForReaders is a regression test. Writes went straight
// to the destination file, so a reader that had already opened a key saw it
// truncated underneath them when a second copy of the same file arrived.
func TestStoreWriteIsAtomicForReaders(t *testing.T) {
	s := newTestStore(t)
	key := mustKey(t)

	original := bytes.Repeat([]byte("original"), 4096)
	if _, err := s.WriteEncrypt(key, "doc", bytes.NewReader(original)); err != nil {
		t.Fatalf("WriteEncrypt: %v", err)
	}

	// Open the file, then overwrite the key while the reader is still open.
	_, reader, err := s.ReadDecrypt(key, "doc")
	if err != nil {
		t.Fatalf("ReadDecrypt: %v", err)
	}

	replacement := bytes.Repeat([]byte("replaced"), 4096)
	if _, err := s.WriteEncrypt(key, "doc", bytes.NewReader(replacement)); err != nil {
		t.Fatalf("second WriteEncrypt: %v", err)
	}

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("the in-progress read returned %d bytes that are not the original contents", len(got))
	}

	// The new contents must nonetheless be what a fresh read sees.
	_, reader2, err := s.ReadDecrypt(key, "doc")
	if err != nil {
		t.Fatalf("ReadDecrypt after overwrite: %v", err)
	}
	got2, err := io.ReadAll(reader2)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got2, replacement) {
		t.Error("a fresh read did not see the replacement contents")
	}
}

// TestStoreFailedWriteLeavesPreviousContents pins that an error partway
// through a write does not destroy the copy already stored.
func TestStoreFailedWriteLeavesPreviousContents(t *testing.T) {
	s := newTestStore(t)
	key := mustKey(t)

	original := []byte("the good copy")
	if _, err := s.WriteEncrypt(key, "doc", bytes.NewReader(original)); err != nil {
		t.Fatalf("WriteEncrypt: %v", err)
	}

	failing := io.MultiReader(
		bytes.NewReader(bytes.Repeat([]byte("partial"), 1000)),
		&errReader{err: errors.New("connection reset")},
	)
	if _, err := s.WriteEncrypt(key, "doc", failing); err == nil {
		t.Fatal("a failed write reported success")
	}

	_, r, err := s.ReadDecrypt(key, "doc")
	if err != nil {
		t.Fatalf("ReadDecrypt: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("got %q after a failed write, want the previous contents %q", got, original)
	}
}

// TestStoreFailedWriteLeavesNoResidue checks the temporary file is cleaned up.
func TestStoreFailedWriteLeavesNoResidue(t *testing.T) {
	s := newTestStore(t)

	failing := io.MultiReader(
		bytes.NewReader([]byte("partial")),
		&errReader{err: errors.New("connection reset")},
	)
	if _, err := s.WriteEncrypt(mustKey(t), "doc", failing); err == nil {
		t.Fatal("a failed write reported success")
	}

	if s.Has("doc") {
		t.Error("a failed write left the key readable")
	}

	var files int
	filepath.WalkDir(s.Root, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			files++
		}
		return nil
	})
	if files != 0 {
		t.Errorf("a failed write left %d file(s) behind", files)
	}
}

// errReader fails after the readers before it are drained.
type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }
