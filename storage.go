package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"strings"
)

const DEFAULT_ROOT_FOLDER = "p2pnetwork"

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashString := hex.EncodeToString(hash[:])

	blockSize := 5
	sliceLen := len(hashString) / blockSize

	paths := make([]string, sliceLen)

	for i := range sliceLen {
		from, to := i*blockSize, (i+1)*blockSize
		paths[i] = hashString[from:to]
	}

	return PathKey{
		Pathname: strings.Join(paths, "/"),
		Filename: hashString,
	}
}

func (s *Store) Read(key string) (int64, io.Reader, error) {
	return s.readStream(key)
}

func (s *Store) Write(key string, r io.Reader) (int64, error) {
	return s.writeStream(key, r)
}

func (s *Store) WriteEncrypt(encryptionKey []byte, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(key)
	if err != nil {
		return 0, err
	}
	n, err := copyEncrypt(encryptionKey, r, f)
	return int64(n), err
}

func (s *Store) ReadDecrypt(encryptionKey []byte, key string) (int64, io.Reader, error) {
	f, err := s.openFileForReading(key)
	if err != nil {
		return 0, nil, err
	}

	pr, pw := io.Pipe()

	go func() {
		_, err := copyDecrypt(encryptionKey, f, pw)
		pw.CloseWithError(err)
		f.Close()
	}()

	return 0, pr, nil
}

func (s *Store) openFileForReading(key string) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)
	fullPath := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())
	return os.Open(fullPath)
}

func (s *Store) writeStream(key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(key)
	if err != nil {
		return 0, err
	}
	return io.Copy(f, r)
}

func (s *Store) Delete(key string) error {
	pathKey := s.PathTransformFunc(key)

	parentDir := strings.Split(pathKey.FullPath(), "/")[0]

	parentDirWRoot := fmt.Sprintf("%s/%s", s.Root, parentDir)
	if err := os.RemoveAll(parentDirWRoot); err != nil {
		log.Printf("Failed to delete [%s] from disk: %v\n", pathKey.Filename, err)
		return err
	}

	log.Printf("Deleted [%s] from disk\n", pathKey.Filename)
	return nil
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

func (s *Store) Has(key string) bool {
	pathKey := s.PathTransformFunc(key)
	fullPath := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())

	_, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false
		}
	}
	return true
}

func (s *Store) readStream(key string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)

	fullPath := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		return 0, nil, err
	}

	file, err := os.Open(fullPath)
	if err != nil {
		return 0, nil, err
	}

	return fileInfo.Size(), file, err
}

func (s *Store) openFileForWriting(key string) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)

	pathnameWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.Pathname)
	if err := os.MkdirAll(pathnameWithRoot, os.ModePerm); err != nil {
		return nil, err
	}

	fullPath := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())
	return os.Create(fullPath)
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	Pathname string
	Filename string
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.Pathname, p.Filename)
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
	return fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())
}
