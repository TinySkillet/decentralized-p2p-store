package node

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// controlSocketName is the socket a running node accepts commands on. It sits
// beside the database, because holding the database is already what grants
// authority over the files it describes.
const controlSocketName = "control.sock"

// controlSocketPerm keeps the socket usable only by the node's own user.
// Anything that can talk to it can delete files as their owner.
const controlSocketPerm os.FileMode = 0o600

// controlDialTimeout bounds how long a command waits to reach a running node.
const controlDialTimeout = 2 * time.Second

// maxSocketPath is the longest a unix socket path may be.
//
// The kernel struct that carries it is a fixed-size buffer: 108 bytes on Linux
// and 104 on macOS, and exceeding it fails with a bare "invalid argument". A
// database a few directories deep is enough to reach that, so the limit has to
// be handled rather than assumed away.
const maxSocketPath = 100

// ControlSocketPath returns the socket path for a given database.
//
// The socket normally sits beside the database, which keeps the two obviously
// paired. When that path would be too long for a unix socket, it moves to the
// user's runtime directory under a name derived from the database path, so the
// node and its commands still agree on where to find each other.
func ControlSocketPath(dbPath string) string {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		abs = dbPath
	}

	beside := filepath.Join(filepath.Dir(abs), controlSocketName)
	if len(beside) <= maxSocketPath {
		return beside
	}

	return filepath.Join(controlRuntimeDir(), "p2p-"+storage.ContentKey([]byte(abs))[:16]+".sock")
}

// controlRuntimeDir returns a private directory for sockets that cannot live
// beside their database.
//
// XDG_RUNTIME_DIR is preferred because it is already private to the user. The
// fallback under the temporary directory is created with owner-only
// permissions, since a shared directory would let another user pre-create the
// path and intercept commands.
func controlRuntimeDir() string {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		if info, err := os.Stat(dir); err == nil && info.IsDir() {
			return dir
		}
	}

	dir := filepath.Join(os.TempDir(), fmt.Sprintf("p2pstorage-%d", os.Getuid()))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return os.TempDir()
	}
	// A pre-existing directory may have looser permissions than we want.
	os.Chmod(dir, 0o700)
	return dir
}

// Control operations.
const (
	opStatus = "status"
	opRepair = "repair"
	opStore  = "store"
	opGet    = "get"
	opDelete = "delete"
)

// controlRequest asks a running node to do something.
//
// One request per connection: there is nothing to multiplex, and it keeps the
// framing to a single message followed by an optional payload.
type controlRequest struct {
	Op   string
	Name string

	// Size is the number of payload bytes that follow this message, used when
	// storing a file.
	Size int64

	// Replicas overrides the node's replication target for this request.
	Replicas int
}

// controlResponse answers a controlRequest.
type controlResponse struct {
	Error string

	Health  []FileHealth
	Offered int

	// Size is the number of payload bytes that follow this message, used when
	// fetching a file.
	Size int64
}

// ListenControl starts accepting commands on the socket beside the database.
//
// A command run against a node's database used to start a second node sharing
// its storage, which produced a series of bugs: two writers racing on the same
// row, replica counts that treated the borrowed storage as a second copy, and
// files owned by a key that vanished when the command exited. Asking the
// running node to act removes all of that by construction.
func (s *FileServer) ListenControl(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	// A socket left behind by a process that died is stale and can be
	// replaced; one that answers belongs to a node that is still running.
	if conn, err := net.DialTimeout("unix", path, controlDialTimeout); err == nil {
		conn.Close()
		return fmt.Errorf("another node is already serving %s", path)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		if len(path) > maxSocketPath {
			return fmt.Errorf("control socket path %q is %d characters, over the %d the system allows: %w",
				path, len(path), maxSocketPath, err)
		}
		return err
	}
	if err := os.Chmod(path, controlSocketPerm); err != nil {
		ln.Close()
		return err
	}

	s.controlListener = ln
	go s.serveControl(ln)

	fmt.Printf("[%s] Accepting commands on %s\n", s.Transport.Address(), path)
	return nil
}

func (s *FileServer) serveControl(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quitch:
				return
			default:
			}
			// A transient accept error leaves conn nil, so it must not be
			// touched before looping.
			log.Printf("[%s] Control accept error: %v", s.Transport.Address(), err)
			continue
		}
		go s.handleControl(conn)
	}
}

func (s *FileServer) handleControl(conn net.Conn) {
	defer conn.Close()

	// One buffered reader for the whole connection. A gob decoder reads ahead,
	// so giving it the raw connection would let it swallow the start of the
	// payload that follows and leave the request waiting for bytes that had
	// already been consumed.
	br := bufio.NewReader(conn)

	var req controlRequest
	if err := gob.NewDecoder(br).Decode(&req); err != nil {
		log.Printf("[%s] Malformed control request: %v", s.Transport.Address(), err)
		return
	}

	resp, payload := s.runControl(req, br)

	if err := gob.NewEncoder(conn).Encode(resp); err != nil {
		log.Printf("[%s] Could not answer a control request: %v", s.Transport.Address(), err)
		return
	}
	if payload == nil {
		return
	}
	if closer, ok := payload.(io.Closer); ok {
		defer closer.Close()
	}
	if _, err := io.Copy(conn, payload); err != nil {
		log.Printf("[%s] Could not send the payload for %q: %v", s.Transport.Address(), req.Op, err)
	}
}

// runControl carries out one request and returns the response, plus a payload
// to stream after it. body carries any bytes the request sent.
func (s *FileServer) runControl(req controlRequest, body io.Reader) (controlResponse, io.Reader) {
	target := s.ReplicationFactor
	if req.Replicas > 0 {
		target = req.Replicas
	}

	switch req.Op {
	case opStatus:
		health, err := s.replicationStatusFor(target)
		if err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Health: health}, nil

	case opRepair:
		// Offered, not placed: a peer refuses contents it has deleted, which
		// is how a deletion reaches nodes that missed it.
		offered, err := s.repairOnceFor(target)
		if err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Offered: offered}, nil

	case opStore:
		if err := s.Store(req.Name, io.LimitReader(body, req.Size)); err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{}, nil

	case opGet:
		size, r, err := s.Get(req.Name)
		if err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Size: size}, r

	case opDelete:
		if err := s.Delete(req.Name); err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{}, nil

	default:
		return controlResponse{Error: fmt.Sprintf("unknown operation %q", req.Op)}, nil
	}
}

// Client talks to a running node.
type Client struct{ path string }

// DialControl returns a client for the node serving this database, or false if
// no node is running.
func DialControl(dbPath string) (*Client, bool) {
	path := ControlSocketPath(dbPath)
	conn, err := net.DialTimeout("unix", path, controlDialTimeout)
	if err != nil {
		return nil, false
	}
	conn.Close()
	return &Client{path: path}, true
}

// controlCall is an open request, held so a caller expecting a payload can
// read it before closing.
type controlCall struct {
	conn net.Conn
	body *bufio.Reader
}

func (c *controlCall) Close() error { return c.conn.Close() }

// do sends one request and reads the response.
func (c *Client) do(req controlRequest, payload io.Reader) (controlResponse, *controlCall, error) {
	conn, err := net.DialTimeout("unix", c.path, controlDialTimeout)
	if err != nil {
		return controlResponse{}, nil, err
	}

	if err := gob.NewEncoder(conn).Encode(req); err != nil {
		conn.Close()
		return controlResponse{}, nil, err
	}
	if payload != nil {
		if _, err := io.Copy(conn, payload); err != nil {
			conn.Close()
			return controlResponse{}, nil, err
		}
	}

	// As on the server side, the decoder and the payload have to share one
	// buffered reader or the decoder will eat into the payload.
	br := bufio.NewReader(conn)

	var resp controlResponse
	if err := gob.NewDecoder(br).Decode(&resp); err != nil {
		conn.Close()
		return controlResponse{}, nil, err
	}
	if resp.Error != "" {
		conn.Close()
		return resp, nil, fmt.Errorf("%s", resp.Error)
	}

	return resp, &controlCall{conn: conn, body: br}, nil
}

func (c *Client) Status(replicas int) ([]FileHealth, error) {
	resp, call, err := c.do(controlRequest{Op: opStatus, Replicas: replicas}, nil)
	if call != nil {
		call.Close()
	}
	if err != nil {
		return nil, err
	}
	return resp.Health, nil
}

func (c *Client) Repair(replicas int) (int, error) {
	resp, call, err := c.do(controlRequest{Op: opRepair, Replicas: replicas}, nil)
	if call != nil {
		call.Close()
	}
	if err != nil {
		return 0, err
	}
	return resp.Offered, nil
}

func (c *Client) Store(name string, size int64, body io.Reader) error {
	_, call, err := c.do(controlRequest{Op: opStore, Name: name, Size: size}, body)
	if call != nil {
		call.Close()
	}
	return err
}

func (c *Client) Delete(name string) error {
	_, call, err := c.do(controlRequest{Op: opDelete, Name: name}, nil)
	if call != nil {
		call.Close()
	}
	return err
}

// get writes the fetched file to w.
func (c *Client) Get(name string, w io.Writer) error {
	resp, call, err := c.do(controlRequest{Op: opGet, Name: name}, nil)
	if err != nil {
		return err
	}
	defer call.Close()

	if _, err := io.CopyN(w, call.body, resp.Size); err != nil {
		return fmt.Errorf("reading %q: %w", name, err)
	}
	return nil
}
