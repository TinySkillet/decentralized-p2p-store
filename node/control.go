package node

import (
	"bufio"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
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

	// Read-model operations. These answer from memory and the database and
	// never touch the network, so they are safe to poll.
	opNode   = "node"
	opPeers  = "peers"
	opFiles  = "files"
	opShares = "shares"

	// opRecheck measures one file now, bounded by a single offer round.
	opRecheck = "recheck"

	// opWatch streams events until the caller disconnects.
	opWatch = "watch"

	// Trust administration.
	opTrust   = "trust"
	opUntrust = "untrust"
	opTrusted = "trusted"
	opMode    = "mode"
)

// defaultWatchHeartbeat is how often a watch with nothing to report sends an
// empty event.
//
// Without it a node that goes quiet is indistinguishable from one that died,
// and neither side notices a connection that has gone away: the server only
// learns the client is gone when a write fails.
const defaultWatchHeartbeat = 15 * time.Second

// watchBuffer is how many events a watcher may fall behind by before it starts
// missing them.
const watchBuffer = 256

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

	// Value carries a free-form argument: a peer label when trusting, or the
	// mode when setting one. Empty when reading.
	Value string
}

// controlResponse answers a controlRequest.
type controlResponse struct {
	Error string

	Health  []FileHealth
	Offered int

	// Size is the number of payload bytes that follow this message, used when
	// fetching a file.
	Size int64

	// Read-model answers. Only the field matching the request is set.
	Node     NodeView
	Peers    []PeerView
	Files    []ReplicaSnapshot
	Shares   []ShareView
	Snapshot ReplicaSnapshot

	// Streaming marks a response followed by a stream of gob-encoded events
	// rather than a byte payload of known length.
	Streaming bool

	Trusted []TrustedPeerView
	Mode    string

	// Changed reports whether an operation altered anything, so "untrusted"
	// and "was not trusted anyway" can be told apart.
	Changed bool
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

		if resp.Streaming {
			// A watch client sends nothing once the stream is open, so a read
			// that returns means it has gone away. Without this the server
			// only finds out when a write fails, and an idle stream does not
			// write until the next heartbeat: the goroutine and its
			// subscription would linger until then.
			go func() {
				var b [1]byte
				_, _ = br.Read(b[:])
				closer.Close()
			}()
		}
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

	case opNode:
		view, err := s.NodeView(context.Background())
		if err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Node: view}, nil

	case opPeers:
		peers, err := s.PeerViews(context.Background())
		if err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Peers: peers}, nil

	case opFiles:
		files, err := s.ReplicationSnapshot(context.Background())
		if err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Files: files}, nil

	case opShares:
		shares, err := s.ShareViews(context.Background())
		if err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Shares: shares}, nil

	case opRecheck:
		snap, err := s.Recheck(context.Background(), req.Name)
		if err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Snapshot: snap}, nil

	case opWatch:
		return controlResponse{Streaming: true}, s.watchPayload()

	case opTrust:
		if err := s.Trust(req.Name, req.Value); err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Changed: true}, nil

	case opUntrust:
		had, err := s.Untrust(req.Name)
		if err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Changed: had}, nil

	case opTrusted:
		trusted, err := s.TrustedPeers(context.Background())
		if err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Trusted: trusted, Mode: s.TrustMode()}, nil

	case opMode:
		if req.Value == "" {
			return controlResponse{Mode: s.TrustMode()}, nil
		}
		if err := s.SetTrustMode(req.Value); err != nil {
			return controlResponse{Error: err.Error()}, nil
		}
		return controlResponse{Mode: s.TrustMode(), Changed: true}, nil

	default:
		return controlResponse{Error: fmt.Sprintf("unknown operation %q", req.Op)}, nil
	}
}

// watchPayload returns a reader that yields gob-encoded events until the
// caller goes away or the node stops.
//
// This is why watch needs no change to the framing: it is still one request
// and one response, and the stream is simply that response's payload. The
// server writes into a pipe and handleControl copies the pipe to the socket,
// exactly as it does for a file.
//
// handleControl closes the read end when the copy finishes, which is what
// unblocks the writer below if the client disappears mid-event.
// heartbeat is the interval this node sends keepalives at.
func (s *FileServer) heartbeat() time.Duration {
	if s.WatchHeartbeat > 0 {
		return s.WatchHeartbeat
	}
	return defaultWatchHeartbeat
}

func (s *FileServer) watchPayload() io.Reader {
	pr, pw := io.Pipe()
	stream := &watchStream{PipeReader: pr, done: make(chan struct{})}

	events, cancel := s.Subscribe(watchBuffer)

	go func() {
		defer cancel()

		enc := gob.NewEncoder(pw)
		ticker := time.NewTicker(s.heartbeat())

		defer ticker.Stop()

		for {
			var e Event
			select {
			case ev, ok := <-events:
				if !ok {
					pw.Close()
					return
				}
				e = ev

			case <-ticker.C:
				// An empty kind is the heartbeat: it proves the connection is
				// alive without claiming anything happened.
				e = Event{At: time.Now()}

			case <-s.quitch:
				// Stop() closes the listener but not connections already
				// established, so a watch has to watch for the shutdown
				// itself or it would outlive the node.
				pw.Close()
				return

			case <-stream.done:
				// handleControl finished copying, which means the client is
				// gone. Without this the goroutine and its subscription sit
				// here until the next heartbeat happens to fail on a write.
				pw.Close()
				return
			}

			if err := enc.Encode(e); err != nil {
				// The client is gone. Closing with the error stops the copy
				// on the other side of the pipe too.
				pw.CloseWithError(err)
				return
			}
		}
	}()

	return stream
}

// watchStream is the payload of a watch, with a close that the writer can see.
//
// handleControl closes the payload when its copy ends, which is how the server
// learns a client has gone away. A plain pipe reader only reports that on the
// next write, so an idle stream would hold its goroutine and its subscription
// until the following heartbeat.
type watchStream struct {
	*io.PipeReader
	done chan struct{}
	once sync.Once
}

func (w *watchStream) Close() error {
	w.once.Do(func() { close(w.done) })
	return w.PipeReader.Close()
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

// Node reports the running node's own state.
func (c *Client) Node() (NodeView, error) {
	resp, call, err := c.do(controlRequest{Op: opNode}, nil)
	if call != nil {
		call.Close()
	}
	if err != nil {
		return NodeView{}, err
	}
	return resp.Node, nil
}

// Peers reports every peer the running node knows, live ones marked online.
func (c *Client) Peers() ([]PeerView, error) {
	resp, call, err := c.do(controlRequest{Op: opPeers}, nil)
	if call != nil {
		call.Close()
	}
	if err != nil {
		return nil, err
	}
	return resp.Peers, nil
}

// Files reports the stored files with their cached replication measurements.
func (c *Client) Files() ([]ReplicaSnapshot, error) {
	resp, call, err := c.do(controlRequest{Op: opFiles}, nil)
	if call != nil {
		call.Close()
	}
	if err != nil {
		return nil, err
	}
	return resp.Files, nil
}

// Shares reports the transfers the running node has recorded.
func (c *Client) Shares() ([]ShareView, error) {
	resp, call, err := c.do(controlRequest{Op: opShares}, nil)
	if call != nil {
		call.Close()
	}
	if err != nil {
		return nil, err
	}
	return resp.Shares, nil
}

// Recheck measures one file now and returns the fresh result.
func (c *Client) Recheck(name string) (ReplicaSnapshot, error) {
	resp, call, err := c.do(controlRequest{Op: opRecheck, Name: name}, nil)
	if call != nil {
		call.Close()
	}
	if err != nil {
		return ReplicaSnapshot{}, err
	}
	return resp.Snapshot, nil
}

// Watch streams events from the running node until stop is called or the node
// shuts down. The channel is closed when the stream ends.
//
// Heartbeats arrive as events with an empty Kind and are not forwarded; they
// exist so a dead connection is noticed rather than waited on for ever.
func (c *Client) Watch() (<-chan Event, func(), error) {
	resp, call, err := c.do(controlRequest{Op: opWatch}, nil)
	if err != nil {
		return nil, nil, err
	}
	if !resp.Streaming {
		call.Close()
		return nil, nil, fmt.Errorf("the node did not open an event stream")
	}

	out := make(chan Event, watchBuffer)
	done := make(chan struct{})

	go func() {
		defer close(out)

		// Decoding from the same buffered reader the response was read from:
		// a fresh reader would lose whatever that decoder had already pulled
		// in ahead of the stream.
		dec := gob.NewDecoder(call.body)
		for {
			var e Event
			if err := dec.Decode(&e); err != nil {
				return
			}
			if e.Kind == "" {
				// The heartbeat proves the connection is alive and reports
				// nothing, so it is not forwarded.
				continue
			}

			// Never send without also watching done: a caller that stops
			// reading must not strand this goroutine for the life of the
			// process, and closing the connection cannot interrupt a blocked
			// channel send.
			select {
			case out <- e:
			case <-done:
				return
			}
		}
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			close(done)
			call.Close()
		})
	}
	return out, stop, nil
}

// Trust approves a peer on the running node.
func (c *Client) Trust(nodeID, label string) error {
	_, call, err := c.do(controlRequest{Op: opTrust, Name: nodeID, Value: label}, nil)
	if call != nil {
		call.Close()
	}
	return err
}

// Untrust withdraws approval, reporting whether the peer had it.
func (c *Client) Untrust(nodeID string) (bool, error) {
	resp, call, err := c.do(controlRequest{Op: opUntrust, Name: nodeID}, nil)
	if call != nil {
		call.Close()
	}
	if err != nil {
		return false, err
	}
	return resp.Changed, nil
}

// Trusted lists the approved peers and reports the current trust mode.
func (c *Client) Trusted() ([]TrustedPeerView, string, error) {
	resp, call, err := c.do(controlRequest{Op: opTrusted}, nil)
	if call != nil {
		call.Close()
	}
	if err != nil {
		return nil, "", err
	}
	return resp.Trusted, resp.Mode, nil
}

// Mode reads the trust mode, or sets it when mode is not empty.
func (c *Client) Mode(mode string) (string, error) {
	resp, call, err := c.do(controlRequest{Op: opMode, Value: mode}, nil)
	if call != nil {
		call.Close()
	}
	if err != nil {
		return "", err
	}
	return resp.Mode, nil
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
