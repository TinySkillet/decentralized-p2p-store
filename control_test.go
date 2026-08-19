package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// withControl gives a node a control socket in a short-lived directory.
func withControl(t *testing.T, node *testNode) string {
	t.Helper()

	node.OwnsDatabase = true
	path := filepath.Join(t.TempDir(), "control.sock")
	if err := node.ListenControl(path); err != nil {
		t.Fatalf("ListenControl: %v", err)
	}
	t.Cleanup(func() { os.Remove(path) })
	return path
}

func TestControlSocketRoundTrip(t *testing.T) {
	owner := newQuietNodeWith(t, 2)
	replica := newQuietNode(t, owner.addr)

	waitForPeerCount(t, owner, 1)
	waitForPeerCount(t, replica, 1)

	path := withControl(t, owner)
	client := &controlClient{path: path}

	payload := randomBytes(t, 4096)

	// Store through the socket: the running node does the work, so the file
	// belongs to it rather than to a short-lived command.
	if err := client.store("through-socket", int64(len(payload)), bytes.NewReader(payload)); err != nil {
		t.Fatalf("store: %v", err)
	}

	f, err := owner.db.FindFileByName(context.Background(), "through-socket")
	if err != nil {
		t.Fatalf("FindFileByName: %v", err)
	}
	if f == nil {
		t.Fatal("the file was not recorded")
	}
	if f.Owner != owner.OwnerID() {
		t.Errorf("Owner = %q, want the serving node %q", short(f.Owner), short(owner.OwnerID()))
	}

	// Fetch it back.
	var got bytes.Buffer
	if err := client.get("through-socket", &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Error("the fetched contents differ from what was stored")
	}

	// Status must count the two real copies, not three.
	health, err := client.status(2)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(health) != 1 {
		t.Fatalf("got %d files, want 1", len(health))
	}
	if health[0].Copies != 2 {
		t.Errorf("Copies = %d, want 2", health[0].Copies)
	}
	if health[0].Target != 2 {
		t.Errorf("Target = %d, want the requested 2", health[0].Target)
	}

	// And delete it, which the node can authorise because it owns it.
	if err := client.delete("through-socket"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if f, err := owner.db.FindFileByName(context.Background(), "through-socket"); err != nil {
		t.Fatalf("FindFileByName: %v", err)
	} else if f != nil {
		t.Error("the name survived deletion")
	}
}

// TestControlStoreHandlesLargePayload covers the framing: the request message
// and the file body share one connection, so a decoder that read ahead into
// the body would deadlock.
func TestControlStoreHandlesLargePayload(t *testing.T) {
	node := newQuietNode(t)
	client := &controlClient{path: withControl(t, node)}

	// Comfortably larger than any buffer in the path.
	payload := randomBytes(t, 512*1024)

	done := make(chan error, 1)
	go func() {
		done <- client.store("big", int64(len(payload)), bytes.NewReader(payload))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("store: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("store deadlocked: the request decoder and the body are fighting over the connection")
	}

	var got bytes.Buffer
	if err := client.get("big", &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Errorf("got %d bytes back, want %d and identical", got.Len(), len(payload))
	}
}

func TestControlReportsErrorsBack(t *testing.T) {
	node := newQuietNode(t)
	client := &controlClient{path: withControl(t, node)}

	// Nothing stored and no peers, so this cannot succeed.
	var out bytes.Buffer
	err := client.get("nothing-here", &out)
	if err == nil {
		t.Fatal("fetching an unknown file reported success")
	}
	if !strings.Contains(err.Error(), "no peers") {
		t.Errorf("error = %v, want the node's own explanation", err)
	}

	if _, _, err := (&controlClient{path: client.path}).do(controlRequest{Op: "nonsense"}, nil); err == nil {
		t.Error("an unknown operation was accepted")
	}
}

func TestControlRefusesASecondNode(t *testing.T) {
	node := newQuietNode(t)
	path := withControl(t, node)

	other := newQuietNode(t)
	err := other.ListenControl(path)
	if err == nil {
		t.Fatal("a second node took over a socket that was already answering")
	}
	if !strings.Contains(err.Error(), "already serving") {
		t.Errorf("error = %v, want it to say a node is already serving", err)
	}

	// The original must still work.
	if _, err := (&controlClient{path: path}).status(1); err != nil {
		t.Errorf("the original node stopped answering: %v", err)
	}
}

// TestControlReplacesAStaleSocket covers the crash case: a socket file left by
// a process that died must not stop the next node from starting.
func TestControlReplacesAStaleSocket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.sock")

	// A socket file that nothing is listening on.
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ln.Close()
	if _, err := os.Stat(path); err == nil {
		t.Log("the closed socket left its file behind, which is the case being tested")
	} else {
		// Go removes it on close; recreate the leftover by hand.
		if f, err := os.Create(path); err == nil {
			f.Close()
		}
	}

	node := newQuietNode(t)
	node.OwnsDatabase = true
	if err := node.ListenControl(path); err != nil {
		t.Fatalf("a stale socket blocked startup: %v", err)
	}
	t.Cleanup(func() { os.Remove(path) })

	if _, err := (&controlClient{path: path}).status(1); err != nil {
		t.Errorf("the node is not answering on the reclaimed socket: %v", err)
	}
}

func TestControlSocketPathFallsBackWhenTooLong(t *testing.T) {
	short := ControlSocketPath(filepath.Join(t.TempDir(), "p2p.db"))
	if filepath.Base(short) != controlSocketName {
		t.Errorf("a short path gave %q, want the socket beside the database", short)
	}

	// A path long enough to exceed what a unix socket allows.
	deep := "/" + strings.Repeat("averyverylongdirectoryname/", 8) + "p2p.db"
	fallback := ControlSocketPath(deep)

	if len(fallback) > maxSocketPath {
		t.Errorf("the fallback path is %d characters, still over the %d limit", len(fallback), maxSocketPath)
	}
	if strings.HasPrefix(fallback, filepath.Dir(deep)) {
		t.Error("the fallback stayed beside the database, where it cannot bind")
	}
	// Both the node and its commands derive it, so it must be stable.
	if ControlSocketPath(deep) != fallback {
		t.Error("the fallback path is not deterministic")
	}
	// And distinct databases must not collide on one socket.
	if ControlSocketPath(deep) == ControlSocketPath("/"+strings.Repeat("anotherlongdirectoryname/", 8)+"p2p.db") {
		t.Error("two databases resolved to the same socket")
	}
}

func TestDialControlReportsNoNode(t *testing.T) {
	// No node is serving this database.
	if _, ok := dialControl(filepath.Join(t.TempDir(), "p2p.db")); ok {
		t.Error("dialControl claimed a node was running when none was")
	}
}
