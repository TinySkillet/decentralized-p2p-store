package node

import (
	"bytes"
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
)

// servingNode starts a node with a control socket and returns a client for it.
func servingNode(t *testing.T, bootstrap ...string) (*testNode, *Client) {
	t.Helper()
	return servingNodeWith(t, nodeConfig{}, bootstrap...)
}

func servingNodeWith(t *testing.T, cfg nodeConfig, bootstrap ...string) (*testNode, *Client) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "p2p.db")
	n := buildTestNodeWithDB(t, dbPath, freeAddr(t), cfg, bootstrap...)

	sock := filepath.Join(t.TempDir(), "control.sock")
	if err := n.ListenControl(sock); err != nil {
		t.Fatalf("ListenControl: %v", err)
	}

	return n, &Client{path: sock}
}

func TestControlNodeReportsTheRunningNode(t *testing.T) {
	n, c := servingNode(t)

	if err := n.Store("counted.txt", bytes.NewReader([]byte("some bytes"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	view, err := c.Node()
	if err != nil {
		t.Fatalf("Node: %v", err)
	}
	if view.NodeID != n.NodeID() {
		t.Fatalf("node op reported %q, want %q", view.NodeID, n.NodeID())
	}
	if view.Files != 1 {
		t.Fatalf("node op reported %d files, want 1", view.Files)
	}
	if view.Bytes != 10 {
		t.Fatalf("node op reported %d bytes, want 10", view.Bytes)
	}
	if view.ReplicationFactor <= 0 {
		t.Fatalf("node op reported a replication factor of %d", view.ReplicationFactor)
	}
}

// The peers op must report the live view, not the database's. A stale
// "connected" row is the case that separates them.
func TestControlPeersReportsLivenessNotTheDatabase(t *testing.T) {
	n, c := servingNode(t)

	now := time.Now()
	if err := n.DB.UpsertPeer(context.Background(), dbpkg.Peer{
		NodeID: "ghost", Address: "192.0.2.80:4000", Status: "connected", LastSeen: &now,
	}); err != nil {
		t.Fatalf("UpsertPeer: %v", err)
	}

	peers, err := c.Peers()
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("peers op returned %d peers, want 1", len(peers))
	}
	if peers[0].Online {
		t.Fatal("peers op reported a stale database row as online")
	}

	// And a genuinely connected peer reads online.
	other := newTestNode(t, n.addr)
	waitForPeerCount(t, n, 1)

	peers, err = c.Peers()
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	online := 0
	for _, p := range peers {
		if p.Online {
			online++
			if p.NodeID != other.NodeID() {
				t.Fatalf("the online peer is %q, want %q", p.NodeID, other.NodeID())
			}
		}
	}
	if online != 1 {
		t.Fatalf("peers op reported %d online peers, want 1", online)
	}
}

func TestControlFilesAndSharesAndRecheck(t *testing.T) {
	n, c := servingNode(t)
	peer := newTestNode(t, n.addr)
	waitForPeerCount(t, n, 1)
	_ = peer

	if err := n.Store("shared.txt", bytes.NewReader([]byte("replicate me"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	files, err := c.Files()
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("files op returned %d files, want 1", len(files))
	}
	if !files[0].Measured() {
		t.Fatal("files op returned an unmeasured file after a store")
	}
	if files[0].Copies != 2 {
		t.Fatalf("files op reported %d copies, want 2", files[0].Copies)
	}

	waitFor(t, "the share to be recorded", 10*time.Second, func() bool {
		shares, err := c.Shares()
		return err == nil && len(shares) == 1
	})

	shares, err := c.Shares()
	if err != nil {
		t.Fatalf("Shares: %v", err)
	}
	if shares[0].Name != "shared.txt" || shares[0].Direction != "outgoing" {
		t.Fatalf("shares op returned %+v", shares[0])
	}

	snap, err := c.Recheck("shared.txt")
	if err != nil {
		t.Fatalf("Recheck: %v", err)
	}
	if snap.Copies != 2 {
		t.Fatalf("recheck reported %d copies, want 2", snap.Copies)
	}

	if _, err := c.Recheck("not-here.txt"); err == nil {
		t.Fatal("recheck accepted a file that is not stored")
	}
}

// The watch op streams events over the existing one-request framing.
func TestControlWatchStreamsEvents(t *testing.T) {
	n, c := servingNode(t)

	events, stop, err := c.Watch()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	if err := n.Store("watched.txt", bytes.NewReader([]byte("event me"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	select {
	case e, ok := <-events:
		if !ok {
			t.Fatal("the event stream closed before delivering anything")
		}
		if e.Kind != EventFileStored {
			t.Fatalf("first event was %q, want %q", e.Kind, EventFileStored)
		}
		if e.Name != "watched.txt" {
			t.Fatalf("the event named %q", e.Name)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no event arrived over the control socket")
	}
}

// A watch must not outlive the node. Stop() closes the listener but not
// connections already established, so the stream has to notice the shutdown
// itself.
func TestControlWatchEndsWhenTheNodeStops(t *testing.T) {
	n, c := servingNode(t)

	events, stop, err := c.Watch()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	n.Stop()

	select {
	case _, ok := <-events:
		if ok {
			// An event may be in flight; the close must still follow.
			select {
			case _, ok := <-events:
				if ok {
					t.Fatal("the stream kept delivering after the node stopped")
				}
			case <-time.After(15 * time.Second):
				t.Fatal("the stream did not close after the node stopped")
			}
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the stream did not close after the node stopped")
	}
}

// The heartbeat proves a quiet connection is alive. It is not delivered as an
// event, so the test observes it by watching a silent node stay open.
func TestControlWatchHeartbeatsWhileIdle(t *testing.T) {
	n, c := servingNodeWith(t, nodeConfig{watchHeartbeat: 100 * time.Millisecond})

	events, stop, err := c.Watch()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	// Several heartbeat intervals with nothing happening.
	time.Sleep(600 * time.Millisecond)

	select {
	case _, ok := <-events:
		if !ok {
			t.Fatal("an idle stream closed; the heartbeats did not keep it open")
		}
		t.Fatal("a heartbeat was delivered as an event")
	default:
	}

	// Still live: a real event gets through.
	if err := n.Store("after-idle.txt", bytes.NewReader([]byte("still here"))); err != nil {
		t.Fatalf("Store: %v", err)
	}
	select {
	case e, ok := <-events:
		if !ok {
			t.Fatal("the stream closed instead of delivering")
		}
		if e.Kind != EventFileStored {
			t.Fatalf("got %q, want %q", e.Kind, EventFileStored)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no event arrived after the idle period")
	}
}

// A caller that stops reading must not strand the decode goroutine. Closing
// the connection cannot interrupt a blocked channel send, so stop has to.
//
// The leak can only be seen by counting goroutines: reading the channel to
// check it closes is exactly what would unblock the stranded send, so a test
// that drains cannot detect this at all.
//
// Fails against a watch whose send has no escape.
func TestControlWatchStopReleasesAnUnreadStream(t *testing.T) {
	n, c := servingNode(t)

	const watchers = 8

	settle(t)
	before := runtime.NumGoroutine()

	stops := make([]func(), 0, watchers)
	channels := make([]<-chan Event, 0, watchers)
	for i := 0; i < watchers; i++ {
		ch, stop, err := c.Watch()
		if err != nil {
			t.Fatalf("Watch %d: %v", i, err)
		}
		stops = append(stops, stop)
		channels = append(channels, ch)
	}

	// Fill every watcher's buffer and then some, with nobody reading.
	for i := 0; i < watchBuffer*3; i++ {
		n.publish(Event{Kind: EventPeerUp, Node: "noisy"})
	}

	// Each decode goroutine has to actually reach a blocked send before stop
	// is called, or it is still sitting in Decode and closing the connection
	// releases it cleanly — which would let a stranding implementation pass.
	// Reading one event proves delivery has started; the rest then back up.
	for i, ch := range channels {
		select {
		case <-ch:
		case <-time.After(20 * time.Second):
			t.Fatalf("watcher %d received nothing", i)
		}
	}
	waitFor(t, "the watchers to back up", 10*time.Second, func() bool {
		return len(channels[0]) == cap(channels[0]) || cap(channels[0]) == 0
	})

	for _, stop := range stops {
		stop()
	}

	// Both the client decode goroutines and the server's stream goroutines
	// must go away. A stranded one never will.
	//
	// Bounded well below the heartbeat on purpose: without the hangup check
	// the server does eventually clean up, but only when the next keepalive
	// fails to write. Allowing a heartbeat's worth of slack here would let
	// that much slower path pass as if it were the same thing.
	if defaultWatchHeartbeat <= 5*time.Second {
		t.Fatalf("this test assumes a heartbeat longer than its 5s budget, but it is %v", defaultWatchHeartbeat)
	}
	waitFor(t, "the watch goroutines to be released", 5*time.Second, func() bool {
		return runtime.NumGoroutine() < before+watchers
	})

	// Calling stop twice must not panic on a closed channel.
	stops[0]()
}

// settle waits for goroutines from earlier work to finish, so a count taken
// after it is a usable baseline.
func settle(t *testing.T) {
	t.Helper()

	previous := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		current := runtime.NumGoroutine()
		if current >= previous {
			return
		}
		previous = current
	}
}

// Two watchers each get every event.
func TestControlWatchFansOutToSeveralClients(t *testing.T) {
	n, c := servingNode(t)

	first, stopFirst, err := c.Watch()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stopFirst()

	second, stopSecond, err := c.Watch()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stopSecond()

	if err := n.Store("both.txt", bytes.NewReader([]byte("two watchers"))); err != nil {
		t.Fatalf("Store: %v", err)
	}

	for i, ch := range []<-chan Event{first, second} {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatalf("watcher %d saw the stream close", i)
			}
			if e.Kind != EventFileStored {
				t.Fatalf("watcher %d got %q", i, e.Kind)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("watcher %d received nothing", i)
		}
	}
}

// An unknown operation must be reported, not silently ignored.
func TestControlRejectsAnUnknownOperation(t *testing.T) {
	_, c := servingNode(t)

	_, call, err := c.do(controlRequest{Op: "nonsense"}, nil)
	if call != nil {
		call.Close()
	}
	if err == nil {
		t.Fatal("an unknown operation was accepted")
	}
}
