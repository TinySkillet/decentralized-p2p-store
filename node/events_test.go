package node

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

// A subscriber that stops reading must not stop the bus. Events past its
// buffer are dropped and counted; every other subscriber keeps receiving.
// Fails against a bus that sends blockingly.
func TestPublishDoesNotBlockOnAStalledSubscriber(t *testing.T) {
	b := newEventBus()

	// Deliberately never drained, and deliberately never cancelled: a
	// blocking bus would still hold its lock, so cancelling here would hang
	// the test rather than failing it. The bus is local and collected with it.
	_, _ = b.subscribe(2)

	reading, cancelReading := b.subscribe(64)
	defer cancelReading()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			b.publish(Event{Kind: EventPeerUp, Node: "node"})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publish blocked on a subscriber that was not reading")
	}

	if got := b.dropped(); got == 0 {
		t.Fatal("expected the stalled subscriber to have dropped events, got 0")
	}

	// The healthy subscriber still got everything.
	got := 0
	for len(reading) > 0 {
		<-reading
		got++
	}
	if got != 50 {
		t.Fatalf("the reading subscriber got %d of 50 events", got)
	}
}

// Cancelling a subscription removes it and closes its channel, so a consumer
// ranging over the channel terminates and the bus does not leak.
func TestCancelSubscriptionClosesAndRemoves(t *testing.T) {
	b := newEventBus()

	ch, cancel := b.subscribe(4)
	if b.subscriberCount() != 1 {
		t.Fatalf("subscriberCount = %d, want 1", b.subscriberCount())
	}

	cancel()

	if b.subscriberCount() != 0 {
		t.Fatalf("after cancel, subscriberCount = %d, want 0", b.subscriberCount())
	}
	if _, open := <-ch; open {
		t.Fatal("the channel was not closed on cancel")
	}

	// Cancelling twice must not panic by closing a closed channel, and
	// publishing to nobody must not panic either.
	cancel()
	b.publish(Event{Kind: EventPeerUp})
}

// Many publishers and a subscriber cancelling underneath them. Run under
// -race, this is the guard on the bus's locking.
func TestBusIsSafeUnderConcurrentUse(t *testing.T) {
	b := newEventBus()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				b.publish(Event{Kind: EventFileStored, Name: "f"})
			}
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, cancel := b.subscribe(1)
			go func() {
				for range ch {
				}
			}()
			cancel()
		}()
	}
	wg.Wait()

	if b.subscriberCount() != 0 {
		t.Fatalf("subscriberCount = %d after every subscription was cancelled", b.subscriberCount())
	}
}

// A subscriber that asks the node about its peers on receiving each event must
// make progress. This is the test for the invariant that publish is never
// called while holding peersLock: a blocking publish under that lock deadlocks
// the node against exactly this, the most natural thing a consumer does.
func TestSubscriberMayQueryTheNodeOnEveryEvent(t *testing.T) {
	a := newTestNode(t)
	events, cancel := a.Subscribe(32)
	defer cancel()

	queried := make(chan int, 32)
	go func() {
		for range events {
			// Takes peersLock.
			_, ids := a.connectedPeers()
			queried <- len(ids)
		}
	}()

	b := newTestNode(t, a.addr)
	waitForPeerCount(t, b, 1)

	select {
	case <-queried:
	case <-time.After(15 * time.Second):
		t.Fatal("a subscriber that queries the peer set on each event made no progress")
	}
}

// The publishers are wired: connecting and disconnecting a peer is announced.
func TestPeerEventsArePublished(t *testing.T) {
	a := newTestNode(t)
	events, cancel := a.Subscribe(32)
	defer cancel()

	b := newTestNode(t, a.addr)
	waitForPeerCount(t, a, 1)

	up := waitForEvent(t, events, EventPeerUp)
	if up.Node != b.OwnerID() && up.Node == "" {
		t.Fatalf("peer.up carried no node identity: %+v", up)
	}
	if up.Peer == "" {
		t.Fatalf("peer.up carried no address: %+v", up)
	}
	if up.At.IsZero() {
		t.Fatalf("peer.up carried no timestamp: %+v", up)
	}

	b.Stop()

	down := waitForEvent(t, events, EventPeerDown)
	if down.Node != up.Node {
		t.Fatalf("peer.down was for %q, want %q", down.Node, up.Node)
	}
}

// Storing a file is announced, with the digest and the number of peers that
// took a copy.
func TestStoreEventIsPublished(t *testing.T) {
	a := newTestNode(t)
	events, cancel := a.Subscribe(32)
	defer cancel()

	b := newTestNode(t, a.addr)
	waitForPeerCount(t, a, 1)
	_ = b

	payload := []byte("an event-bearing file")
	if err := a.Store("notes.txt", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	e := waitForEvent(t, events, EventFileStored)
	if e.Name != "notes.txt" {
		t.Fatalf("file.stored named %q", e.Name)
	}
	if e.Digest == "" {
		t.Fatalf("file.stored carried no digest: %+v", e)
	}
	if e.Size != int64(len(payload)) {
		t.Fatalf("file.stored size = %d, want %d", e.Size, len(payload))
	}
	if e.Count != 1 {
		t.Fatalf("file.stored replicated to %d peers, want 1", e.Count)
	}
}

// The receiving node announces the file it was given, distinguishing an
// unsolicited push from a transfer it asked for. The trust rules turn on
// exactly that distinction, so the two must not collapse into one kind.
func TestReceiveAndFetchEventsAreDistinct(t *testing.T) {
	origin := newTestNode(t)
	pushed := newTestNode(t, origin.addr)
	waitForPeerCount(t, pushed, 1)

	pushedEvents, cancelPushed := pushed.Subscribe(32)
	defer cancelPushed()

	payload := []byte("a file with an audience")
	if err := origin.Store("shared.txt", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// The replica was pushed at it, unsolicited.
	got := waitForEvent(t, pushedEvents, EventFileReceived)
	if got.Name != "shared.txt" {
		t.Fatalf("file.received named %q, want shared.txt", got.Name)
	}
	if got.Node == "" {
		t.Fatalf("file.received did not say who sent it: %+v", got)
	}
	if got.Size != int64(len(payload)) {
		t.Fatalf("file.received size = %d, want %d", got.Size, len(payload))
	}

	// A third node joining afterwards has never seen the file, so its copy
	// arrives because it asked.
	fetcher := newTestNode(t, origin.addr)
	waitForPeerCount(t, fetcher, 1)

	fetcherEvents, cancelFetcher := fetcher.Subscribe(32)
	defer cancelFetcher()

	_, r, err := fetcher.Get("shared.txt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatalf("reading the fetched file: %v", err)
	}

	fetched := waitForEvent(t, fetcherEvents, EventFileFetched)
	if fetched.Name != "shared.txt" {
		t.Fatalf("file.fetched named %q, want shared.txt", fetched.Name)
	}
	if fetched.Digest != got.Digest {
		t.Fatalf("file.fetched digest %q differs from the pushed %q", fetched.Digest, got.Digest)
	}
}

// waitForEvent reads until an event of the wanted kind arrives.
func waitForEvent(t *testing.T, events <-chan Event, kind string) Event {
	t.Helper()

	deadline := time.After(20 * time.Second)
	for {
		select {
		case e, ok := <-events:
			if !ok {
				t.Fatalf("the event stream closed before a %s arrived", kind)
			}
			if e.Kind == kind {
				return e
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a %s event", kind)
		}
	}
}
