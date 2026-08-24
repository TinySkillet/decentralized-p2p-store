package node

import (
	"sync"
	"time"
)

// Event kinds.
const (
	EventPeerUp       = "peer.up"
	EventPeerDown     = "peer.down"
	EventPeerRefused  = "peer.refused"
	EventFileStored   = "file.stored"
	EventFileFetched  = "file.fetched"
	EventFileReceived = "file.received"
	EventFileDeleted  = "file.deleted"
	EventRepaired     = "repair.offered"
	EventReclaimed    = "sweep.reclaimed"
)

// Event is one thing that happened on this node.
//
// It is a single concrete struct with a Kind discriminator rather than an
// interface payload like Message, so it encodes without any type registration
// and a subscriber never has to type-switch to read the common fields.
type Event struct {
	Kind string
	At   time.Time

	// Node is the peer's identity, and Peer the address it was reached at.
	Node string
	Peer string

	Name   string
	Digest string
	Size   int64

	// Count carries whatever the kind counts: peers offered a copy, files
	// reclaimed, and so on.
	Count int

	// Err explains a refusal or failure, and is empty otherwise.
	Err string
}

// subscriber is one consumer of the bus.
type subscriber struct {
	ch chan Event

	// dropped counts events this subscriber missed because it was not reading
	// fast enough. Reported so it can say it missed updates rather than
	// quietly showing a stale picture.
	dropped int
}

// eventBus fans events out to subscribers.
//
// A send that would block is dropped for that subscriber alone. The node must
// never stall because something stopped reading — a browser tab that went away
// is the expected case, not an exceptional one.
type eventBus struct {
	mu   sync.Mutex
	next int
	subs map[int]*subscriber
}

func newEventBus() *eventBus {
	return &eventBus{subs: make(map[int]*subscriber)}
}

// subscribe returns a channel of events and a function that stops the
// subscription. The channel is closed when the subscription is cancelled, so a
// consumer ranging over it terminates.
func (b *eventBus) subscribe(buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 1
	}

	b.mu.Lock()
	id := b.next
	b.next++
	sub := &subscriber{ch: make(chan Event, buffer)}
	b.subs[id] = sub
	b.mu.Unlock()

	var once sync.Once
	return sub.ch, func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, id)
			b.mu.Unlock()
			close(sub.ch)
		})
	}
}

// publish delivers e to every subscriber. It never blocks.
//
// Callers must not hold peersLock: a subscriber may well ask the node about its
// peers on receiving an event, and that would deadlock.
func (b *eventBus) publish(e Event) {
	if e.At.IsZero() {
		e.At = time.Now()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, sub := range b.subs {
		select {
		case sub.ch <- e:
		default:
			sub.dropped++
		}
	}
}

// subscriberCount reports how many subscriptions are live, so a leak is
// visible to a test.
func (b *eventBus) subscriberCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}

// dropped reports how many events every subscriber has missed in total.
func (b *eventBus) dropped() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	total := 0
	for _, sub := range b.subs {
		total += sub.dropped
	}
	return total
}

// publish emits an event from the node, if a bus is configured.
func (s *FileServer) publish(e Event) {
	if s.events == nil {
		return
	}
	s.events.publish(e)
}

// Subscribe returns a stream of events from this node.
func (s *FileServer) Subscribe(buffer int) (<-chan Event, func()) {
	return s.events.subscribe(buffer)
}
