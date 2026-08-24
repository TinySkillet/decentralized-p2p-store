package node

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
)

// This file caches replication measurements so a person can be shown how well
// replicated their files are without waiting for the network.
//
// The measurement itself is expensive: checkFile asks every connected peer and
// waits up to offerTimeout for the answers, so ReplicationStatus costs
// O(files) sequential round-trips and is unusable behind a refresh. But the
// repair cycle already performs exactly that measurement on every file it
// examines and then throws the result away. The cache is that result, kept.
//
// **Invalidation is by direct call, not by the event bus.** The bus drops
// events for a subscriber that is not keeping up — that is what stops a
// stalled browser tab stalling the node — so anything built on it may miss a
// message. Missing a UI update is a cosmetic fault; missing an invalidation
// would leave the cache reporting copies that no longer exist, and a file
// would look safe while it was not. The bus is for display, never for
// correctness.

// measurement is one observation of how widely a blob is held.
//
// Keyed by digest rather than name because a blob may carry several names and
// they all share its copies: measuring per name would ask the same question
// several times and could hold contradictory answers.
type measurement struct {
	copies    int
	target    int
	holders   []string
	untrusted []string
	at        time.Time
}

// healthCache remembers the most recent measurement for each blob.
type healthCache struct {
	mu      sync.Mutex
	entries map[string]measurement

	// refused records peers known to have thrown a copy away, per blob.
	//
	// It exists because the two facts arrive in an unpredictable order. A
	// sender records its optimistic count when the transfer completes, and the
	// receiver's refusal travels back independently — so the refusal can
	// arrive first, and subtracting it there would correct an entry that has
	// not been written yet. Remembering it instead makes the outcome the same
	// either way.
	refused map[string]map[string]bool
}

func newHealthCache() *healthCache {
	return &healthCache{
		entries: make(map[string]measurement),
		refused: make(map[string]map[string]bool),
	}
}

// record stores an optimistic count, one derived from transfers appearing to
// succeed rather than from asking. Holders known to have refused are removed,
// whichever order the two arrived in.
func (c *healthCache) record(digest string, m measurement) {
	if digest == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if refused := c.refused[digest]; len(refused) > 0 {
		kept := m.holders[:0:0]
		for _, h := range m.holders {
			if !refused[h] {
				kept = append(kept, h)
			}
		}
		m.copies -= len(m.holders) - len(kept)
		if m.copies < 0 {
			m.copies = 0
		}
		m.holders = kept
	}

	c.entries[digest] = m
}

// recordMeasured stores a count obtained by asking every peer directly.
//
// That answer supersedes any remembered refusal: a peer that once threw a copy
// away and now says it holds one has been approved since, or was sent it
// again. Clearing the refusals here is also what stops them accumulating.
func (c *healthCache) recordMeasured(digest string, m measurement) {
	if digest == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.refused, digest)
	c.entries[digest] = m
}

// markRefused records that a peer did not keep a copy of a blob.
func (c *healthCache) markRefused(digest, nodeID string) {
	if digest == "" || nodeID == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.refused[digest] == nil {
		c.refused[digest] = make(map[string]bool)
	}
	c.refused[digest][nodeID] = true

	c.dropHolderLocked(digest, nodeID)
}

func (c *healthCache) lookup(digest string) (measurement, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.entries[digest]
	return m, ok
}

func (c *healthCache) forget(digest string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, digest)
	delete(c.refused, digest)
}

// dropHolder removes a peer from every measurement it appeared in.
//
// A peer disconnecting is the one event that makes a cached count wrong in the
// dangerous direction — a file would read as better replicated than it is — so
// it is corrected precisely rather than left to age out.
func (c *healthCache) dropHolder(nodeID string) {
	if nodeID == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for digest, m := range c.entries {
		kept := m.holders[:0:0]
		for _, h := range m.holders {
			if h != nodeID {
				kept = append(kept, h)
			}
		}
		if len(kept) == len(m.holders) {
			continue
		}
		m.copies -= len(m.holders) - len(kept)
		if m.copies < 0 {
			m.copies = 0
		}
		m.holders = kept

		// The departed peer must leave the untrusted list too, or a file
		// would keep reporting a fragility that no longer applies.
		stillUntrusted := m.untrusted[:0:0]
		for _, h := range m.untrusted {
			if h != nodeID {
				stillUntrusted = append(stillUntrusted, h)
			}
		}
		m.untrusted = stillUntrusted

		c.entries[digest] = m
	}
}

// dropHolderFor removes one peer from one blob's measurement.
//
// Narrower than dropHolder, which is for a peer that has gone away entirely.
// A refused push says nothing about the other files that peer holds.
func (c *healthCache) dropHolderFor(digest, nodeID string) {
	if digest == "" || nodeID == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropHolderLocked(digest, nodeID)
}

// dropHolderLocked removes one peer from one blob's measurement. The caller
// holds the lock.
func (c *healthCache) dropHolderLocked(digest, nodeID string) {
	m, ok := c.entries[digest]
	if !ok {
		return
	}

	kept := m.holders[:0:0]
	for _, h := range m.holders {
		if h != nodeID {
			kept = append(kept, h)
		}
	}
	if len(kept) == len(m.holders) {
		return
	}

	m.copies -= len(m.holders) - len(kept)
	if m.copies < 0 {
		m.copies = 0
	}
	m.holders = kept

	stillUntrusted := m.untrusted[:0:0]
	for _, h := range m.untrusted {
		if h != nodeID {
			stillUntrusted = append(stillUntrusted, h)
		}
	}
	m.untrusted = stillUntrusted

	c.entries[digest] = m
}

func (c *healthCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// recordOptimisticHealth caches a count derived from transfers that appeared
// to succeed, rather than from asking.
func (s *FileServer) recordOptimisticHealth(h FileHealth) {
	if s.health == nil {
		return
	}
	s.health.record(h.Digest, measurement{
		copies:    h.Copies,
		target:    h.Target,
		holders:   append([]string(nil), h.Holders...),
		untrusted: append([]string(nil), h.UntrustedHolders...),
		at:        time.Now(),
	})
}

// ReplicaSnapshot is a file's replication as last measured. It carries the age
// of the measurement because a stale number presented as current is worse than
// no number at all.
type ReplicaSnapshot struct {
	FileHealth

	// MeasuredAt is when the copies were last counted, zero if never.
	MeasuredAt time.Time
}

// Measured reports whether this file has ever been checked.
func (r ReplicaSnapshot) Measured() bool { return !r.MeasuredAt.IsZero() }

// Age is how long ago the measurement was taken, and is meaningless unless
// Measured reports true.
func (r ReplicaSnapshot) Age() time.Duration {
	if !r.Measured() {
		return 0
	}
	return time.Since(r.MeasuredAt)
}

// ReplicationSnapshot reports the replication of every file from cached
// measurements, without touching the network.
//
// A file that has never been measured is returned with Measured false rather
// than omitted or reported as zero copies: "not checked yet" and "no copies"
// are different facts, and conflating them would either hide files or raise
// false alarms.
func (s *FileServer) ReplicationSnapshot(ctx context.Context) ([]ReplicaSnapshot, error) {
	if s.DB == nil {
		return nil, nil
	}

	files, err := s.DB.ListFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing files: %w", err)
	}

	out := make([]ReplicaSnapshot, 0, len(files))
	for _, f := range files {
		if !s.store.Has(f.Hash) {
			continue
		}

		snap := ReplicaSnapshot{FileHealth: FileHealth{
			Name:   f.Name,
			Digest: f.Hash,
			Size:   f.Size,
			Target: s.ReplicationFactor,
		}}

		if m, ok := s.health.lookup(f.Hash); ok {
			snap.Copies = m.copies
			snap.Holders = m.holders
			snap.UntrustedHolders = m.untrusted
			snap.MeasuredAt = m.at
			if m.target > 0 {
				snap.Target = m.target
			}
		}

		out = append(out, snap)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Recheck measures one file now and returns the fresh result.
//
// Bounded by a single offer round, unlike ReplicationStatus which pays that
// cost once per file, so this is the one measurement safe to put behind a
// button.
func (s *FileServer) Recheck(ctx context.Context, name string) (ReplicaSnapshot, error) {
	if s.DB == nil {
		return ReplicaSnapshot{}, fmt.Errorf("no database")
	}

	files, err := s.DB.ListFiles(ctx)
	if err != nil {
		return ReplicaSnapshot{}, fmt.Errorf("listing files: %w", err)
	}

	var found *dbpkg.File
	for i := range files {
		if files[i].Name == name {
			found = &files[i]
			break
		}
	}
	if found == nil {
		return ReplicaSnapshot{}, fmt.Errorf("no file named %q is stored here", name)
	}
	if !s.store.Has(found.Hash) {
		return ReplicaSnapshot{}, fmt.Errorf("%q is recorded but its contents are not held here", name)
	}

	// checkFile records the measurement as a side effect, so the cache is
	// refreshed by the same call that answers.
	health, _, err := s.checkFile(*found, s.ReplicationFactor)
	if err != nil {
		return ReplicaSnapshot{}, err
	}

	snap := ReplicaSnapshot{FileHealth: health}
	if m, ok := s.health.lookup(found.Hash); ok {
		snap.MeasuredAt = m.at
	}
	return snap, nil
}
