package node

import (
	"context"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
)

// This file is the read model: everything a person, or a UI acting for one,
// should be shown about the node. It answers from memory and the database and
// never touches the network, so it is safe behind a refresh or a poll.
//
// The rule that shapes it: **liveness is membership of the live peer set, never
// the peers.status column.** Status is written only on connect and disconnect,
// so a node that crashed reads "connected" for ever, and GetActivePeers hides
// the 4th and later identity on a host as an anti-Sybil measure. A view built
// from SQLite would therefore show peers that are gone and hide peers that are
// there — the two failures that matter most.

// NodeView describes this node itself.
type NodeView struct {
	NodeID  string
	OwnerID string

	// Address is where the transport is listening.
	Address string

	ReplicationFactor int

	// Peers counts live connections, not database rows.
	Peers int

	Files int
	Bytes int64
}

// PeerView is one peer as it should be shown.
type PeerView struct {
	NodeID string

	// Address is where this node reached the peer if it is online, and the
	// last address recorded for it otherwise.
	Address string

	// Addrs are other places the peer says it is reachable.
	Addrs []string

	// Online is membership of the live peer set. It is the only liveness
	// signal in this struct; the database cannot supply one.
	Online bool

	// LastSeen comes from the database and is nil for a peer that has
	// connected but not yet been recorded.
	LastSeen *time.Time
}

// Host is the peer's address without its port, or the empty string if the
// address is not a host:port pair. Peers sharing a host are worth showing
// together: that is what the per-host cap is defending against.
func (p PeerView) Host() string {
	host, _, err := net.SplitHostPort(p.Address)
	if err != nil {
		return ""
	}
	return host
}

// Short is the abbreviated node id, for display.
func (p PeerView) Short() string { return storage.Short(p.NodeID) }

// FileView is one stored file as it should be shown.
type FileView struct {
	Name   string
	Digest string
	Size   int64

	// Owner is the node id that stored the file, empty for files predating
	// ownership. Mine reports whether that owner is this node, which is what
	// decides if deletion can be authorised here.
	Owner string
	Mine  bool

	CreatedAt time.Time
}

// Short is the abbreviated digest, for display.
func (f FileView) Short() string { return storage.Short(f.Digest) }

// NodeView reports this node's own state. It does not touch the network.
func (s *FileServer) NodeView(ctx context.Context) (NodeView, error) {
	v := NodeView{
		NodeID:            s.NodeID(),
		OwnerID:           s.OwnerID(),
		Address:           s.Transport.Address(),
		ReplicationFactor: s.ReplicationFactor,
		Peers:             s.peerCount(),
	}

	if s.DB == nil {
		return v, nil
	}

	files, err := s.DB.ListFiles(ctx)
	if err != nil {
		return v, fmt.Errorf("listing files: %w", err)
	}
	v.Files = len(files)
	for _, f := range files {
		v.Bytes += f.Size
	}
	return v, nil
}

// PeerViews reports every peer this node knows about: those connected now, and
// those only the database remembers.
//
// A peer is online if and only if it is in the live set. A database row saying
// "connected" for a node that is not there is reported as offline, which is the
// whole point of building the view this way round.
func (s *FileServer) PeerViews(ctx context.Context) ([]PeerView, error) {
	live, ids := s.connectedPeers()

	views := make(map[string]*PeerView, len(live))
	for _, id := range ids {
		p := live[id]
		views[id] = &PeerView{
			NodeID:  id,
			Address: peerAddress(p),
			Addrs:   advertisedAddrs(p),
			Online:  true,
		}
	}

	// The database contributes peers that are offline, and last-seen times for
	// those that are not. It never contributes liveness.
	if s.DB != nil {
		known, err := s.DB.ListKnownPeers(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing known peers: %w", err)
		}
		for _, k := range known {
			v, ok := views[k.NodeID]
			if !ok {
				v = &PeerView{NodeID: k.NodeID, Address: k.Address, Addrs: k.Addrs}
				views[k.NodeID] = v
			}
			v.LastSeen = k.LastSeen

			// A live peer keeps the address it was actually reached at; the
			// recorded one may be stale. For an offline peer the record is all
			// there is.
			if !v.Online && len(v.Addrs) == 0 {
				v.Addrs = k.Addrs
			}
		}
	}

	out := make([]PeerView, 0, len(views))
	for _, v := range views {
		out = append(out, *v)
	}

	// Online first, then most recently seen, then by id so the order is total
	// and a refresh does not shuffle the list.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Online != b.Online {
			return a.Online
		}
		at, bt := timeOrZero(a.LastSeen), timeOrZero(b.LastSeen)
		if !at.Equal(bt) {
			return at.After(bt)
		}
		return a.NodeID < b.NodeID
	})
	return out, nil
}

// FileViews reports the files recorded here, newest first.
func (s *FileServer) FileViews(ctx context.Context) ([]FileView, error) {
	if s.DB == nil {
		return nil, nil
	}

	files, err := s.DB.ListFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing files: %w", err)
	}

	owner := s.OwnerID()
	out := make([]FileView, 0, len(files))
	for _, f := range files {
		out = append(out, FileView{
			Name:      f.Name,
			Digest:    f.Hash,
			Size:      f.Size,
			Owner:     f.Owner,
			Mine:      f.Owner == owner,
			CreatedAt: f.CreatedAt,
		})
	}
	return out, nil
}

// timeOrZero dereferences a possibly-nil timestamp.
func timeOrZero(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
