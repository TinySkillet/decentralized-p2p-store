package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
)

const (
	// DefaultReplicationFactor is how many copies of a file the network aims
	// to hold, counting the node that stored it.
	DefaultReplicationFactor = 3

	// DefaultRepairInterval is how often a node checks the files it holds and
	// restores any that have fallen below the target.
	DefaultRepairInterval = 5 * time.Minute

	// maxRepairsPerCycle bounds the work one cycle does, so a node holding
	// thousands of files spreads the checking out instead of flooding its
	// peers with availability queries in one burst.
	maxRepairsPerCycle = 25
)

// FileHealth reports how well replicated one file is.
type FileHealth struct {
	Name   string
	Digest string
	Size   int64

	// Copies counts every node known to hold the file, including this one.
	Copies int
	Target int

	// Holders are the peers that answered yes, excluding this node.
	Holders []string
}

// AtRisk reports whether the file has fewer copies than the target.
func (h FileHealth) AtRisk() bool { return h.Copies < h.Target }

// repairLoop periodically restores files that have fallen below the
// replication target.
//
// Replication happens when a file is stored, to whichever peers are connected
// at that moment. Those peers leave, and nothing until now noticed. This is
// what turns a one-off push into a durability guarantee.
func (s *FileServer) repairLoop() {
	for {
		// Jitter keeps every node in the network from running its cycle at
		// the same instant and asking each other simultaneously.
		wait := s.RepairInterval + time.Duration(rand.Int63n(int64(s.RepairInterval/2)))

		select {
		case <-time.After(wait):
		case <-s.quitch:
			return
		}

		if repaired, err := s.RepairOnce(); err != nil {
			log.Printf("[%s] Repair cycle failed: %v", s.Transport.Address(), err)
		} else if repaired > 0 {
			fmt.Printf("[%s] Repair: restored %d missing replica(s)\n", s.Transport.Address(), repaired)
		}
	}
}

// RepairOnce checks the files this node holds and pushes copies of any that
// are under-replicated. It returns how many replicas it created.
func (s *FileServer) RepairOnce() (int, error) {
	if s.DB == nil {
		return 0, nil
	}
	if s.peerCount() == 0 {
		// Nothing can be repaired with nobody to repair to.
		return 0, nil
	}

	files, err := s.DB.ListFiles(context.Background())
	if err != nil {
		return 0, err
	}

	// One blob can carry several names; checking it once per cycle is enough.
	seen := make(map[string]bool, len(files))

	repaired := 0
	checked := 0
	for _, f := range files {
		if checked >= maxRepairsPerCycle {
			break
		}
		if seen[f.Hash] || !s.store.Has(f.Hash) {
			continue
		}
		seen[f.Hash] = true
		checked++

		n, err := s.repairFile(f)
		if err != nil {
			log.Printf("[%s] Could not repair %s: %v", s.Transport.Address(), short(f.Hash), err)
			continue
		}
		repaired += n

		select {
		case <-s.quitch:
			return repaired, nil
		default:
		}
	}

	return repaired, nil
}

// repairFile brings one file back up to the replication target, and reports
// how many new copies it placed.
func (s *FileServer) repairFile(f dbpkg.File) (int, error) {
	health, lacking, err := s.checkFile(f)
	if err != nil {
		return 0, err
	}
	if !health.AtRisk() {
		return 0, nil
	}

	needed := health.Target - health.Copies
	fmt.Printf("[%s] '%s' has %d of %d copies, placing %d more\n",
		s.Transport.Address(), f.Name, health.Copies, health.Target, needed)

	ownerID := s.storageOwnerID()

	placed := 0
	for _, addr := range lacking {
		if placed >= needed {
			break
		}
		if s.isStorageOwner(addr, ownerID) {
			// Pushing to the node whose storage this one is borrowing would
			// write the file back where it already is.
			continue
		}
		if err := s.pushTo(addr, f); err != nil {
			log.Printf("[%s] Could not place a copy of %s on %s: %v", s.Transport.Address(), short(f.Hash), addr, err)
			continue
		}
		placed++
	}

	if placed < needed {
		// Worth saying out loud: the network is too small to reach the
		// target, and no amount of retrying will change that.
		fmt.Printf("[%s] '%s' is still short of its target: only %d peer(s) available to take a copy\n",
			s.Transport.Address(), f.Name, placed)
	}

	return placed, nil
}

// checkFile asks every connected peer whether it holds the file, and splits
// them into those that do and those that could take a copy.
//
// A peer that answers with a different digest for the same name is counted as
// lacking this file: it holds something else under that name, not this.
func (s *FileServer) checkFile(f dbpkg.File) (FileHealth, []string, error) {
	health := FileHealth{
		Name:   f.Name,
		Digest: f.Hash,
		Size:   f.Size,
		Target: s.ReplicationFactor,
	}

	// A command borrows the storage of the node that owns the database. Its
	// copy of a file is that node's copy, so it only counts as an independent
	// one when that node is not itself answering.
	ownerID := s.storageOwnerID()

	requestID, err := newRequestID()
	if err != nil {
		return health, nil, err
	}

	peers, addrs := s.connectedPeers()
	if len(addrs) == 0 {
		health.Copies = 1
		return health, nil, nil
	}

	req, release := s.newRequest(requestID, f.Name, len(addrs))
	defer release()

	query := Message{Payload: MessageGetFile{RequestID: requestID, Name: f.Name}}
	asked := 0
	for _, addr := range addrs {
		if err := sendMessage(peers[addr], &query); err != nil {
			continue
		}
		asked++
	}

	var lacking []string
	ownerAnswered := false
	for _, reply := range s.collectOffers(req, asked) {
		if reply.offer.Have && reply.offer.Digest == f.Hash {
			health.Copies++
			health.Holders = append(health.Holders, reply.from)
			if s.isStorageOwner(reply.from, ownerID) {
				ownerAnswered = true
			}
			continue
		}
		lacking = append(lacking, reply.from)
	}

	// Count this node's own copy unless it is the borrowed one already
	// reported by the node that owns the storage.
	if !ownerAnswered {
		health.Copies++
	}

	return health, lacking, nil
}

// collectOffers gathers every reply to a request, rather than stopping at the
// first peer that holds the file.
func (s *FileServer) collectOffers(req *fileRequest, asked int) []peerOffer {
	out := make([]peerOffer, 0, asked)
	deadline := time.After(offerTimeout)

	for len(out) < asked {
		select {
		case reply := <-req.offers:
			out = append(out, reply)
		case <-deadline:
			return out
		case <-s.quitch:
			return out
		}
	}
	return out
}

// pushTo sends one peer a copy of a file it does not have.
func (s *FileServer) pushTo(addr string, f dbpkg.File) error {
	peer, ok := s.peer(addr)
	if !ok {
		return fmt.Errorf("peer %s is no longer connected", addr)
	}

	_, body, err := s.store.ReadDecrypt(s.EncryptionKey, f.Hash)
	if err != nil {
		return err
	}
	if rc, ok := body.(io.Closer); ok {
		defer rc.Close()
	}

	msg := Message{
		Payload: MessageStoreFile{
			Name:   f.Name,
			Digest: f.Hash,
			Size:   f.Size,
		},
	}

	if _, err := sendFile(peer, &msg, body); err != nil {
		return err
	}

	if s.DB != nil {
		shareID := contentKey([]byte(f.Hash + addr + "outgoing"))
		if err := s.DB.InsertShare(context.Background(), dbpkg.Share{
			ID:        shareID,
			FileID:    f.ID,
			PeerID:    addr,
			Direction: "outgoing",
		}); err != nil {
			log.Printf("[%s] Failed to record outgoing share to %s: %v", s.Transport.Address(), addr, err)
		}
	}

	return nil
}

// ReplicationStatus reports how well replicated every file this node holds is.
func (s *FileServer) ReplicationStatus() ([]FileHealth, error) {
	if s.DB == nil {
		return nil, nil
	}

	files, err := s.DB.ListFiles(context.Background())
	if err != nil {
		return nil, err
	}

	out := make([]FileHealth, 0, len(files))
	for _, f := range files {
		if !s.store.Has(f.Hash) {
			continue
		}
		health, _, err := s.checkFile(f)
		if err != nil {
			return nil, err
		}
		out = append(out, health)
	}
	return out, nil
}
