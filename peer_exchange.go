package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (s *FileServer) handleMessagePeerExchange(from string, msg MessagePeerExchange) error {
	fmt.Printf("[%s] Received peer exchange with %d peers from %s\n", s.Transport.Address(), len(msg.Peers), from)

	go s.discoverPeers(msg.Peers)

	return nil
}

func (s *FileServer) discoverPeers(peers []PeerInfo) {
	myAddr := s.Transport.Address()
	const maxAttempts = 10

	attempted := 0
	connected := 0

	for _, peerInfo := range peers {
		if attempted >= maxAttempts {
			break
		}

		if peerInfo.Address == "" {
			continue
		}
		// Gossip eventually hands this node its own address back. Identity
		// catches that reliably; the handshake rejects it as a backstop for
		// entries that predate node ids.
		if peerInfo.NodeID != "" && peerInfo.NodeID == s.NodeID() {
			continue
		}

		if _, alreadyConnected := s.peer(peerInfo.Address); alreadyConnected {
			continue
		}
		if s.hasPeerWithNodeID(peerInfo.NodeID) {
			continue
		}

		// Counted whether or not the dial succeeds. Only counting successes
		// let a list of dead addresses run far past the limit.
		attempted++

		if err := s.Transport.Dial(peerInfo.Address); err != nil {
			continue
		}

		fmt.Printf("[%s] Connected to discovered peer %s\n", myAddr, peerInfo.Address)
		connected++

		select {
		case <-time.After(100 * time.Millisecond):
		case <-s.quitch:
			return
		}
	}

	if connected > 0 {
		fmt.Printf("[%s] Peer discovery: connected to %d new peer(s)\n", myAddr, connected)
	}
}

func (s *FileServer) sendPeerExchange(peerAddr string) error {
	if s.DB == nil {
		return nil
	}

	activePeers, err := s.DB.GetActivePeers(context.Background(), 30*time.Minute, 50)
	if err != nil {
		fmt.Printf("[%s] Error getting active peers: %v\n", s.Transport.Address(), err)
		return err
	}

	peerInfos := make([]PeerInfo, 0, len(activePeers))
	for _, p := range activePeers {
		if p.LastSeen != nil {
			peerInfos = append(peerInfos, PeerInfo{
				Address:  p.Address,
				NodeID:   p.NodeID,
				LastSeen: *p.LastSeen,
			})
		}
	}

	fmt.Printf("[%s] Sending %d peer(s) to %s\n", s.Transport.Address(), len(peerInfos), peerAddr)

	peer, ok := s.peer(peerAddr)
	if !ok {
		return fmt.Errorf("peer %s not found in connected peers", peerAddr)
	}

	msg := Message{
		Payload: MessagePeerExchange{
			Peers: peerInfos,
		},
	}

	if err := sendMessage(peer, &msg); err != nil && !isExpectedNetworkError(err) {
		return err
	}

	return nil
}

// Checks for expected network errors that don't need logging
func isExpectedNetworkError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	expectedErrors := []string{
		"broken pipe",
		"use of closed network connection",
		"connection reset by peer",
		"EOF",
	}

	for _, expected := range expectedErrors {
		if strings.Contains(errMsg, expected) {
			return true
		}
	}

	return false
}
