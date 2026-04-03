package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

func (s *FileServer) handleMessagePeerExchange(from string, msg MessagePeerExchange) error {
	fmt.Printf("[%s] Received peer exchange with %d peers from %s\n", s.Transport.Address(), len(msg.Peers), from)

	go s.discoverPeers(msg.Peers)

	return nil
}

func (s *FileServer) discoverPeers(peers []PeerInfo) {
	myAddr := s.Transport.Address()
	maxAttempts := 10
	attempted := 0
	connected := 0

	for _, peerInfo := range peers {
		if attempted >= maxAttempts {
			break
		}

		if peerInfo.Address == myAddr {
			continue
		}

		s.peersLock.Lock()
		_, alreadyConnected := s.peers[peerInfo.Address]
		s.peersLock.Unlock()

		if alreadyConnected {
			continue
		}

		err := s.Transport.Dial(peerInfo.Address)
		if err == nil {
			fmt.Printf("[%s] Connected to discovered peer %s\n", myAddr, peerInfo.Address)
			connected++
			attempted++
			time.Sleep(100 * time.Millisecond)
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
				LastSeen: *p.LastSeen,
			})
		}
	}

	fmt.Printf("[%s] Sending %d peer(s) to %s\n", s.Transport.Address(), len(peerInfos), peerAddr)

	msg := Message{
		Payload: MessagePeerExchange{
			Peers: peerInfos,
		},
	}

	s.peersLock.Lock()
	peer, ok := s.peers[peerAddr]
	s.peersLock.Unlock()

	if !ok {
		return fmt.Errorf("peer %s not found in connected peers", peerAddr)
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	peer.Send([]byte{p2p.IncomingMessage})
	binary.Write(peer, binary.LittleEndian, int64(buf.Len()))
	err = peer.Send(buf.Bytes())

	if err != nil && !isExpectedNetworkError(err) {
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
