// Wire messages and the framing that carries them.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"time"

	"github.com/TinySkillet/DecentralizedP2PStorage/p2p"
)

// encodeMessage renders msg as a single wire frame.
//
// Tag, length and payload are emitted together so that a concurrent send on
// the same connection cannot land between them and corrupt the frame.
func encodeMessage(msg *Message) ([]byte, error) {
	payload := new(bytes.Buffer)
	if err := gob.NewEncoder(payload).Encode(msg); err != nil {
		return nil, err
	}
	if payload.Len() > p2p.MaxMessageSize {
		return nil, fmt.Errorf("message of %d bytes exceeds the %d byte limit", payload.Len(), p2p.MaxMessageSize)
	}

	frame := new(bytes.Buffer)
	frame.WriteByte(p2p.IncomingMessage)
	if err := binary.Write(frame, binary.LittleEndian, int64(payload.Len())); err != nil {
		return nil, err
	}
	frame.Write(payload.Bytes())

	return frame.Bytes(), nil
}

// sendMessage writes msg to peer as a single frame.
func sendMessage(peer p2p.Peer, msg *Message) error {
	frame, err := encodeMessage(msg)
	if err != nil {
		return err
	}
	return peer.Send(frame)
}

// sendFile announces a file and streams its body as one indivisible transfer.
func sendFile(peer p2p.Peer, msg *Message, body io.Reader) (int64, error) {
	frame, err := encodeMessage(msg)
	if err != nil {
		return 0, err
	}
	return peer.SendStream(frame, body)
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
	gob.Register(MessageFileOffer{})
	gob.Register(MessageFetchFile{})
	gob.Register(MessageDeleteFile{})
	gob.Register(MessagePeerExchange{})
	gob.Register(PeerInfo{})
	gob.Register(Handshake{})
	gob.Register(HandshakeProof{})
}

type Message struct {
	Payload any
}

// MessageStoreFile announces that a file stream follows immediately on the
// same connection. RequestID is empty when the file is pushed unsolicited by
// a Store, and echoes the requester's id when it answers a fetch.
//
// Digest identifies the contents; Name travels with it so the receiving node
// can list what it holds under the name its owner gave it.
type MessageStoreFile struct {
	RequestID string
	Name      string
	Digest    string
	Size      int64

	// Owner is the node that stored the file. It travels with every copy so
	// that a peer holding a replica knows who is entitled to delete it.
	Owner string
}

// MessageGetFile asks every peer whether it holds anything under Name.
type MessageGetFile struct {
	RequestID string
	Name      string
}

// MessageFileOffer answers a MessageGetFile, resolving the name to the
// contents the answering peer holds for it. Peers that hold nothing answer
// too, with Have false, so the requester can give up as soon as every peer
// has spoken instead of waiting out the timeout.
type MessageFileOffer struct {
	RequestID string
	Name      string
	Have      bool
	Digest    string
	Size      int64
}

// MessageFetchFile asks the single peer chosen from the offers to send the
// contents. It names a digest rather than a file name: the requester knows
// exactly which bytes it expects, and can reject anything else.
type MessageFetchFile struct {
	RequestID string
	Name      string
	Digest    string
}

// MessageDeleteFile asks peers to forget a name.
//
// Digest names the contents being deleted. Two nodes may legitimately use the
// same name for different files, so a peer only acts when its own name refers
// to the same contents.
//
// Signature is the owner's authorisation, over the name and digest together.
// Without it, reaching a peer would be enough to destroy anything it holds.
type MessageDeleteFile struct {
	Name      string
	Digest    string
	Owner     string
	Signature []byte
}

type MessagePeerExchange struct {
	Peers []PeerInfo
}

type PeerInfo struct {
	Address  string
	NodeID   string
	LastSeen time.Time
}
