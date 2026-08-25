// Wire messages and the framing that carries them.
package node

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
	gob.Register(MessageStoreRefused{})
	gob.Register(MessageTrustGranted{})
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

// MessageStoreRefused tells a sender that a file it pushed was not kept.
//
// Without it the sender cannot tell acceptance from refusal: a refused push
// still drains the body and closes the stream cleanly — deliberately, because
// anything else wedges the connection — so the write succeeds either way. The
// sender would then count a copy that does not exist, and a file at risk would
// read as safe. That is the failure this whole project keeps finding, so the
// receiver says so explicitly.
type MessageStoreRefused struct {
	Name   string
	Digest string

	// Reason is for the sender's log and event feed. It is not acted on: the
	// refusal itself is what matters.
	Reason string
}

// MessageTrustGranted tells a peer this node has approved it.
//
// Two operators approving each other do so one after the other, and the first
// approval places nothing: at that moment the other side still refuses. Without
// this the copies wait for the next repair cycle, five minutes later, so the
// ordinary first-run flow — two people approving each other — appears to do
// nothing at all.
//
// It does tell the peer something about this node's trust state. That is
// information it can establish anyway by pushing a file and seeing whether it
// is kept, and it concerns only the peer being told.
type MessageTrustGranted struct{}

type MessagePeerExchange struct {
	Peers []PeerInfo
}

type PeerInfo struct {
	Address  string
	NodeID   string
	LastSeen time.Time
}
