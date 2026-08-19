package p2p

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MaxMessageSize bounds a single framed message.
//
// The length prefix arrives from an unauthenticated peer, so it must never be
// used to size an allocation unchecked. A negative value panics make, and a
// large one lets any peer that can complete a TCP handshake exhaust the
// node's memory.
const MaxMessageSize = 8 << 20 // 8 MiB

type Decoder interface {
	Decode(io.Reader, *RPC) error
}

type DefaultDecoder struct{}

func (decoder DefaultDecoder) Decode(r io.Reader, msg *RPC) error {
	// io.ReadFull rather than Read: a Read may legally return (0, nil), which
	// would be misread as a frame tag of zero.
	tag := make([]byte, 1)
	if _, err := io.ReadFull(r, tag); err != nil {
		return err
	}

	switch tag[0] {
	case IncomingStream:
		msg.Stream = true
		return nil
	case IncomingMessage:
		// handled below
	default:
		// Anything else means the stream is out of sync with the sender.
		// Treating it as a message would compound the desync.
		return fmt.Errorf("unknown frame tag 0x%02x", tag[0])
	}

	var length int64
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return err
	}
	if length < 0 || length > MaxMessageSize {
		return fmt.Errorf("message length %d out of range (max %d)", length, MaxMessageSize)
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}

	msg.Payload = buf

	return nil
}
