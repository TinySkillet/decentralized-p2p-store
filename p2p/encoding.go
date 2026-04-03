package p2p

import (
	"encoding/binary"
	"io"
)

type Decoder interface {
	Decode(io.Reader, *RPC) error
}

type DefaultDecoder struct{}

func (decoder DefaultDecoder) Decode(r io.Reader, msg *RPC) error {

	peekBuf := make([]byte, 1)
	if _, err := r.Read(peekBuf); err != nil {
		return err
	}

	stream := peekBuf[0] == IncomingStream
	if stream {
		msg.Stream = true
		return nil
	}

	var length int64
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return err
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}

	msg.Payload = buf

	return nil
}
