package p2p

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"strings"
	"testing"
)

// frame builds a wire-format message frame: tag, little-endian length, body.
func frame(tag byte, length int64, body []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(tag)
	binary.Write(&buf, binary.LittleEndian, length)
	buf.Write(body)
	return buf.Bytes()
}

func TestDecodeMessage(t *testing.T) {
	body := []byte("hello payload")

	var rpc RPC
	if err := (DefaultDecoder{}).Decode(bytes.NewReader(frame(IncomingMessage, int64(len(body)), body)), &rpc); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if rpc.Stream {
		t.Error("message frame decoded as a stream")
	}
	if !bytes.Equal(rpc.Payload, body) {
		t.Errorf("Payload = %q, want %q", rpc.Payload, body)
	}
}

func TestDecodeStream(t *testing.T) {
	var rpc RPC
	if err := (DefaultDecoder{}).Decode(bytes.NewReader([]byte{IncomingStream}), &rpc); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !rpc.Stream {
		t.Error("stream frame did not set Stream")
	}
	if rpc.Payload != nil {
		t.Error("stream frame should not carry a payload")
	}
}

func TestDecodeEmptyMessage(t *testing.T) {
	var rpc RPC
	if err := (DefaultDecoder{}).Decode(bytes.NewReader(frame(IncomingMessage, 0, nil)), &rpc); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rpc.Payload) != 0 {
		t.Errorf("Payload = %q, want empty", rpc.Payload)
	}
}

// TestDecodeRejectsHostileLengths is a regression test. The length prefix was
// passed straight to make(), so a peer could panic the node with a negative
// length or exhaust its memory with a huge one.
func TestDecodeRejectsHostileLengths(t *testing.T) {
	lengths := []struct {
		name   string
		length int64
	}{
		{"negative", -1},
		{"minInt64", math.MinInt64},
		{"maxInt64", math.MaxInt64},
		{"just over the cap", MaxMessageSize + 1},
		{"one exabyte", 1 << 60},
	}

	for _, tc := range lengths {
		t.Run(tc.name, func(t *testing.T) {
			// Only the header is supplied; a correct decoder rejects the
			// frame on the length alone, without waiting for a body it can
			// never receive.
			r := bytes.NewReader(frame(IncomingMessage, tc.length, nil))

			var rpc RPC
			err := (DefaultDecoder{}).Decode(r, &rpc)
			if err == nil {
				t.Fatalf("length %d was accepted, want an error", tc.length)
			}
			if !strings.Contains(err.Error(), "out of range") {
				t.Errorf("error = %v, want an out-of-range rejection", err)
			}
			if rpc.Payload != nil {
				t.Error("a rejected frame should not yield a payload")
			}
		})
	}
}

func TestDecodeAcceptsMaximumLength(t *testing.T) {
	// The cap itself must remain legal, so the check is not off by one.
	body := make([]byte, MaxMessageSize)

	var rpc RPC
	if err := (DefaultDecoder{}).Decode(bytes.NewReader(frame(IncomingMessage, MaxMessageSize, body)), &rpc); err != nil {
		t.Fatalf("Decode at the size cap: %v", err)
	}
	if len(rpc.Payload) != MaxMessageSize {
		t.Errorf("Payload length = %d, want %d", len(rpc.Payload), MaxMessageSize)
	}
}

func TestDecodeRejectsUnknownTag(t *testing.T) {
	var rpc RPC
	err := (DefaultDecoder{}).Decode(bytes.NewReader([]byte{0x7f, 0, 0, 0, 0, 0, 0, 0, 0}), &rpc)
	if err == nil {
		t.Fatal("unknown frame tag was accepted, want an error")
	}
	if !strings.Contains(err.Error(), "unknown frame tag") {
		t.Errorf("error = %v, want an unknown-tag rejection", err)
	}
}

func TestDecodeTruncatedFrames(t *testing.T) {
	body := []byte("payload")
	full := frame(IncomingMessage, int64(len(body)), body)

	for n := range len(full) {
		var rpc RPC
		if err := (DefaultDecoder{}).Decode(bytes.NewReader(full[:n]), &rpc); err == nil {
			t.Errorf("truncating to %d bytes was accepted, want an error", n)
		}
	}
}

func TestDecodeOnEmptyStreamReturnsEOF(t *testing.T) {
	var rpc RPC
	if err := (DefaultDecoder{}).Decode(bytes.NewReader(nil), &rpc); err != io.EOF {
		t.Errorf("error = %v, want io.EOF", err)
	}
}

func TestDecodeSequentialFrames(t *testing.T) {
	// The read loop reuses one connection, so frames must decode back to back
	// leaving the reader positioned exactly at the next frame.
	var wire bytes.Buffer
	wire.Write(frame(IncomingMessage, 3, []byte("one")))
	wire.WriteByte(IncomingStream)
	wire.Write(frame(IncomingMessage, 5, []byte("three")))

	r := bytes.NewReader(wire.Bytes())

	var first RPC
	if err := (DefaultDecoder{}).Decode(r, &first); err != nil {
		t.Fatalf("first Decode: %v", err)
	}
	if string(first.Payload) != "one" {
		t.Errorf("first payload = %q, want %q", first.Payload, "one")
	}

	var second RPC
	if err := (DefaultDecoder{}).Decode(r, &second); err != nil {
		t.Fatalf("second Decode: %v", err)
	}
	if !second.Stream {
		t.Error("second frame should be a stream")
	}

	var third RPC
	if err := (DefaultDecoder{}).Decode(r, &third); err != nil {
		t.Fatalf("third Decode: %v", err)
	}
	if string(third.Payload) != "three" {
		t.Errorf("third payload = %q, want %q", third.Payload, "three")
	}
}
