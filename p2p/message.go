package p2p

// ProtocolVersion identifies the wire format spoken by this build.
//
// It is exchanged during the handshake and a mismatch ends the connection.
// Without it, two nodes running different versions connect successfully and
// then misread each other's frames, which surfaces much later as corrupt
// data rather than as a refused connection.
const ProtocolVersion uint32 = 1

// Frame tags. Every frame on a connection begins with one of these.
const (
	IncomingMessage = 0x1
	IncomingStream  = 0x2
)

type RPC struct {
	From    string
	Payload []byte
	Stream  bool
}
