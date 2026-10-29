package worker

import (
	"context"
	"sync"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"
)

// Encoding content writes into reused buffers.
//
// Every blob that reaches this node through the worker (a spread transfer,
// a registry pull by digest, a rescue) goes to containerd as content Write
// calls of up to rpcChunk bytes, and gRPC encodes each into a fresh buffer:
// one byte of garbage for every byte received, which keeps the collector
// busy and the heap well above what the worker needs while transfers run.
//
// A content write is a strict request/response exchange (containerd's
// remote writer sends one request, then waits for its answer). Once the
// answer is in, containerd has read the whole request, so gRPC's transport
// is done with its bytes, and the buffer can encode the next one. Only
// write requests sent through writeStream, and only once answered, go back
// to the pool; a request whose send or answer failed leaves its buffer to
// the collector.

const contentWriteMethod = "/containerd.services.content.v1.Content/Write"

// pooledWriteMin is the smallest write payload worth a pooled buffer.
const pooledWriteMin = 64 << 10

var writeBufs = sync.Pool{New: func() any {
	b := make([]byte, 0, rpcChunk+4<<10)
	return &b
}}

// writeSlot is how writeStream lends the codec a pooled buffer for one
// request, and gets it back.
type writeSlot struct{ buf *[]byte }

// writeSlots maps a request being sent by a writeStream to its slot.
var writeSlots sync.Map // *contentapi.WriteContentRequest -> *writeSlot

// pooledCodec is gRPC's proto codec, except that write requests sent
// through a writeStream are encoded into a pooled buffer.
type pooledCodec struct{ encoding.Codec }

func (c pooledCodec) Marshal(v any) ([]byte, error) {
	req, ok := v.(*contentapi.WriteContentRequest)
	if !ok {
		return c.Codec.Marshal(v)
	}
	s, ok := writeSlots.Load(req)
	if !ok {
		return c.Codec.Marshal(v)
	}
	bp := writeBufs.Get().(*[]byte)
	out, err := proto.MarshalOptions{}.MarshalAppend((*bp)[:0], req)
	if err != nil {
		writeBufs.Put(bp)
		return nil, err
	}
	*bp = out[:0] // keeps out's array (a bigger one, if it had to grow)
	s.(*writeSlot).buf = bp
	return out, nil
}

// writeStream wraps a content Write stream: requests it sends are encoded
// into pooled buffers, given back once the next answer arrives.
type writeStream struct {
	grpc.ClientStream
	sent []*[]byte // encoded requests not answered yet
}

func (s *writeStream) SendMsg(m any) error {
	req, ok := m.(*contentapi.WriteContentRequest)
	if !ok || len(req.Data) < pooledWriteMin {
		return s.ClientStream.SendMsg(m)
	}
	slot := &writeSlot{}
	writeSlots.Store(req, slot)
	err := s.ClientStream.SendMsg(m)
	writeSlots.Delete(req)
	if slot.buf != nil && err == nil {
		s.sent = append(s.sent, slot.buf)
	}
	return err
}

func (s *writeStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err == nil {
		for _, bp := range s.sent {
			writeBufs.Put(bp)
		}
	}
	clear(s.sent)
	s.sent = s.sent[:0]
	return err
}

// pooledWrites returns the dial options that make content writes use
// pooled buffers: the codec, and the stream interceptor that lends it
// them.
func pooledWrites() []grpc.DialOption {
	base := encoding.GetCodec("proto")
	if base == nil {
		return nil
	}
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pooledCodec{base})),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			cs, err := streamer(ctx, desc, cc, method, opts...)
			if err != nil || method != contentWriteMethod {
				return cs, err
			}
			return &writeStream{ClientStream: cs}, nil
		}),
	}
}
