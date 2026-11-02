package worker

import (
	"sync"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

// Encoding content writes into reused buffers.
//
// Every blob that reaches this node through the worker (a spread transfer,
// a registry pull by digest, a rescue) goes to containerd as content Write
// calls of up to rpcChunk bytes. gRPC's proto codec encodes each into a
// buffer from its default pool, which it zeroes on every Get, and a write
// request (rpcChunk plus a few bytes) is just above the pool's largest
// size, so each one is a fresh, zeroed allocation: a byte of zeroing and
// garbage for every byte received.
//
// pooledCodec encodes large write requests into buffers of its own pool
// instead, sized for one request and never zeroed: the encoding overwrites
// every byte gRPC sends. gRPC hands a buffer back (Free) once its transport
// has written it, so the next request reuses it. Everything else goes
// through gRPC's proto codec unchanged.

// pooledWriteMin is the smallest write payload worth a pooled buffer.
const pooledWriteMin = 64 << 10

var writeBufs = sync.Pool{New: func() any {
	b := make([]byte, 0, rpcChunk+4<<10)
	return &b
}}

// writeBufPool lends writeBufs' buffers to gRPC, without zeroing them.
type writeBufPool struct{}

func (writeBufPool) Get(n int) *[]byte {
	bp := writeBufs.Get().(*[]byte)
	if cap(*bp) < n {
		*bp = make([]byte, n)
	}
	*bp = (*bp)[:n]
	return bp
}

func (writeBufPool) Put(bp *[]byte) { writeBufs.Put(bp) }

// pooledCodec is gRPC's proto codec, except that content write requests of
// pooledWriteMin or more are encoded into writeBufs' buffers.
type pooledCodec struct{ encoding.CodecV2 }

func (c pooledCodec) Marshal(v any) (mem.BufferSlice, error) {
	req, ok := v.(*contentapi.WriteContentRequest)
	if !ok || len(req.Data) < pooledWriteMin {
		return c.CodecV2.Marshal(v)
	}
	// Size first, so the encoding below can use the cached size and never
	// grow the buffer.
	n := proto.Size(req)
	bp := writeBufPool{}.Get(n)
	out, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend((*bp)[:0], req)
	if err != nil {
		writeBufPool{}.Put(bp)
		return nil, err
	}
	*bp = out
	return mem.BufferSlice{mem.NewBuffer(bp, writeBufPool{})}, nil
}

// pooledWrites returns the dial option that makes content writes use
// pooled buffers.
func pooledWrites() []grpc.DialOption {
	base := encoding.GetCodecV2("proto")
	if base == nil {
		return nil
	}
	return []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.ForceCodecV2(pooledCodec{base}))}
}
