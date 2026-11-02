package worker

import (
	"bytes"
	"testing"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"
)

func TestPooledWritesEncodeLikeProto(t *testing.T) {
	codec := pooledCodec{encoding.GetCodecV2("proto")}
	data := bytes.Repeat([]byte{7}, rpcChunk)
	req := &contentapi.WriteContentRequest{Action: contentapi.WriteAction_WRITE, Offset: 42, Data: data}
	bs, err := codec.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	want, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bs.Materialize(), want) {
		t.Fatal("pooled encoding differs from proto.Marshal")
	}
	if len(bs) != 1 || cap(bs[0].ReadOnlyData()) < rpcChunk+4<<10 {
		t.Fatal("a large write must be encoded into one pooled buffer")
	}
	bs.Free() // what gRPC does once its transport wrote it
}

func TestPooledBuffersAreReusedWithoutZeroing(t *testing.T) {
	codec := pooledCodec{encoding.GetCodecV2("proto")}
	big := &contentapi.WriteContentRequest{Action: contentapi.WriteAction_WRITE, Data: bytes.Repeat([]byte{1}, rpcChunk)}
	small := &contentapi.WriteContentRequest{Action: contentapi.WriteAction_WRITE, Data: bytes.Repeat([]byte{2}, pooledWriteMin)}
	for i := 0; i < 3; i++ { // a reused buffer must encode exactly, whatever it held
		for _, req := range []*contentapi.WriteContentRequest{big, small} {
			bs, err := codec.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := proto.Marshal(req)
			if !bytes.Equal(bs.Materialize(), want) {
				t.Fatalf("round %d: encoding differs after reuse", i)
			}
			bs.Free()
		}
	}
	if n := testing.AllocsPerRun(50, func() {
		bs, _ := codec.Marshal(big)
		bs.Free()
	}); n > 2 {
		t.Fatalf("%.0f allocations per pooled write, want the buffer reused", n)
	}
}

func TestPooledCodecLeavesOtherMessagesToProto(t *testing.T) {
	codec := pooledCodec{encoding.GetCodecV2("proto")}
	for _, m := range []proto.Message{
		&contentapi.WriteContentRequest{Action: contentapi.WriteAction_WRITE, Data: []byte("tiny")},
		&contentapi.InfoRequest{Digest: "sha256:x"},
	} {
		bs, err := codec.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := proto.Marshal(m)
		if !bytes.Equal(bs.Materialize(), want) {
			t.Fatalf("%T encoded differently", m)
		}
		bs.Free()
	}
	if len(pooledWrites()) != 1 {
		t.Fatal("the pooled codec must be installed: gRPC's proto codec is registered")
	}
}
