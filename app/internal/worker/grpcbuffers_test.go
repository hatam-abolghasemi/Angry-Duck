package worker

import (
	"bytes"
	"errors"
	"testing"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"
)

// fakeStream encodes what it is sent with codec, as gRPC does in SendMsg,
// and keeps the bytes.
type fakeStream struct {
	grpc.ClientStream
	codec   encoding.Codec
	sent    [][]byte
	sendErr error
	recvErr error
}

func (f *fakeStream) SendMsg(m any) error {
	b, err := f.codec.Marshal(m)
	if err != nil {
		return err
	}
	f.sent = append(f.sent, b)
	return f.sendErr
}

func (f *fakeStream) RecvMsg(any) error { return f.recvErr }

func TestPooledWritesEncodeLikeProto(t *testing.T) {
	codec := pooledCodec{encoding.GetCodec("proto")}
	fs := &fakeStream{codec: codec}
	ws := &writeStream{ClientStream: fs}
	data := bytes.Repeat([]byte{7}, rpcChunk)
	req := &contentapi.WriteContentRequest{Action: contentapi.WriteAction_WRITE, Offset: 42, Data: data}
	if err := ws.SendMsg(req); err != nil {
		t.Fatal(err)
	}
	want, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fs.sent[0], want) {
		t.Fatal("pooled encoding differs from proto.Marshal")
	}
	if len(ws.sent) != 1 {
		t.Fatalf("%d buffers waiting for an answer, want 1", len(ws.sent))
	}
	if _, ok := writeSlots.Load(req); ok {
		t.Fatal("slot left behind after the send")
	}
	if err := ws.RecvMsg(&contentapi.WriteContentResponse{}); err != nil {
		t.Fatal(err)
	}
	if len(ws.sent) != 0 {
		t.Fatal("answered buffers not given back")
	}
}

func TestPooledWritesKeepBuffersOfFailedSends(t *testing.T) {
	codec := pooledCodec{encoding.GetCodec("proto")}
	fs := &fakeStream{codec: codec, sendErr: errors.New("broken")}
	ws := &writeStream{ClientStream: fs}
	req := &contentapi.WriteContentRequest{Action: contentapi.WriteAction_WRITE, Data: make([]byte, pooledWriteMin)}
	if err := ws.SendMsg(req); err == nil {
		t.Fatal("want the send's error")
	}
	if len(ws.sent) != 0 {
		t.Fatal("a failed send's buffer must not be reused: the transport may still hold it")
	}
	fs.sendErr, fs.recvErr = nil, errors.New("gone")
	if err := ws.SendMsg(req); err != nil {
		t.Fatal(err)
	}
	if err := ws.RecvMsg(&contentapi.WriteContentResponse{}); err == nil {
		t.Fatal("want the answer's error")
	}
	if len(ws.sent) != 0 {
		t.Fatal("unanswered buffers must be left to the collector, not kept")
	}
}

func TestPooledCodecOnlyForWriteStreams(t *testing.T) {
	codec := pooledCodec{encoding.GetCodec("proto")}
	req := &contentapi.WriteContentRequest{Action: contentapi.WriteAction_WRITE, Data: make([]byte, rpcChunk)}
	// Not sent through a writeStream: plain encoding, nothing to give back.
	b, err := codec.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := proto.Marshal(req)
	if !bytes.Equal(b, want) {
		t.Fatal("encoding differs")
	}
	// Small writes and other messages go through unchanged.
	fs := &fakeStream{codec: codec}
	ws := &writeStream{ClientStream: fs}
	if err := ws.SendMsg(&contentapi.WriteContentRequest{Action: contentapi.WriteAction_WRITE, Data: []byte("tiny")}); err != nil {
		t.Fatal(err)
	}
	if err := ws.SendMsg(&contentapi.InfoRequest{Digest: "sha256:x"}); err != nil {
		t.Fatal(err)
	}
	if len(ws.sent) != 0 {
		t.Fatal("pooled a small write or another message")
	}
	if codec.Name() != "proto" {
		t.Fatalf("codec name %q", codec.Name())
	}
}
