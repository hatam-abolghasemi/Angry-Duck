package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/errdefs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

// Lean listings of containerd's content, snapshots and images.
//
// The periodic loops list every blob, snapshot and image on the node, but
// use only a digest, a name or one label of each. containerd's client
// decodes every entry whole: each content blob's and snapshot's labels
// (garbage-collection references, sources: several long strings each) into
// a fresh map, each image's labels and target annotations too, and then
// copies all of it again into its own types. On a node with a few thousand
// blobs that is tens of thousands of allocations per listing, several
// times a minute, and most of a worker's idle CPU and garbage.
//
// These calls send the same requests over the same connection (so the
// namespace and call counting apply as before) and read the answers
// straight from the wire, keeping only those fields: the same results,
// without decoding the rest.

const (
	contentListMethod   = "/containerd.services.content.v1.Content/List"
	snapshotsListMethod = "/containerd.services.snapshots.v1.Snapshots/List"
	imagesListMethod    = "/containerd.services.images.v1.Images/List"
)

// leanReply is a reply read by parse, straight from its encoding, while
// gRPC still holds the bytes.
type leanReply struct {
	parse func(b []byte) error
}

// leanCodec encodes requests as gRPC's proto codec does and hands replies
// to their leanReply.
type leanCodec struct{ encoding.CodecV2 }

func (c leanCodec) Unmarshal(data mem.BufferSlice, v any) error {
	r, ok := v.(*leanReply)
	if !ok {
		return c.CodecV2.Unmarshal(data, v)
	}
	if len(data) == 1 {
		return r.parse(data[0].ReadOnlyData())
	}
	buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
	defer buf.Free()
	return r.parse(buf.ReadOnlyData())
}

func leanCallOption() (grpc.CallOption, error) {
	base := encoding.GetCodecV2("proto")
	if base == nil {
		return nil, errors.New("gRPC's proto codec isn't registered")
	}
	return grpc.ForceCodecV2(leanCodec{base}), nil
}

// leanStream sends req on a server stream and hands every reply to parse.
func leanStream(ctx context.Context, cc grpc.ClientConnInterface, method string, req any, parse func([]byte) error) error {
	opt, err := leanCallOption()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // ends the stream when parse fails partway
	st, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method, opt)
	if err != nil {
		return errdefs.FromGRPC(err)
	}
	if err := st.SendMsg(req); err != nil {
		if err == io.EOF {
			err = st.RecvMsg(&leanReply{parse: parse}) // the real error
		}
		return errdefs.FromGRPC(err)
	}
	if err := st.CloseSend(); err != nil {
		return errdefs.FromGRPC(err)
	}
	reply := &leanReply{parse: parse}
	for {
		if err := st.RecvMsg(reply); err != nil {
			if err == io.EOF {
				return nil
			}
			return errdefs.FromGRPC(err)
		}
	}
}

// leanContentDigests lists the digest of every blob in the content store.
func leanContentDigests(ctx context.Context, cc grpc.ClientConnInterface) (map[string]bool, error) {
	have := make(map[string]bool)
	err := leanStream(ctx, cc, contentListMethod, &contentapi.ListContentRequest{}, func(b []byte) error {
		// ListContentResponse: repeated Info info = 1; Info: string digest = 1.
		return eachMessage(b, 1, func(info []byte) error {
			d, err := stringField(info, 1)
			if err != nil {
				return err
			}
			have[d] = true
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return have, nil
}

// leanSnapshotNames lists the name of every snapshot of snapshotter.
func leanSnapshotNames(ctx context.Context, cc grpc.ClientConnInterface, snapshotter string) (map[string]bool, error) {
	keys := make(map[string]bool)
	err := leanStream(ctx, cc, snapshotsListMethod, &snapshotsapi.ListSnapshotsRequest{Snapshotter: snapshotter}, func(b []byte) error {
		// ListSnapshotsResponse: repeated Info info = 1; Info: string name = 1.
		return eachMessage(b, 1, func(info []byte) error {
			name, err := stringField(info, 1)
			if err != nil {
				return err
			}
			keys[name] = true
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// leanImages lists every image: its name, the digest of its target, and
// the value of label on it ("" when it hasn't one, or label is "").
func leanImages(ctx context.Context, cc grpc.ClientConnInterface, label string, fn func(name, target, value string)) error {
	opt, err := leanCallOption()
	if err != nil {
		return err
	}
	return errdefs.FromGRPC(cc.Invoke(ctx, imagesListMethod, &imagesapi.ListImagesRequest{}, &leanReply{parse: func(b []byte) error {
		// ListImagesResponse: repeated Image images = 1.
		return eachMessage(b, 1, func(img []byte) error {
			name, target, value, err := parseImage(img, label)
			if err != nil {
				return err
			}
			fn(name, target, value)
			return nil
		})
	}}, opt))
}

// parseImage reads an Image: string name = 1, map<string, string> labels
// = 2, Descriptor target = 3 (Descriptor: string digest = 2). As in a full
// decode, a field given twice keeps its last value, and a message given
// twice is merged.
func parseImage(b []byte, label string) (name, target, value string, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", "", "", protowire.ParseError(n)
		}
		b = b[n:]
		if typ != protowire.BytesType || num < 1 || num > 3 {
			if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
				return "", "", "", protowire.ParseError(n)
			}
			b = b[n:]
			continue
		}
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return "", "", "", protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case 1:
			if name, err = utf8String(v); err != nil {
				return "", "", "", err
			}
		case 2:
			if label == "" {
				continue
			}
			k, val, err := mapEntry(v)
			if err != nil {
				return "", "", "", err
			}
			if k == label {
				value = val
			}
		case 3:
			d, ok, err := optionalStringField(v, 2)
			if err != nil {
				return "", "", "", err
			}
			if ok {
				target = d
			}
		}
	}
	return name, target, value, nil
}

// eachMessage calls fn with every field num of b, a message field.
func eachMessage(b []byte, num protowire.Number, fn func([]byte) error) error {
	for len(b) > 0 {
		got, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		if got == num && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			if err := fn(v); err != nil {
				return err
			}
			continue
		}
		if n = protowire.ConsumeFieldValue(got, typ, b); n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
	}
	return nil
}

// stringField reads string field num of message b ("" when absent).
func stringField(b []byte, num protowire.Number) (string, error) {
	s, _, err := optionalStringField(b, num)
	return s, err
}

// optionalStringField reads string field num of message b (its last
// occurrence), and whether it is there.
func optionalStringField(b []byte, num protowire.Number) (s string, ok bool, err error) {
	var last []byte
	for len(b) > 0 {
		got, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", false, protowire.ParseError(n)
		}
		b = b[n:]
		if got == num && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return "", false, protowire.ParseError(n)
			}
			b = b[n:]
			last, ok = v, true
			continue
		}
		if n = protowire.ConsumeFieldValue(got, typ, b); n < 0 {
			return "", false, protowire.ParseError(n)
		}
		b = b[n:]
	}
	if !ok {
		return "", false, nil
	}
	s, err = utf8String(last)
	return s, true, err
}

// mapEntry reads a map<string, string> entry: key = 1, value = 2.
func mapEntry(b []byte) (key, value string, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		b = b[n:]
		if typ == protowire.BytesType && (num == 1 || num == 2) {
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return "", "", protowire.ParseError(n)
			}
			b = b[n:]
			s, err := utf8String(v)
			if err != nil {
				return "", "", err
			}
			if num == 1 {
				key = s
			} else {
				value = s
			}
			continue
		}
		if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
			return "", "", protowire.ParseError(n)
		}
		b = b[n:]
	}
	return key, value, nil
}

// utf8String copies b into a string, refusing invalid UTF-8 as a proto3
// string decode does.
func utf8String(b []byte) (string, error) {
	if !utf8.Valid(b) {
		return "", fmt.Errorf("string field contains invalid UTF-8")
	}
	return string(b), nil
}
