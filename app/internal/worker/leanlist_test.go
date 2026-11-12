package worker

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/content"
	contentproxy "github.com/containerd/containerd/content/proxy"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/snapshots"
	snapshotproxy "github.com/containerd/containerd/snapshots/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeLists answers containerd's three list calls with fixed entries, the
// content and snapshots ones spread over several stream messages.
type fakeLists struct {
	blobs  []*contentapi.Info
	snaps  []*snapshotsapi.Info
	images []*imagesapi.Image
}

type fakeContentList struct {
	contentapi.UnimplementedContentServer
	*fakeLists
}

func (f fakeContentList) List(req *contentapi.ListContentRequest, st contentapi.Content_ListServer) error {
	for i := 0; i < len(f.blobs); i += 7 {
		if err := st.Send(&contentapi.ListContentResponse{Info: f.blobs[i:min(i+7, len(f.blobs))]}); err != nil {
			return err
		}
	}
	return nil
}

type fakeSnapshotList struct {
	snapshotsapi.UnimplementedSnapshotsServer
	*fakeLists
}

func (f fakeSnapshotList) List(req *snapshotsapi.ListSnapshotsRequest, st snapshotsapi.Snapshots_ListServer) error {
	if req.Snapshotter != "overlayfs" {
		return status.Errorf(codes.InvalidArgument, "snapshotter not loaded: %s", req.Snapshotter)
	}
	for i := 0; i < len(f.snaps); i += 5 {
		if err := st.Send(&snapshotsapi.ListSnapshotsResponse{Info: f.snaps[i:min(i+5, len(f.snaps))]}); err != nil {
			return err
		}
	}
	return nil
}

type fakeImageList struct {
	imagesapi.UnimplementedImagesServer
	*fakeLists
}

func (f fakeImageList) List(context.Context, *imagesapi.ListImagesRequest) (*imagesapi.ListImagesResponse, error) {
	return &imagesapi.ListImagesResponse{Images: f.images}, nil
}

func newFakeLists() *fakeLists {
	f := &fakeLists{}
	labels := func(i int) map[string]string {
		return map[string]string{
			"containerd.io/gc.ref.content.l.0":                   fmt.Sprintf("sha256:%064d", i+1),
			"containerd.io/distribution.source.registry.example": "team/app",
			"containerd.io/uncompressed":                         fmt.Sprintf("sha256:%064d", i+2),
		}
	}
	for i := 0; i < 50; i++ {
		f.blobs = append(f.blobs, &contentapi.Info{Digest: fmt.Sprintf("sha256:%064d", i), Size: int64(i * 1000), Labels: labels(i), CreatedAt: timestamppb.Now()})
	}
	for i := 0; i < 23; i++ {
		f.snaps = append(f.snaps, &snapshotsapi.Info{Name: fmt.Sprintf("sha256:%064x", i), Parent: "p", Kind: snapshotsapi.Kind_COMMITTED, Labels: labels(i)})
	}
	for i := 0; i < 9; i++ {
		img := &imagesapi.Image{
			Name:   fmt.Sprintf("registry.example/team/app-%d:v%d", i, i),
			Labels: map[string]string{"io.cri-containerd.image": "managed"},
			Target: &types.Descriptor{MediaType: "application/vnd.oci.image.index.v1+json", Digest: fmt.Sprintf("sha256:%064d", i%4), Size: 512, Annotations: map[string]string{"a": "b"}},
		}
		if i%3 == 0 {
			img.Labels[RetainLabel] = fmt.Sprint(1790000000 + i)
		}
		f.images = append(f.images, img)
	}
	f.images = append(f.images, &imagesapi.Image{Name: "no-target"})
	return f
}

func serveFakeLists(t *testing.T, f *fakeLists) *grpc.ClientConn {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "c.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	contentapi.RegisterContentServer(srv, fakeContentList{fakeLists: f})
	snapshotsapi.RegisterSnapshotsServer(srv, fakeSnapshotList{fakeLists: f})
	imagesapi.RegisterImagesServer(srv, fakeImageList{fakeLists: f})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	cc, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc
}

// The lean listings give what containerd's own client does.
func TestLeanListingsMatchContainerdClient(t *testing.T) {
	f := newFakeLists()
	cc := serveFakeLists(t, f)
	ctx := context.Background()

	want := map[string]bool{}
	if err := contentproxy.NewContentStore(contentapi.NewContentClient(cc)).Walk(ctx, func(i content.Info) error {
		want[i.Digest.String()] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := leanContentDigests(ctx, cc)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("content: got %d digests, want %d", len(got), len(want))
	}

	wantS := map[string]bool{}
	if err := snapshotproxy.NewSnapshotter(snapshotsapi.NewSnapshotsClient(cc), "overlayfs").Walk(ctx, func(_ context.Context, i snapshots.Info) error {
		wantS[i.Name] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	gotS, err := leanSnapshotNames(ctx, cc, "overlayfs")
	if err != nil {
		t.Fatal(err)
	}
	if len(gotS) != 23 || fmt.Sprint(gotS) != fmt.Sprint(wantS) {
		t.Fatalf("snapshots: got %d names, want %d", len(gotS), len(wantS))
	}

	// An error comes back as the client's would.
	_, err = leanSnapshotNames(ctx, cc, "nope")
	wantErr := snapshotproxy.NewSnapshotter(snapshotsapi.NewSnapshotsClient(cc), "nope").Walk(ctx, func(context.Context, snapshots.Info) error { return nil })
	if err == nil || err.Error() != wantErr.Error() || !errdefs.IsInvalidArgument(err) {
		t.Fatalf("error: got %v, want %v", err, wantErr)
	}

	for _, label := range []string{"", RetainLabel, "io.cri-containerd.image", "absent"} {
		type entry struct{ target, value string }
		got := map[string]entry{}
		if err := leanImages(ctx, cc, label, func(name, target, value string) { got[name] = entry{target, value} }); err != nil {
			t.Fatal(err)
		}
		if len(got) != len(f.images) {
			t.Fatalf("images: got %d, want %d", len(got), len(f.images))
		}
		for _, img := range f.images {
			e := got[img.Name]
			if e.target != img.GetTarget().GetDigest() {
				t.Fatalf("%s: target %q, want %q", img.Name, e.target, img.GetTarget().GetDigest())
			}
			want := ""
			if label != "" {
				want = img.Labels[label]
			}
			if e.value != want {
				t.Fatalf("%s: label %q = %q, want %q", img.Name, label, e.value, want)
			}
		}
	}
}

// Replies split over several buffers parse the same.
func TestLeanCodecReadsSplitReplies(t *testing.T) {
	codec := leanCodec{nil}
	b, _ := proto.Marshal(&contentapi.ListContentResponse{Info: []*contentapi.Info{{Digest: "sha256:aa", Labels: map[string]string{"k": "v"}}, {Digest: "sha256:bb"}}})
	var got []string
	reply := &leanReply{parse: func(b []byte) error {
		return eachMessage(b, 1, func(info []byte) error {
			d, err := stringField(info, 1)
			got = append(got, d)
			return err
		})
	}}
	if err := codec.Unmarshal(splitBuffers(b, 3), reply); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[sha256:aa sha256:bb]" {
		t.Fatalf("got %v", got)
	}
}

// Fields given twice end as a full decode leaves them: the last value,
// and messages merged.
func TestParseImageRepeatedFields(t *testing.T) {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, "first")
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendBytes(b, mustMarshal(t, &types.Descriptor{Digest: "sha256:old", MediaType: "m"}))
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendBytes(b, mapEntryBytes("l", "1"))
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, "second")
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendBytes(b, mustMarshal(t, &types.Descriptor{Size: 9})) // no digest: merged, keeps the old
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendBytes(b, mapEntryBytes("l", "2"))

	var full imagesapi.Image
	if err := proto.Unmarshal(b, &full); err != nil {
		t.Fatal(err)
	}
	name, target, value, err := parseImage(b, "l")
	if err != nil {
		t.Fatal(err)
	}
	if name != full.Name || target != full.Target.Digest || value != full.Labels["l"] {
		t.Fatalf("got %q %q %q, full decode %q %q %q", name, target, value, full.Name, full.Target.Digest, full.Labels["l"])
	}
}

// Broken replies are errors, as in a full decode, never a panic or a
// partial answer taken as whole.
func TestLeanParsersRejectBrokenReplies(t *testing.T) {
	good, _ := proto.Marshal(&imagesapi.ListImagesResponse{Images: []*imagesapi.Image{{Name: "a", Labels: map[string]string{"k": "v"}, Target: &types.Descriptor{Digest: "sha256:aa"}}}})
	for cut := 1; cut < len(good); cut++ {
		b := good[:cut]
		var full imagesapi.ListImagesResponse
		fullErr := proto.Unmarshal(b, &full)
		err := eachMessage(b, 1, func(img []byte) error {
			_, _, _, err := parseImage(img, "k")
			return err
		})
		if fullErr != nil && err == nil {
			t.Fatalf("cut at %d: full decode fails (%v), lean parse doesn't", cut, fullErr)
		}
	}
	bad := protowire.AppendTag(nil, 1, protowire.BytesType)
	bad = protowire.AppendBytes(bad, protowire.AppendString(protowire.AppendTag(nil, 1, protowire.BytesType), "\xff\xfe"))
	if _, err := stringField(bad[2:], 1); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mapEntryBytes(k, v string) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, k)
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	return protowire.AppendString(b, v)
}

// splitBuffers cuts b into n gRPC buffers.
func splitBuffers(b []byte, n int) mem.BufferSlice {
	var bs mem.BufferSlice
	step := (len(b) + n - 1) / n
	for i := 0; i < len(b); i += step {
		bs = append(bs, mem.SliceBuffer(b[i:min(i+step, len(b))]))
	}
	return bs
}
