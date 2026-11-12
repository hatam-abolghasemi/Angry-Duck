package worker

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type fakeCRI struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	sandboxes  []*runtimeapi.PodSandbox
	containers []*runtimeapi.Container
}

func (f *fakeCRI) ListPodSandbox(_ context.Context, req *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	var items []*runtimeapi.PodSandbox
	for _, s := range f.sandboxes {
		if st := req.GetFilter().GetState(); st == nil || st.State == s.State {
			items = append(items, s)
		}
	}
	return &runtimeapi.ListPodSandboxResponse{Items: items}, nil
}

func (f *fakeCRI) ListContainers(_ context.Context, req *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	var cs []*runtimeapi.Container
	for _, c := range f.containers {
		if st := req.GetFilter().GetState(); st == nil || st.State == c.State {
			cs = append(cs, c)
		}
	}
	return &runtimeapi.ListContainersResponse{Containers: cs}, nil
}

type fakeCRIImages struct {
	runtimeapi.UnimplementedImageServiceServer
	images []*runtimeapi.Image
}

func (f *fakeCRIImages) ListImages(context.Context, *runtimeapi.ListImagesRequest) (*runtimeapi.ListImagesResponse, error) {
	return &runtimeapi.ListImagesResponse{Images: f.images}, nil
}

func newFakeCRI() (*fakeCRI, *fakeCRIImages) {
	f := &fakeCRI{}
	big := strings.Repeat(`{"apiVersion":"apps/v1","kind":"Deployment"}`, 200)
	for i := 0; i < 12; i++ {
		st := runtimeapi.PodSandboxState_SANDBOX_READY
		if i%4 == 3 {
			st = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
		}
		f.sandboxes = append(f.sandboxes, &runtimeapi.PodSandbox{
			Id: fmt.Sprintf("sb%02d", i), State: st,
			Metadata:    &runtimeapi.PodSandboxMetadata{Name: fmt.Sprintf("pod-%d", i), Namespace: "ns", Uid: "u"},
			Labels:      map[string]string{"app": "x", "pod-template-hash": "abc"},
			Annotations: map[string]string{"kubectl.kubernetes.io/last-applied-configuration": big},
		})
		for j := 0; j < 3; j++ {
			c := &runtimeapi.Container{
				Id: fmt.Sprintf("c%02d-%d", i, j), PodSandboxId: fmt.Sprintf("sb%02d", i),
				Metadata:    &runtimeapi.ContainerMetadata{Name: "app"},
				Image:       &runtimeapi.ImageSpec{Image: fmt.Sprintf("sha256:%064d", j), Annotations: map[string]string{"a": "b"}},
				ImageRef:    fmt.Sprintf("sha256:%064d", j),
				State:       runtimeapi.ContainerState(j % 4),
				Labels:      map[string]string{"io.kubernetes.container.name": "app"},
				Annotations: map[string]string{"io.kubernetes.container.hash": "1234"},
			}
			if j == 1 {
				c.Image.UserSpecifiedImage = "registry.example/team/app:v1"
			}
			if j == 2 && i%2 == 0 {
				c.Image = nil
			}
			f.containers = append(f.containers, c)
		}
	}
	imgs := &fakeCRIImages{}
	for i := 0; i < 6; i++ {
		img := &runtimeapi.Image{Id: fmt.Sprintf("sha256:%064d", i), Size: 1234, Spec: &runtimeapi.ImageSpec{Image: "x", Annotations: map[string]string{"k": "v"}}}
		if i%2 == 0 {
			img.RepoTags = []string{fmt.Sprintf("registry.example/app-%d:v1", i), fmt.Sprintf("registry.example/app-%d:latest", i)}
		}
		if i%3 != 0 {
			img.RepoDigests = []string{fmt.Sprintf("registry.example/app-%d@sha256:%064d", i, i)}
		}
		imgs.images = append(imgs.images, img)
	}
	return f, imgs
}

func serveFakeCRI(t testing.TB) (*grpc.ClientConn, *fakeCRI, *fakeCRIImages) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "cri.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	f, imgs := newFakeCRI()
	srv := grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(srv, f)
	runtimeapi.RegisterImageServiceServer(srv, imgs)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	cc, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc, f, imgs
}

// The lean CRI listings give what the CRI client does, in every field the
// worker reads, and the listings built on them come out the same.
func TestLeanCRIListingsMatchClient(t *testing.T) {
	cc, _, _ := serveFakeCRI(t)
	ctx := context.Background()
	rt := runtimeapi.NewRuntimeServiceClient(cc)

	for _, req := range []*runtimeapi.ListContainersRequest{{}, {Filter: &runtimeapi.ContainerFilter{State: &runtimeapi.ContainerStateValue{State: runtimeapi.ContainerState_CONTAINER_RUNNING}}}} {
		full, err := rt.ListContainers(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		lean, err := leanContainers(ctx, cc, req)
		if err != nil {
			t.Fatal(err)
		}
		if len(lean) != len(full.Containers) || len(lean) == 0 {
			t.Fatalf("got %d containers, want %d", len(lean), len(full.Containers))
		}
		for i, c := range full.Containers {
			l := lean[i]
			if l.GetId() != c.GetId() || l.GetPodSandboxId() != c.GetPodSandboxId() || l.GetImageRef() != c.GetImageRef() || l.GetState() != c.GetState() ||
				l.GetImage().GetImage() != c.GetImage().GetImage() || l.GetImage().GetUserSpecifiedImage() != c.GetImage().GetUserSpecifiedImage() {
				t.Fatalf("container %d differs:\n lean %v\n full %v", i, l, c)
			}
		}
		if !reflect.DeepEqual(toPsOutput(lean), toPsOutput(full.Containers)) {
			t.Fatal("ps output differs")
		}
	}

	r := criRuntime{images: runtimeapi.NewImageServiceClient(cc), rt: rt, cc: cc}
	sb, err := rt.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{Filter: &runtimeapi.PodSandboxFilter{State: &runtimeapi.PodSandboxStateValue{State: runtimeapi.PodSandboxState_SANDBOX_READY}}})
	if err != nil {
		t.Fatal(err)
	}
	all, _ := rt.ListContainers(ctx, &runtimeapi.ListContainersRequest{})
	ready := map[string]bool{}
	for _, s := range sb.Items {
		ready[s.Id] = true
	}
	pods, err := r.ListInUsePods()
	if err != nil {
		t.Fatal(err)
	}
	if want := inUsePods(all.Containers, ready); !reflect.DeepEqual(pods, want) || len(want) == 0 {
		t.Fatalf("in-use pods:\n got %v\nwant %v", pods, want)
	}
	used, err := r.ListInUseImages()
	if err != nil {
		t.Fatal(err)
	}
	if want := inUseImages(all.Containers, ready); !reflect.DeepEqual(used, want) || len(want) == 0 {
		t.Fatalf("in-use images:\n got %v\nwant %v", used, want)
	}

	fullImgs, err := runtimeapi.NewImageServiceClient(cc).ListImages(ctx, &runtimeapi.ListImagesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var want crictlImagesOutput
	for _, img := range fullImgs.GetImages() {
		want.Images = append(want.Images, imageEntry{ID: img.GetId(), RepoTags: img.GetRepoTags(), RepoDigests: img.GetRepoDigests()})
	}
	got, err := r.listImages()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("images:\n got %+v\nwant %+v", got, want)
	}
}

// An image spec given twice is merged, as in a full decode.
func TestParseContainerMergesImageSpec(t *testing.T) {
	a, _ := proto.Marshal(&runtimeapi.Container{Id: "x", Image: &runtimeapi.ImageSpec{Image: "first", UserSpecifiedImage: "asked"}, State: runtimeapi.ContainerState_CONTAINER_EXITED})
	b, _ := proto.Marshal(&runtimeapi.Container{Image: &runtimeapi.ImageSpec{Image: "second"}, State: runtimeapi.ContainerState_CONTAINER_RUNNING})
	msg := append(a, b...)
	var full runtimeapi.Container
	if err := proto.Unmarshal(msg, &full); err != nil {
		t.Fatal(err)
	}
	c, err := parseContainer(msg)
	if err != nil {
		t.Fatal(err)
	}
	if c.GetId() != full.GetId() || c.GetImage().GetImage() != full.GetImage().GetImage() ||
		c.GetImage().GetUserSpecifiedImage() != full.GetImage().GetUserSpecifiedImage() || c.GetState() != full.GetState() {
		t.Fatalf("got %v, full decode %v", c, &full)
	}
	for cut := 1; cut < len(a); cut++ {
		var f runtimeapi.Container
		if proto.Unmarshal(a[:cut], &f) != nil {
			if _, err := parseContainer(a[:cut]); err == nil {
				t.Fatalf("cut at %d: full decode fails, lean parse doesn't", cut)
			}
		}
	}
}
