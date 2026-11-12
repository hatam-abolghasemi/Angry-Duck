//go:build integration

// Direct writes against a real containerd: blobs written straight into
// their ingest files and committed by containerd, failures left as
// containerd would leave a failed download, and containerd's own writes
// of the same blob unaffected.
//
//	ANGRYDUCK_CONTAINERD=/run/containerd/containerd.sock ANGRYDUCK_CONTAINERD_ROOT=/var/lib/containerd \
//	  go test -tags integration -run Direct ./internal/worker/
package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/leases"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func directStore(t *testing.T, prefix string) *ContainerdStore {
	t.Helper()
	s := streamStore(t, prefix)
	if err := s.EnableDirectWrites(context.Background(), "it"); err != nil {
		t.Fatalf("direct writes must work against this containerd: %v", err)
	}
	if !s.DirectWrites() {
		t.Fatal("EnableDirectWrites succeeded but DirectWrites is false")
	}
	return s
}

func testLease(t *testing.T, s *ContainerdStore) string {
	t.Helper()
	l := "it-" + randomSuffix()
	if err := s.CreateLease(context.Background(), l, time.Hour); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.DeleteLease(context.Background(), l) })
	return l
}

// ingestRefs lists the refs of the writes still in s's ingest directory.
func ingestRefs(t *testing.T, s *ContainerdStore) []string {
	t.Helper()
	entries, err := os.ReadDir(s.ingestDir)
	if err != nil {
		t.Fatal(err)
	}
	var refs []string
	for _, e := range entries {
		if b, err := os.ReadFile(filepath.Join(s.ingestDir, e.Name(), "ref")); err == nil {
			refs = append(refs, string(b))
		}
	}
	return refs
}

func leftIngest(t *testing.T, s *ContainerdStore, ref string) bool {
	for _, r := range ingestRefs(t, s) {
		if strings.HasPrefix(r, s.namespace+"/") && strings.HasSuffix(r, "/"+ref) {
			return true
		}
	}
	return false
}

func TestContainerd_DirectWritesCommitBlobsOfEverySize(t *testing.T) {
	s := directStore(t, "direct")
	ctx := context.Background()
	for _, n := range []int{directMin - 1, directMin, directMin + 1, 3*directWindow + 12345, 40 << 20} {
		blob := randomBlob(t, n)
		d := digest.FromBytes(blob)
		lease := testLease(t, s)
		got, err := s.WriteBlob(ctx, lease, lease, d.String(), int64(n), &chunkedReader{b: blob, n: 48 << 10})
		if err != nil || got != int64(n) {
			t.Fatalf("%d bytes: wrote %d, %v", n, got, err)
		}
		back, err := s.ReadBlob(ctx, d.String(), int64(n))
		if err != nil || !bytes.Equal(back, blob) {
			t.Fatalf("%d bytes: read back differs: %v", n, err)
		}
		info, err := s.client.ContentStore().Info(s.ns(ctx), d)
		if err != nil || info.Size != int64(n) {
			t.Fatalf("%d bytes: containerd's info %v, %v", n, info, err)
		}
		if leftIngest(t, s, lease) {
			t.Fatalf("%d bytes: ingest left behind after the commit", n)
		}
		// Held by the lease like any written blob.
		res, err := s.client.LeasesService().ListResources(s.ns(ctx), leases.Lease{ID: lease})
		if err != nil || len(res) != 1 || res[0].ID != d.String() {
			t.Fatalf("%d bytes: lease holds %v, %v", n, res, err)
		}
	}
}

// Bytes that don't match the digest fail the commit; the write is aborted
// and nothing is left: what a bad download through the API gives.
func TestContainerd_DirectWriteOfWrongBytesIsAbortedAndLeavesNothing(t *testing.T) {
	s := directStore(t, "direct-bad")
	ctx := context.Background()
	blob := randomBlob(t, 6<<20)
	d := digest.FromBytes(blob)
	bad := append([]byte(nil), blob...)
	bad[len(bad)/2] ^= 0xff
	for name, r := range map[string]io.Reader{
		"corrupt":   bytes.NewReader(bad),
		"short":     bytes.NewReader(blob[:len(blob)-1]),
		"long":      bytes.NewReader(append(append([]byte(nil), blob...), 'x')),
		"read fail": io.MultiReader(bytes.NewReader(blob[:3<<20]), errReader{}),
	} {
		lease := testLease(t, s)
		if _, err := s.WriteBlob(ctx, lease, lease, d.String(), int64(len(blob)), r); err == nil {
			t.Fatalf("%s: a write of the wrong bytes succeeded", name)
		}
		if have, _ := s.HasBlob(d.String()); have {
			t.Fatalf("%s: the blob is in the store", name)
		}
		if leftIngest(t, s, lease) {
			t.Fatalf("%s: the failed write's ingest is still there", name)
		}
	}
	// The right bytes go in afterwards.
	lease := testLease(t, s)
	if _, err := s.WriteBlob(ctx, lease, lease, d.String(), int64(len(blob)), bytes.NewReader(blob)); err != nil {
		t.Fatal(err)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("connection reset") }

// A worker that dies half-way leaves an interrupted write under its
// lease. containerd keeps working: another writer (kubelet's pull, say)
// writes and commits the same blob under its own ref, and the leftover
// goes with its lease.
func TestContainerd_DirectWriteLeftHalfwayDoesntDisturbContainerd(t *testing.T) {
	s := directStore(t, "direct-crash")
	ctx := context.Background()
	blob := randomBlob(t, 8<<20)
	d := digest.FromBytes(blob)
	lease := "it-crash-" + randomSuffix()
	if err := s.CreateLease(ctx, lease, time.Hour); err != nil {
		t.Fatal(err)
	}
	// What writeDirect does, up to dying with half the bytes written.
	lctx := leases.WithLease(s.ns(ctx), lease)
	desc := ocispec.Descriptor{Digest: d, Size: int64(len(blob))}
	cw, err := content.OpenWriter(lctx, s.client.ContentStore(), content.WithRef(lease), content.WithDescriptor(desc))
	if err != nil {
		t.Fatal(err)
	}
	cw.Close()
	path, err := s.ingestDataPath(lease)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, blob[:len(blob)/2], 0); err != nil {
		t.Fatal(err)
	}
	// containerd itself writes the same blob, through its API, under
	// another ref, as a pull would.
	other := testLease(t, s)
	octx := leases.WithLease(s.ns(ctx), other)
	if err := content.WriteBlob(octx, s.client.ContentStore(), "pull-"+randomSuffix(), bytes.NewReader(blob), desc); err != nil {
		t.Fatalf("containerd's own write of the blob: %v", err)
	}
	if back, err := s.ReadBlob(ctx, d.String(), int64(len(blob))); err != nil || !bytes.Equal(back, blob) {
		t.Fatalf("blob after containerd's own write: %v", err)
	}
	// The leftover goes with its lease.
	if !leftIngest(t, s, lease) {
		t.Fatal("the interrupted write should still be there under its lease")
	}
	if err := s.client.LeasesService().Delete(s.ns(ctx), leases.Lease{ID: lease}, leases.SynchronousDelete); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for leftIngest(t, s, lease) {
		if time.Now().After(deadline) {
			t.Fatal("the interrupted write is still there after its lease went")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Many blobs at once, and the same blob twice at once, as a spread's two
// paths may: each lands, none disturbs another.
func TestContainerd_DirectWritesInParallel(t *testing.T) {
	s := directStore(t, "direct-par")
	ctx := context.Background()
	blobs := make([][]byte, 6)
	for i := range blobs {
		blobs[i] = randomBlob(t, 5<<20+i*4097)
	}
	blobs = append(blobs, blobs[0], blobs[0], blobs[1]) // the same blob several times at once
	var wg sync.WaitGroup
	errs := make([]error, len(blobs))
	leaseOf := make([]string, len(blobs))
	for i, b := range blobs {
		lease := testLease(t, s)
		leaseOf[i] = lease
		wg.Add(1)
		go func(i int, b []byte, lease string) {
			defer wg.Done()
			_, errs[i] = s.WriteBlob(ctx, lease, lease, digest.FromBytes(b).String(), int64(len(b)), &chunkedReader{b: b, n: 32 << 10})
		}(i, b, lease)
	}
	wg.Wait()
	for i, b := range blobs {
		if errs[i] != nil {
			t.Fatalf("blob %d: %v", i, errs[i])
		}
		if back, err := s.ReadBlob(ctx, digest.FromBytes(b).String(), int64(len(b))); err != nil || !bytes.Equal(back, b) {
			t.Fatalf("blob %d differs: %v", i, err)
		}
		// Every write holds its blob, and none leaves a write behind.
		res, err := s.client.LeasesService().ListResources(s.ns(ctx), leases.Lease{ID: leaseOf[i]})
		if err != nil || len(res) != 1 || res[0].ID != digest.FromBytes(b).String() {
			t.Fatalf("blob %d: lease holds %v, %v", i, res, err)
		}
		if leftIngest(t, s, leaseOf[i]) {
			t.Fatalf("blob %d: a write left behind", i)
		}
	}
}

// The probe refuses an ingest directory that isn't containerd's.
func TestContainerd_DirectWritesStayOffWithoutContainerdsIngest(t *testing.T) {
	s := streamStore(t, "direct-off")
	s.ingestDir = t.TempDir()
	if err := s.EnableDirectWrites(context.Background(), "it"); err == nil || s.DirectWrites() {
		t.Fatalf("direct writes on with a foreign ingest directory: %v", err)
	}
	// Writes still work, through the API.
	blob := randomBlob(t, 3<<20)
	lease := testLease(t, s)
	if _, err := s.WriteBlob(context.Background(), lease, lease, digest.FromBytes(blob).String(), int64(len(blob)), bytes.NewReader(blob)); err != nil {
		t.Fatal(err)
	}
}

// chunkedReader hands out b in reads of at most n bytes, like a socket.
type chunkedReader struct {
	b []byte
	n int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	if len(p) > r.n {
		p = p[:r.n]
	}
	k := copy(p, r.b)
	r.b = r.b[k:]
	return k, nil
}
