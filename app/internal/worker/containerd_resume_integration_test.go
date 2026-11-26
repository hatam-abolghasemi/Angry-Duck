//go:build integration

// Resumed writes against a real containerd, through the content API and
// with direct writes: a write that stops part-way stays in its ingest, the
// next write under the same ref goes on from what containerd says is
// there, and containerd still checks the whole blob at commit.
//
//	ANGRYDUCK_CONTAINERD=/run/containerd/containerd.sock ANGRYDUCK_CONTAINERD_ROOT=/var/lib/containerd \
//	  go test -tags integration -run Resume ./internal/worker/
package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"angryduck/internal/model"
)

// resumeModes are the two ways a store writes blobs.
func resumeModes(t *testing.T) map[string]*ContainerdStore {
	return map[string]*ContainerdStore{
		"api":    streamStore(t, "resume-api"),
		"direct": directStore(t, "resume-direct"),
	}
}

// failingAfter sends b[from:cut], then fails.
func failingAfter(b []byte, cut int) BlobSource {
	return func(off int64) (io.ReadCloser, int64, error) {
		return io.NopCloser(io.MultiReader(bytes.NewReader(b[off:cut]), failReader{errors.New("connection reset")})), off, nil
	}
}

// fromOffset sends b from wherever it is asked to, and records where.
func fromOffset(b []byte, asked *int64) BlobSource {
	return func(off int64) (io.ReadCloser, int64, error) {
		*asked = off
		return io.NopCloser(bytes.NewReader(b[off:])), off, nil
	}
}

func keepAll(error) bool { return true }

func committed(t *testing.T, s *ContainerdStore, d string, want []byte) {
	t.Helper()
	body, n, err := s.OpenContent(context.Background(), d)
	if err != nil {
		t.Fatalf("committed blob: %v", err)
	}
	defer body.Close()
	got, _ := io.ReadAll(body)
	if n != int64(len(want)) || !bytes.Equal(got, want) {
		t.Fatalf("committed blob differs: %d bytes", len(got))
	}
}

func TestContainerd_ResumeGoesOnFromAnInterruptedWrite(t *testing.T) {
	for mode, s := range resumeModes(t) {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			blob := randomBlob(t, 5<<20+12345)
			d := sha(blob)
			size := int64(len(blob))
			ref := "resume-" + randomSuffix()
			first, second := testLease(t, s), testLease(t, s)

			cut := 3<<20 + 777
			res, err := s.WriteBlobFrom(ctx, first, ref, d, size, failingAfter(blob, cut), keepAll)
			if err == nil || !res.Kept {
				t.Fatalf("interrupted write: %+v, %v; want kept and an error", res, err)
			}
			if mode == "direct" && res.Received != int64(cut) {
				t.Fatalf("direct write kept %d bytes, want %d", res.Received, cut)
			}
			if res.Received <= 0 || res.Received > int64(cut) {
				t.Fatalf("kept %d bytes of %d sent", res.Received, cut)
			}
			if !leftIngest(t, s, ref) {
				t.Fatal("the interrupted write's ingest is gone")
			}

			// The next write, under another job's lease, goes on from there.
			var asked int64 = -1
			res2, err := s.WriteBlobFrom(ctx, second, ref, d, size, fromOffset(blob, &asked), keepAll)
			if err != nil {
				t.Fatalf("resumed write: %v", err)
			}
			if asked != res.Received || res2.Resumed != res.Received || res2.Received != size-res.Received || res2.Dropped != 0 {
				t.Fatalf("asked from %d, result %+v; want it to go on from %d", asked, res2, res.Received)
			}
			committed(t, s, d, blob)
			if leftIngest(t, s, ref) {
				t.Fatal("ingest left after the commit")
			}
			// The lease of the job that committed it holds it.
			if held, err := s.HoldContent(ctx, second, d); err != nil || !held {
				t.Fatalf("blob not held by the second lease: %v %v", held, err)
			}
		})
	}
}

func TestContainerd_ResumeStartsOverForASourceThatCantSkip(t *testing.T) {
	for mode, s := range resumeModes(t) {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			blob := randomBlob(t, 3<<20+5)
			d, size, ref, lease := sha(blob), int64(len(blob)), "resume-"+randomSuffix(), testLease(t, s)
			res, _ := s.WriteBlobFrom(ctx, lease, ref, d, size, failingAfter(blob, 2<<20+1), keepAll)
			if !res.Kept {
				t.Fatalf("first write not kept: %+v", res)
			}
			whole := func(int64) (io.ReadCloser, int64, error) { return io.NopCloser(bytes.NewReader(blob)), 0, nil }
			res2, err := s.WriteBlobFrom(ctx, lease, ref, d, size, whole, keepAll)
			if err != nil {
				t.Fatalf("write from 0: %v", err)
			}
			if res2.Dropped != res.Received || res2.Resumed != 0 || res2.Received != size {
				t.Fatalf("result %+v; want %d dropped and the whole blob written", res2, res.Received)
			}
			committed(t, s, d, blob)
		})
	}
}

func TestContainerd_ResumeOntoBadBytesFailsTheCommitAndLeavesNothing(t *testing.T) {
	for mode, s := range resumeModes(t) {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			blob := randomBlob(t, 3<<20+99)
			bad := append([]byte(nil), blob...)
			copy(bad[100:], "not the blob's bytes")
			d, size, ref, lease := sha(blob), int64(len(blob)), "resume-"+randomSuffix(), testLease(t, s)
			if res, _ := s.WriteBlobFrom(ctx, lease, ref, d, size, failingAfter(bad, 2<<20), keepAll); !res.Kept {
				t.Fatalf("first write not kept: %+v", res)
			}
			var asked int64
			if _, err := s.WriteBlobFrom(ctx, lease, ref, d, size, fromOffset(blob, &asked), keepAll); err == nil {
				t.Fatal("a blob with wrong bytes was committed")
			}
			if leftIngest(t, s, ref) {
				t.Fatal("a write containerd rejected was kept")
			}
			res, err := s.WriteBlobFrom(ctx, lease, ref, d, size, fromOffset(blob, &asked), keepAll)
			if err != nil || asked != 0 || res.Resumed != 0 {
				t.Fatalf("after the rejection: asked from %d, %+v, %v; want a clean write from 0", asked, res, err)
			}
			committed(t, s, d, blob)
		})
	}
}

// A write cancelled part-way (a hand-off) is kept when keep says so, and
// aborted as before when it doesn't.
// A blob containerd already holds in another namespace (it shares it) is
// committed without reading the source. Before 1.15.0 a direct write of
// one failed: there is no ingest file to write.
func TestContainerd_WriteOfABlobAnotherNamespaceHoldsCommitsWithoutFetching(t *testing.T) {
	other := streamStore(t, "resume-other")
	blob := randomBlob(t, 2<<20+1)
	d, size := sha(blob), int64(len(blob))
	if _, err := other.WriteBlob(context.Background(), testLease(t, other), "have-"+randomSuffix(), d, size, bytes.NewReader(blob)); err != nil {
		t.Fatal(err)
	}
	for mode, s := range resumeModes(t) {
		t.Run(mode, func(t *testing.T) {
			read := false
			src := func(int64) (io.ReadCloser, int64, error) {
				read = true
				return io.NopCloser(bytes.NewReader(blob)), 0, nil
			}
			res, err := s.WriteBlobFrom(context.Background(), testLease(t, s), "resume-"+randomSuffix(), d, size, src, keepAll)
			if err != nil || !res.Present || read || res.Received != 0 {
				t.Fatalf("result %+v, %v, source read %v; want it committed from what containerd has", res, err, read)
			}
			committed(t, s, d, blob)
		})
	}
}

func TestContainerd_ResumeKeepsOrAbortsACancelledWrite(t *testing.T) {
	for mode, s := range resumeModes(t) {
		t.Run(mode, func(t *testing.T) {
			blob := randomBlob(t, 4<<20)
			d, size, lease := sha(blob), int64(len(blob)), testLease(t, s)
			for _, keep := range []bool{true, false} {
				ref := "resume-" + randomSuffix()
				ctx, cancel := context.WithCancel(context.Background())
				src := func(off int64) (io.ReadCloser, int64, error) {
					pr, pw := io.Pipe()
					go func() {
						_, _ = pw.Write(blob[:2<<20])
						// The store has read them: cancel, as a hand-off does.
						cancel()
						pw.CloseWithError(context.Canceled)
					}()
					return pr, 0, nil
				}
				res, err := s.WriteBlobFrom(ctx, lease, ref, d, size, src, func(error) bool { return keep })
				cancel()
				if err == nil {
					t.Fatal("a cancelled write succeeded")
				}
				if res.Kept != keep || leftIngest(t, s, ref) != keep {
					t.Fatalf("keep=%v: result %+v, ingest left %v", keep, res, leftIngest(t, s, ref))
				}
			}
		})
	}
}

// End to end on real containerd: a registry pull cut off part-way, then
// the blob from a peer worker, which sends only the rest; and a pull still
// running taken over by a peer copy, with a stream reading the blob across
// the hand-off.
func TestContainerd_SpreadResumesFromAPeer(t *testing.T) {
	for _, direct := range []bool{false, true} {
		name := map[bool]string{false: "api", true: "direct"}[direct]
		t.Run(name, func(t *testing.T) {
			blob := randomBlob(t, 9<<20+4321)
			d, size := sha(blob), int64(len(blob))

			// The peer's store is in memory: on the same containerd, the
			// receiver would find the blob shared from the peer's namespace.
			srcStore := newFakeSpreadStore()
			srcStore.blobs[d] = blob
			src := NewSpread(srcStore, &fakeRegistry{}, spreadToken, "src", "linux/amd64", "", time.Minute)
			srcMux := http.NewServeMux()
			src.Register(srcMux)
			var mu sync.Mutex
			var ranges []string
			srcSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				ranges = append(ranges, r.Header.Get("Range"))
				mu.Unlock()
				srcMux.ServeHTTP(w, r)
			}))
			t.Cleanup(srcSrv.Close)

			var store *ContainerdStore
			if direct {
				store = directStore(t, "resume-dst")
			} else {
				store = streamStore(t, "resume-dst")
			}
			ctl := newControllerSink(t)
			sp := NewSpread(store, &brokenRegistry{blob: blob, cut: 5<<20 + 3}, spreadToken, "dst-"+randomSuffix()[:6], "linux/amd64", ctl.srv.URL, time.Minute)
			if !sp.EnableResume() {
				t.Fatal("a ContainerdStore resumes")
			}
			mux := http.NewServeMux()
			sp.Register(mux)
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)

			postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Image: "r.io/app:1", Digest: d, Size: size, Path: model.SpreadPathRegistry})
			sp.wg.Wait()
			r := ctl.all()
			if len(r) != 1 || r[0].Result != model.SpreadResultFailed {
				t.Fatalf("reports %+v", r)
			}
			kept := r[0].Bytes
			postSpread(t, srv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: size, Path: model.SpreadPathPeer,
				Peer: &model.Holder{NodeID: "src", Address: strings.TrimPrefix(srcSrv.URL, "http://")}})
			sp.wg.Wait()
			r = ctl.all()
			last := r[len(r)-1]
			if last.Result != model.SpreadResultOK {
				t.Fatalf("resume: %+v", last)
			}
			mu.Lock()
			got := append([]string(nil), ranges...)
			mu.Unlock()
			if direct {
				// Direct writes keep every byte received.
				if want := "bytes=" + strconv.FormatInt(kept, 10) + "-"; len(got) != 1 || got[0] != want || last.Bytes != size-kept {
					t.Fatalf("Range %q, %d bytes; want %s and only the rest", got, last.Bytes, want)
				}
			} else if len(got) != 1 || !strings.HasPrefix(got[0], "bytes=") || last.Bytes >= size {
				// Through the API, containerd has what it acknowledged.
				t.Fatalf("Range %q, %d bytes; want a resume", got, last.Bytes)
			}
			committed(t, store, d, blob)
		})
	}
}
