//go:build integration

// Streaming against a real containerd: the ingest file is found, read
// while containerd writes it, and read on through the commit.
//
//	ANGRYDUCK_CONTAINERD=/run/containerd/containerd.sock ANGRYDUCK_CONTAINERD_ROOT=/var/lib/containerd \
//	  go test -tags integration -run Stream ./internal/worker/
package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"angryduck/internal/model"
)

func streamStore(t *testing.T, prefix string) *ContainerdStore {
	t.Helper()
	s := NewContainerdStore(testClient(t), testNS(t, prefix), "overlayfs")
	if s.UseBlobDir(os.Getenv("ANGRYDUCK_CONTAINERD_ROOT"), "/etc/containerd/config.toml") == "" {
		t.Skip("containerd's content store isn't readable here")
	}
	if err := s.VerifyStreaming(context.Background()); err != nil {
		t.Fatalf("set ANGRYDUCK_CONTAINERD_ROOT to this containerd's root: %v", err)
	}
	if !s.CanStream() {
		t.Fatal("content store mounted but CanStream is false")
	}
	return s
}

func randomBlob(t *testing.T, n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestContainerd_StreamsABlobWhileContainerdWritesIt(t *testing.T) {
	store := streamStore(t, "stream-src")
	blob := randomBlob(t, 5<<20+12345) // several 1 MiB writes and a tail
	d := digestOf(blob)
	reg, ctl := newPacedRegistry(), newControllerSink(t)
	reg.size[d] = int64(len(blob))
	sp := NewSpread(store, reg, spreadToken, "b", "linux/amd64", ctl.srv.URL, time.Minute)
	if !sp.Streams() {
		t.Fatal("a ContainerdStore with its content store mounted must stream")
	}
	srv := spreadServer(t, sp)

	startRegistryFetch(t, srv, reg, "s1", d, int64(len(blob)))
	first := 3<<20 + 100
	if _, err := reg.pipe(d).Write(blob[:first]); err != nil {
		t.Fatal(err)
	}
	resp := getContent(t, srv, d)
	if resp.StatusCode != http.StatusOK || resp.Header.Get(StreamHeader) != "1" {
		t.Fatalf("status %d stream %q", resp.StatusCode, resp.Header.Get(StreamHeader))
	}
	// Up to the last full 1 MiB write containerd acknowledged.
	if got := readN(t, resp.Body, 3<<20); !bytes.Equal(got, blob[:3<<20]) {
		t.Fatal("bytes read from the ingest file differ")
	}
	if _, err := reg.pipe(d).Write(blob[first:]); err != nil {
		t.Fatal(err)
	}
	reg.pipe(d).Close()
	rest, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(rest, blob[3<<20:]) {
		t.Fatalf("rest: %d bytes, err %v", len(rest), err)
	}
	sp.wg.Wait()
	if got := ctl.byPath()[model.SpreadPathRegistry]; got != model.SpreadResultOK {
		t.Fatalf("source's transfer = %q", got)
	}
	if have, _ := store.HasBlob(d); !have {
		t.Fatal("blob not committed on the source")
	}
	// Committed now: served as before, not streamed.
	resp = getContent(t, srv, d)
	if resp.Header.Get(StreamHeader) != "" {
		t.Fatal("a committed blob is still streamed")
	}
}

// Node to node: the receiver streams from a source still pulling, its own
// containerd verifies the digest at commit.
func TestContainerd_ReceiverCommitsAStreamedBlob(t *testing.T) {
	src, dst := streamStore(t, "stream-a"), streamStore(t, "stream-b")
	blob := randomBlob(t, 4<<20+777)
	d := digestOf(blob)
	reg := newPacedRegistry()
	reg.size[d] = int64(len(blob))
	srcSp := NewSpread(src, reg, spreadToken, "a", "linux/amd64", newControllerSink(t).srv.URL, time.Minute)
	srcSrv := spreadServer(t, srcSp)
	ctl := newControllerSink(t)
	dstSp := NewSpread(dst, &fakeRegistry{}, spreadToken, "b", "linux/amd64", ctl.srv.URL, time.Minute)
	dstSrv := spreadServer(t, dstSp)

	startRegistryFetch(t, srcSrv, reg, "s1", d, int64(len(blob)))
	_, _ = reg.pipe(d).Write(blob[:2<<20])
	r := postSpreadURL(t, dstSrv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "a", Address: strings.TrimPrefix(srcSrv, "http://")}, Stream: true})
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", r.StatusCode)
	}
	time.Sleep(200 * time.Millisecond)
	_, _ = reg.pipe(d).Write(blob[2<<20:])
	reg.pipe(d).Close()
	srcSp.wg.Wait()
	dstSp.wg.Wait()
	if got := ctl.byPath()[model.SpreadPathPeer]; got != model.SpreadResultOK {
		t.Fatalf("receiver = %q (%+v)", got, ctl.reports)
	}
	got, err := dst.ReadBlob(context.Background(), d, 8<<20)
	if err != nil || !bytes.Equal(got, blob) {
		t.Fatalf("receiver's blob: %d bytes, err %v", len(got), err)
	}
}

func TestContainerd_StreamCutShortWhenTheSourceFails(t *testing.T) {
	src, dst := streamStore(t, "stream-c"), streamStore(t, "stream-d")
	blob := randomBlob(t, 3<<20)
	d := digestOf(blob)
	reg := newPacedRegistry()
	reg.size[d] = int64(len(blob))
	srcSp := NewSpread(src, reg, spreadToken, "a", "linux/amd64", newControllerSink(t).srv.URL, time.Minute)
	srcSrv := spreadServer(t, srcSp)
	ctl := newControllerSink(t)
	dstSp := NewSpread(dst, &fakeRegistry{}, spreadToken, "b", "linux/amd64", ctl.srv.URL, time.Minute)
	dstSrv := spreadServer(t, dstSp)

	startRegistryFetch(t, srcSrv, reg, "s1", d, int64(len(blob)))
	_, _ = reg.pipe(d).Write(blob[:(3<<20)/2])
	r := postSpreadURL(t, dstSrv, "/spread/fetch", model.SpreadFetch{Job: "s1", Digest: d, Size: int64(len(blob)), Path: model.SpreadPathPeer,
		Peer: &model.Holder{NodeID: "a", Address: strings.TrimPrefix(srcSrv, "http://")}, Stream: true})
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", r.StatusCode)
	}
	time.Sleep(200 * time.Millisecond)
	reg.pipe(d).CloseWithError(errors.New("registry went away"))
	srcSp.wg.Wait()
	dstSp.wg.Wait()
	ctl.mu.Lock()
	reports := append([]model.SpreadDone(nil), ctl.reports...)
	ctl.mu.Unlock()
	if len(reports) != 1 || reports[0].Result != model.SpreadResultFailed || !reports[0].Upstream {
		t.Fatalf("reports %+v, want one failed with upstream", reports)
	}
	if have, _ := dst.HasBlob(d); have {
		t.Fatal("a partial blob was committed")
	}
}

func spreadServer(t *testing.T, sp *Spread) string {
	mux := http.NewServeMux()
	sp.Register(mux)
	srv := httptestServer(t, mux)
	return srv
}
