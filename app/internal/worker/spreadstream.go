package worker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
)

// Streaming: a node serves a spread blob to peers while it is still
// receiving it, so a large blob doesn't cross the cluster one whole
// transfer per hop. The blob stays the unit of work: every receiver still
// writes the whole blob and containerd still checks its digest at commit,
// on every node. Only the waiting between hops goes away.
//
// The bytes come from containerd's own ingest file (see OpenIngest), up to
// what containerd has acknowledged writing, so nothing is buffered or
// written twice. When the transfer feeding a stream ends, the stream goes
// on from whatever else has the bytes: the committed blob (the transfer
// finished, or the blob arrived over the other path), or the other path's
// transfer of the same blob. With none of them left, the connection is cut
// short and the receiver's write fails; its report says the source failed
// it, so the controller doesn't hold it against the receiver.

var spreadStreamedBytes = metrics.NewCounterVec(
	"angryduck_worker_spread_streamed_bytes_total",
	"Bytes sent to peers from blobs this node was still receiving (also counted in spread_bytes_total{path=\"served\"}).",
	"node",
)

// StreamHeader marks a response served from a blob still arriving.
const StreamHeader = "Angryduck-Stream"

// IngestStore is what streaming needs on top of SpreadStore.
// ContainerdStore has it.
type IngestStore interface {
	CanStream() bool
	OpenIngest(ref string) (*os.File, error)
}

// Feed states.
const (
	feedRunning = iota
	feedCommitted
	feedEnded // failed or cancelled: its bytes can't be trusted to grow
)

// feed is one transfer of a blob into the content store, followed by the
// streams that serve it.
type feed struct {
	ref     string
	opened  bool  // the ingest file holds this write, from zero
	written int64 // bytes containerd has written so far
	state   int
}

// arriving is a blob coming in over one or two paths, with a channel
// closed (and replaced) whenever any of its feeds moves.
type arriving struct {
	size    int64
	feeds   []*feed
	changed chan struct{}
}

// feedObserver ties WriteBlob's progress to a feed.
type feedObserver struct {
	s      *Spread
	digest string
	f      *feed
}

func (o feedObserver) opened() {
	o.s.feedUpdate(o.digest, o.f, func(f *feed) { f.opened, f.written = true, 0 })
}
func (o feedObserver) wrote(total int64) {
	o.s.feedUpdate(o.digest, o.f, func(f *feed) { f.written = total })
}

// feedStart registers a transfer of dgst as a feed. Nil when this node
// can't stream.
func (s *Spread) feedStart(dgst, ref string, size int64) *feed {
	if s.ingest == nil {
		return nil
	}
	f := &feed{ref: ref}
	s.streamMu.Lock()
	a := s.arriving[dgst]
	if a == nil {
		a = &arriving{size: size, changed: make(chan struct{})}
		s.arriving[dgst] = a
	}
	a.feeds = append(a.feeds, f)
	s.streamMu.Unlock()
	return f
}

// feedUpdate changes f under the lock and wakes dgst's streams.
func (s *Spread) feedUpdate(dgst string, f *feed, change func(*feed)) {
	s.streamMu.Lock()
	change(f)
	if a := s.arriving[dgst]; a != nil {
		close(a.changed)
		a.changed = make(chan struct{})
	}
	s.streamMu.Unlock()
}

// feedEnd marks f done and forgets dgst once no feed of it runs. Streams
// holding f still see how it ended.
func (s *Spread) feedEnd(dgst string, f *feed, committed bool) {
	if f == nil {
		return
	}
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	if committed {
		f.state = feedCommitted
	} else {
		f.state = feedEnded
	}
	a := s.arriving[dgst]
	if a == nil {
		return
	}
	kept := a.feeds[:0]
	for _, x := range a.feeds {
		if x != f {
			kept = append(kept, x)
		}
	}
	a.feeds = kept
	close(a.changed)
	a.changed = make(chan struct{})
	if len(a.feeds) == 0 {
		delete(s.arriving, dgst)
	}
}

// streamPick returns the feed to read from next: the one that has written
// the most, else any still running (waiting for it to open). changed fires
// on the next move. ok is false when dgst isn't arriving any more.
func (s *Spread) streamPick(dgst string) (best *feed, written int64, changed <-chan struct{}, ok bool) {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	a := s.arriving[dgst]
	if a == nil {
		return nil, 0, nil, false
	}
	for _, f := range a.feeds {
		if f.state != feedRunning {
			continue
		}
		w := int64(-1)
		if f.opened {
			w = f.written
		}
		if best == nil || w > written {
			best, written = f, w
		}
	}
	return best, written, a.changed, true
}

// feedNow reads f's state and progress.
func (s *Spread) feedNow(dgst string, f *feed) (state int, opened bool, written int64, changed <-chan struct{}) {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	state, opened, written = f.state, f.opened, f.written
	if a := s.arriving[dgst]; a != nil {
		changed = a.changed
	}
	return
}

// arrivingSize returns the size of dgst if it is arriving here.
func (s *Spread) arrivingSize(dgst string) (int64, bool) {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	a := s.arriving[dgst]
	if a == nil {
		return 0, false
	}
	return a.size, true
}

// serveStream serves dgst while it is still arriving. False: it isn't
// arriving (nothing was written to w).
func (s *Spread) serveStream(w http.ResponseWriter, r *http.Request, dgst string) bool {
	size, ok := s.arrivingSize(dgst)
	if !ok {
		return false
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", dgst)
	w.Header().Set(StreamHeader, "1")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	if r.Method == http.MethodHead {
		return true
	}
	n, err := s.stream(r.Context(), w, dgst, size)
	spreadBytesTotal.Add(n, s.nodeID, "served")
	spreadStreamedBytes.Add(n, s.nodeID)
	if err != nil {
		logging.Infof("angryduck-worker[%s]: spread: streaming %s to %s stopped after %d of %d bytes: %v", s.nodeID, dgst, r.RemoteAddr, n, size, err)
		panic(http.ErrAbortHandler) // short body: the receiver's write fails
	}
	logging.Debugf("angryduck-worker[%s]: spread: streamed %s (%d bytes) to %s", s.nodeID, dgst, n, r.RemoteAddr)
	return true
}

var errStreamSourceGone = errors.New("the transfer feeding the stream ended and the blob isn't here")

// stream copies dgst to w from offset 0 to size, following whichever feed
// has the bytes, then the committed blob. It returns the bytes sent.
func (s *Spread) stream(ctx context.Context, w io.Writer, dgst string, size int64) (int64, error) {
	var (
		off  int64
		cur  *feed
		file *os.File // cur's ingest file, or the committed blob
	)
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	// send copies [off, upto) of file to w; the file's own ReadFrom path
	// keeps sendfile.
	send := func(upto int64) error {
		if _, err := file.Seek(off, io.SeekStart); err != nil {
			return err
		}
		n, err := copyToWriter(w, &io.LimitedReader{R: file, N: upto - off})
		off += n
		if err == nil && off < upto {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	wait := func(changed <-chan struct{}) error {
		if changed == nil {
			return nil
		}
		select {
		case <-changed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for off < size {
		if err := ctx.Err(); err != nil {
			return off, err
		}
		if cur == nil {
			f, _, changed, arriving := s.streamPick(dgst)
			if f == nil {
				// Nothing running: committed, or gone.
				if s.have(ctx, dgst) {
					return s.streamCommitted(ctx, w, dgst, off, size)
				}
				if !arriving {
					return off, errStreamSourceGone
				}
				if err := wait(changed); err != nil {
					return off, err
				}
				continue
			}
			cur = f
			if file != nil {
				file.Close()
				file = nil
			}
		}
		state, opened, written, changed := s.feedNow(dgst, cur)
		switch {
		case state == feedCommitted:
			if file == nil {
				// Never opened its ingest file: read the committed blob.
				return s.streamCommitted(ctx, w, dgst, off, size)
			}
			written = size // the same file, complete
		case state == feedEnded:
			// Its bytes so far are still good, but no more are coming.
			if file != nil && opened && written > off {
				if err := send(written); err != nil {
					return off, err
				}
			}
			cur = nil
			continue
		case !opened:
			if err := wait(changed); err != nil {
				return off, err
			}
			continue
		}
		if file == nil {
			f, err := s.ingest.OpenIngest(cur.ref)
			if err != nil {
				// Not found (committed or aborted meanwhile, or a layout
				// we don't know): wait for this feed to end, then go on
				// from the committed blob or another feed.
				if state == feedCommitted {
					return s.streamCommitted(ctx, w, dgst, off, size)
				}
				if err := wait(changed); err != nil {
					return off, err
				}
				continue
			}
			file = f
		}
		if written > off {
			if err := send(written); err != nil {
				return off, err
			}
			continue
		}
		// Nothing new here. Another copy of the blob coming in over the
		// other path may be further ahead (a slow registry pull raced by a
		// peer push): go on from that one.
		if f, w, _, _ := s.streamPick(dgst); f != nil && f != cur && w > off {
			cur = f
			file.Close()
			file = nil
			continue
		}
		if err := wait(changed); err != nil {
			return off, err
		}
	}
	return off, nil
}

// streamCommitted sends the rest of a committed blob, from off.
func (s *Spread) streamCommitted(ctx context.Context, w io.Writer, dgst string, off, size int64) (int64, error) {
	body, n, err := s.store.OpenContent(ctx, dgst)
	if err != nil {
		return off, err
	}
	defer body.Close()
	if n != size {
		return off, errors.New("committed blob has a different size")
	}
	if seeker, ok := body.(io.Seeker); ok {
		if _, err := seeker.Seek(off, io.SeekStart); err != nil {
			return off, err
		}
	} else if _, err := io.CopyN(io.Discard, body, off); err != nil {
		return off, err
	}
	sent, err := copyToWriter(w, &io.LimitedReader{R: body, N: size - off})
	off += sent
	if err == nil && off < size {
		err = io.ErrUnexpectedEOF
	}
	return off, err
}

// copyToWriter copies through w's ReadFrom when it has one (an HTTP
// response: sendfile for a file), like serveBody.
func copyToWriter(w io.Writer, r io.Reader) (int64, error) {
	if rf, ok := w.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(w, r)
}

// readTracker remembers whether reading the source failed, to tell a peer
// that cut a stream short from a local failure.
type readTracker struct {
	r   io.Reader
	mu  sync.Mutex
	err error
}

func (t *readTracker) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && err != io.EOF {
		t.mu.Lock()
		if t.err == nil {
			t.err = err
		}
		t.mu.Unlock()
	}
	return n, err
}

func (t *readTracker) failed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err != nil
}
