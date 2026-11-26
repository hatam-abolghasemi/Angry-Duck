package controller

import (
	"context"
	"testing"
	"time"

	"angryduck/internal/model"
)

// waitOpen waits until f's workers have want connections open (a server
// sees a closed connection a moment after the client closed it).
func waitOpen(t *testing.T, f *spreadFleet, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for f.open.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%d connection(s) open to the workers, want %d", f.open.Load(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Once every job ended, the connections to workers are idle: the next
// tick closes them instead of keeping one to every worker for good.
func TestSpreader_ClosesIdleConnectionsToWorkersOnceEveryJobEnded(t *testing.T) {
	s, f, reg := spreadSetup(t, 10)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	job := s.jobFor(img)
	for round := 0; round < 10; round++ {
		recs := f.take()
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			s.done(model.SpreadDone{Node: r.node, Job: job.id, Digest: r.Digest, Path: r.Path, Result: model.SpreadResultOK, Bytes: 100, Seconds: 2})
		}
		s.tick(context.Background())
		s.wg.Wait()
	}
	report(reg, "full", reg.worker("full").Address, 0.5, img)
	s.tick(context.Background())
	s.wg.Wait()
	if s.jobFor(img) != nil {
		t.Fatal("job still running after every node registered the image")
	}
	if f.open.Load() == 0 {
		t.Fatal("no connection kept between the spread's requests: nothing to close")
	}
	s.tick(context.Background())
	waitOpen(t, f, 0)
}

// While a job runs, its connections stay open for its next requests.
func TestSpreader_KeepsConnectionsWhileAJobRuns(t *testing.T) {
	s, f, _ := spreadSetup(t, 10)
	if _, err := s.Start(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	s.wg.Wait()
	open := f.open.Load()
	if open == 0 {
		t.Fatal("no connection to the workers after the first orders")
	}
	s.tick(context.Background())
	s.wg.Wait()
	time.Sleep(50 * time.Millisecond)
	if got := f.open.Load(); got < open {
		t.Fatalf("%d connection(s) open while the job runs, had %d: closed under a running job", got, open)
	}
}
