package worker

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// The receiving side of the slow-chain guard: the controller offers to
// restart a late transfer from a node that holds the blob (SpreadFetch
// Replace), and the receiver decides, because only it knows how far and
// how fast its transfer is going.

// replaceMargin: restart only when the new source should finish at least
// this much sooner than the running transfer (a restart throws away what
// arrived so far).
const replaceMargin = 1.25

// rateWindow: the running transfer is judged on its recent speed, not its
// average, so a stream that just sped up (its source got a faster copy)
// isn't thrown away for how slow it was before.
const rateWindow = 3 * time.Second

// xferProgress counts the bytes a transfer received and keeps a few
// timestamps to tell its recent speed.
type xferProgress struct {
	mu    sync.Mutex
	began time.Time
	got   int64
	marks [4]progressMark // every ~rateWindow/2, oldest first once full
	n     int
}

type progressMark struct {
	at  time.Time
	got int64
}

func (p *xferProgress) start(now time.Time) {
	p.mu.Lock()
	p.began = now
	p.mu.Unlock()
}

func (p *xferProgress) add(n int, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.got += int64(n)
	if p.n > 0 && now.Sub(p.marks[p.n-1].at) < rateWindow/2 {
		return
	}
	if p.n == len(p.marks) {
		copy(p.marks[:], p.marks[1:])
		p.n--
	}
	p.marks[p.n] = progressMark{at: now, got: p.got}
	p.n++
}

// state returns the bytes received and the recent rate (bytes per second
// over about the last rateWindow; over the whole transfer if younger).
func (p *xferProgress) state(now time.Time) (got int64, rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	from := progressMark{at: p.began}
	for i := 0; i < p.n; i++ {
		if now.Sub(p.marks[i].at) <= rateWindow {
			break
		}
		from = p.marks[i]
	}
	if el := now.Sub(from.at).Seconds(); el > 0 {
		rate = float64(p.got-from.got) / el
	}
	return p.got, rate
}

// recvRateMinBytes: transfers smaller than this don't tell what this
// node's link takes.
const recvRateMinBytes = 4 << 20

// expectLocked caps the speed a restart of old is expected at by what this
// node can take: its fastest finished transfer, less what its other
// transfers use now. Without that history, a node already receiving other
// transfers is taken to be at its link's limit (a restart would only share
// it): the restart is expected at old's own speed, so it is kept. Caller
// holds s.mu.
func (s *Spread) expectLocked(old *spreadXfer, expect float64, now time.Time) float64 {
	var others float64
	busy := false
	for _, x := range s.xfers {
		if x == old {
			continue
		}
		busy = true
		_, r := x.progress.state(now)
		others += r
	}
	switch {
	case s.peakRecv > 0:
		if room := s.peakRecv - others; room < expect {
			expect = room
		}
	case busy:
		if _, r := old.progress.state(now); r < expect {
			expect = r
		}
	}
	if expect < 0 {
		expect = 0
	}
	return expect
}

// progressReader counts what passes through into p.
type progressReader struct {
	r io.Reader
	p *xferProgress
}

func (r *progressReader) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	if n > 0 {
		r.p.add(n, time.Now())
	}
	return n, err
}

// keepRunning decides a Replace order: keep the running transfer unless
// restarting at expect bytes per second should finish clearly sooner.
func keepRunning(p *xferProgress, size int64, expect float64, now time.Time) (bool, string) {
	got, rate := p.state(now)
	if expect <= 0 {
		return true, "keeping: no room to expect a faster restart"
	}
	restart := float64(size) / expect
	if rate <= 0 {
		return false, fmt.Sprintf("stalled after %d of %d bytes; restarting should take %.1fs", got, size, restart)
	}
	left := float64(size-got) / rate
	if restart*replaceMargin < left {
		return false, fmt.Sprintf("%d of %d bytes at %.1f MB/s, %.1fs to go; restarting should take %.1fs", got, size, rate/1e6, left, restart)
	}
	return true, fmt.Sprintf("keeping: %d of %d bytes at %.1f MB/s, %.1fs to go; restarting would take %.1fs", got, size, rate/1e6, left, restart)
}
