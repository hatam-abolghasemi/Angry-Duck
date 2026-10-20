package worker

import (
	"context"
	"sync"
)

// keyedLock is one lock per key: holders of different keys never wait on
// each other. An entry lives only while someone holds or waits for it.
type keyedLock struct {
	mu sync.Mutex
	m  map[string]*keyedEntry
}

type keyedEntry struct {
	ch    chan struct{} // capacity 1: full while held
	users int           // holding or waiting
}

func newKeyedLock() *keyedLock { return &keyedLock{m: map[string]*keyedEntry{}} }

// lock takes key's lock, or gives up when ctx is done. On success the
// returned func releases it.
func (k *keyedLock) lock(ctx context.Context, key string) (func(), error) {
	k.mu.Lock()
	e := k.m[key]
	if e == nil {
		e = &keyedEntry{ch: make(chan struct{}, 1)}
		k.m[key] = e
	}
	e.users++
	k.mu.Unlock()

	select {
	case e.ch <- struct{}{}:
		return func() {
			<-e.ch
			k.leave(key, e)
		}, nil
	case <-ctx.Done():
		k.leave(key, e)
		return nil, ctx.Err()
	}
}

func (k *keyedLock) leave(key string, e *keyedEntry) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e.users--
	if e.users == 0 && k.m[key] == e {
		delete(k.m, key)
	}
}

// len is the number of keys held or waited for, for tests.
func (k *keyedLock) len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.m)
}
