package kube

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDaemonSetGone(t *testing.T) {
	cases := []struct {
		status int
		body   string
		gone   bool
		err    bool
	}{
		{404, `{}`, true, false},
		{200, `{"metadata":{"deletionTimestamp":"2026-10-06T10:00:00Z"}}`, true, false},
		{200, `{"metadata":{}}`, false, false},
		{403, `forbidden`, false, true},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/apis/apps/v1/namespaces/angryduck/daemonsets/angryduck-worker" {
				t.Errorf("path %s", r.URL.Path)
			}
			w.WriteHeader(c.status)
			w.Write([]byte(c.body))
		}))
		gone, err := New(srv.URL, "", srv.Client()).DaemonSetGone(context.Background(), "angryduck", "angryduck-worker")
		srv.Close()
		if gone != c.gone || (err != nil) != c.err {
			t.Errorf("status %d: gone=%v err=%v", c.status, gone, err)
		}
	}
}
