package worker

import "testing"

func TestIsPeerDisconnect(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"write unix ->@: broken pipe", true},
		{"read: connection reset by peer", true},
		{"use of closed network connection", true},
		{`ctr: failed to ingest "blobs/sha256/4f6acb6...": failed to read expected number of bytes: unexpected EOF`, false},
		{"ctr: content digest sha256:xyz: not found", false},
		{"", false},
		{"some completely unrecognized ctr error we've never seen before", false},
	}
	for _, c := range cases {
		if got := isPeerDisconnect(c.msg); got != c.want {
			t.Errorf("isPeerDisconnect(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}

func TestIsPeerDisconnectCaseInsensitive(t *testing.T) {
	if !isPeerDisconnect("Write: Broken Pipe") {
		t.Fatal("classification must not depend on case")
	}
}
