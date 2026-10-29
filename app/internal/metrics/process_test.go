package metrics

import "testing"

func TestParseCgroups(t *testing.T) {
	for _, c := range []struct{ in, v2, v1 string }{
		{"0::/\n", "", ""}, // a container's own namespace: the mount's root
		{"0::/kubepods/pod1/abc\n", "/kubepods/pod1/abc", ""},
		{"12:cpu,cpuacct:/x\n4:memory:/kubepods/pod1/abc\n0::/\n", "", "/kubepods/pod1/abc"},
		{"5:memory:/\n", "", ""},
	} {
		v2, v1 := parseCgroups(c.in)
		if v2 != c.v2 || v1 != c.v1 {
			t.Errorf("%q: got %q %q, want %q %q", c.in, v2, v1, c.v2, c.v1)
		}
	}
}
