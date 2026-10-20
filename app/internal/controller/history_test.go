package controller

import "testing"

func TestViaPaths(t *testing.T) {
	for _, tc := range []struct {
		paths map[string]bool
		want  string
	}{
		{nil, "local"},
		{map[string]bool{"peer": true}, "peer"},
		{map[string]bool{"registry": true, "peer": true}, "peer+registry"},
	} {
		if got := viaPaths(tc.paths); got != tc.want {
			t.Errorf("viaPaths(%v) = %q, want %q", tc.paths, got, tc.want)
		}
	}
}
