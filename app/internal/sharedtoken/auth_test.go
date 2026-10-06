package sharedtoken

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

const tok = "0123456789abcdef0123456789abcdef"

func call(a *Auth, header string) int {
	h := a.Guard("/x", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	if header != "" {
		req.Header.Set("Authorization", header)
	}
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec.Code
}

func TestGuard(t *testing.T) {
	cases := []struct {
		name   string
		auth   *Auth
		header string
		want   int
	}{
		{"no token configured passes", &Auth{}, "", 200},
		{"enforce rejects missing", &Auth{Token: tok}, "", 401},
		{"enforce rejects wrong", &Auth{Token: tok}, "Bearer nope", 401},
		{"enforce accepts valid", &Auth{Token: tok}, "Bearer " + tok, 200},
		{"warn passes missing", &Auth{Token: tok, Mode: Warn}, "", 200},
		{"nil auth passes", nil, "", 200},
	}
	for _, c := range cases {
		if got := call(c.auth, c.header); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

func TestParseMode(t *testing.T) {
	for in, want := range map[string]Mode{"": Enforce, "enforce": Enforce, "WARN": Warn} {
		if got, err := ParseMode(in); err != nil || got != want {
			t.Errorf("ParseMode(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := ParseMode("off"); err == nil {
		t.Error("ParseMode(off) should fail")
	}
}
