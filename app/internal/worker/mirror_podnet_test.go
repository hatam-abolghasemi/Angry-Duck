package worker

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMirror_RequiresClientToken(t *testing.T) {
	m := NewMirror(nil, "n1", testToken, "http://controller")
	m.RequireClientToken("client-token")
	call := func(auth string) int {
		req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		m.Handler().ServeHTTP(rec, req)
		return rec.Code
	}
	if c := call(""); c != http.StatusUnauthorized {
		t.Errorf("no token: %d, want 401", c)
	}
	if c := call("Bearer " + testToken); c != http.StatusUnauthorized {
		t.Errorf("the shared token must not open the mirror: %d", c)
	}
	if c := call("Bearer client-token"); c != http.StatusOK {
		t.Errorf("valid token: %d, want 200", c)
	}
}

func TestHostsConfig_TokenHeaderAndRemoveOwn(t *testing.T) {
	dir := t.TempDir()
	h := &HostsConfig{ConfigDir: dir, Mirror: "10.233.64.7:18082", Token: "tok", NodeID: "n1", Extra: []string{"registry.example.com"}}
	h.Sync(nil, true)
	p := filepath.Join(dir, "registry.example.com", "hosts.toml")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`[host."http://10.233.64.7:18082"]`, `[host."http://10.233.64.7:18082".header]`, `Authorization = "Bearer tok"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("hosts.toml lacks %q:\n%s", want, b)
		}
	}
	if st, _ := os.Stat(p); st.Mode().Perm() != 0o600 {
		t.Errorf("hosts.toml mode %v: it holds a token", st.Mode().Perm())
	}

	// Another worker pod's file (surge rollout) and a hand-written one stay.
	other := filepath.Join(dir, "other.example.com", "hosts.toml")
	os.MkdirAll(filepath.Dir(other), 0o755)
	os.WriteFile(other, []byte(hostsToml("other.example.com", "10.233.64.99:18082", "x")), 0o600)
	manual := filepath.Join(dir, "manual.example.com", "hosts.toml")
	os.MkdirAll(filepath.Dir(manual), 0o755)
	os.WriteFile(manual, []byte(`server = "https://manual.example.com"`), 0o644)

	if n := h.RemoveOwn(); n != 1 {
		t.Fatalf("RemoveOwn removed %d, want 1", n)
	}
	if _, err := os.Stat(filepath.Dir(p)); !os.IsNotExist(err) {
		t.Error("our empty registry directory was left behind")
	}
	for _, keep := range []string{other, manual} {
		if _, err := os.Stat(keep); err != nil {
			t.Errorf("%s removed: %v", keep, err)
		}
	}
	h.Sync(nil, true)
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Error("Sync wrote again after RemoveOwn")
	}
}
