package worker

// mirror_test.go's TestHostsTOML* cover Ensure/Remove and foreign-file
// preservation. Two things were still untested: splitRegistry's
// docker.io special case (its dedicated test writes a foreign file at
// that path first, so Ensure skips it and the real registry-1.docker.io
// mapping was never actually exercised) and Check()'s two warnings
// (missing config_path, a shadowing per-registry directory), which
// nothing called at all.

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// captureLog redirects the package's underlying logger for the duration
// of fn and returns everything written to it. logging.go wraps the
// standard library's default logger, so redirecting that is sufficient.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(prev)
	fn()
	return buf.String()
}

func TestSplitRegistry(t *testing.T) {
	cases := []struct {
		entry      string
		wantDir    string
		wantServer string
	}{
		{"*", "_default", ""},
		{"_default", "_default", ""},
		{"docker.io", "docker.io", "https://registry-1.docker.io"},
		{"registry.internal-registry.example.com", "registry.internal-registry.example.com", "https://registry.internal-registry.example.com"},
		{"http://127.0.0.1:5000", "127.0.0.1:5000", "http://127.0.0.1:5000"},
		{"https://registry.internal-registry.example.com", "registry.internal-registry.example.com", "https://registry.internal-registry.example.com"},
	}
	for _, c := range cases {
		dir, server := splitRegistry(c.entry)
		if dir != c.wantDir || server != c.wantServer {
			t.Errorf("splitRegistry(%q) = (%q, %q), want (%q, %q)", c.entry, dir, server, c.wantDir, c.wantServer)
		}
	}
}

func TestHostsTOML_DockerIOGetsTheRegistryOneMirrorURL(t *testing.T) {
	// Unlike the docker.io case in mirror_test.go (which pre-seeds a
	// foreign file so Ensure never writes one), this leaves the
	// directory empty so the real docker.io -> registry-1.docker.io
	// mapping actually gets rendered and checked.
	dir := t.TempDir()
	hx, _ := NewHostExec("")
	h := NewHostsTOML(hx, dir, []string{"docker.io"}, "http://127.0.0.1:18081")
	h.Ensure()

	b, err := os.ReadFile(filepath.Join(dir, "docker.io", "hosts.toml"))
	if err != nil {
		t.Fatalf("expected angryduck to write docker.io/hosts.toml: %v", err)
	}
	if !strings.Contains(string(b), `server = "https://registry-1.docker.io"`) {
		t.Fatalf("docker.io must be pinned to registry-1.docker.io, got:\n%s", b)
	}
}

func TestRenderHostsTOML_WithServer(t *testing.T) {
	got := renderHostsTOML("https://registry.internal-registry.example.com", "http://127.0.0.1:18081")
	for _, want := range []string{
		hostsMarker,
		`server = "https://registry.internal-registry.example.com"`,
		`[host."http://127.0.0.1:18081"]`,
		`capabilities = ["pull"]`,
		`dial_timeout = "200ms"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("renderHostsTOML output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "resolve") {
		t.Error("must never include resolve capability — tags always resolve at origin")
	}
}

func TestRenderHostsTOML_NoServerKeepsOrigin(t *testing.T) {
	got := renderHostsTOML("", "http://127.0.0.1:18081")
	if strings.Contains(got, "\nserver") {
		t.Errorf("must not emit a server line for _default, got:\n%s", got)
	}
	if !strings.Contains(got, `[host."http://127.0.0.1:18081"]`) {
		t.Errorf("mirror entry missing:\n%s", got)
	}
}

func TestCheck_WarnsWhenConfigPathMissing(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "etc", "containerd"), 0o755); err != nil {
		t.Fatal(err)
	}
	// containerd config.toml exists but never sets config_path.
	cfg := "version = 2\n[plugins.\"io.containerd.grpc.v1.cri\".registry]\n"
	if err := os.WriteFile(filepath.Join(root, "etc/containerd/config.toml"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	hx, _ := NewHostExec(root)
	h := NewHostsTOML(hx, "/etc/containerd/certs.d", []string{"*"}, "http://127.0.0.1:18081")

	out := captureLog(t, h.Check)
	if !strings.Contains(out, "config_path") {
		t.Fatalf("expected a warning about missing config_path, got:\n%s", out)
	}
}

func TestCheck_NoWarningWhenConfigPathIsSet(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "etc", "containerd"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := "version = 2\n[plugins.\"io.containerd.grpc.v1.cri\".registry]\n  config_path = \"/etc/containerd/certs.d\"\n"
	if err := os.WriteFile(filepath.Join(root, "etc/containerd/config.toml"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	hx, _ := NewHostExec(root)
	h := NewHostsTOML(hx, "/etc/containerd/certs.d", []string{"*"}, "http://127.0.0.1:18081")

	out := captureLog(t, h.Check)
	if strings.Contains(out, "config_path does not point") {
		t.Fatalf("did not expect a config_path warning, got:\n%s", out)
	}
}

func TestCheck_WarnsAboutShadowingRegistryDirectory(t *testing.T) {
	root := t.TempDir()
	dir := "/etc/containerd/certs.d"
	// A registry with its own directory NOT written by angryduck, while
	// we're configured to cover everything via "*" (_default). containerd
	// will use this directory instead of _default, silently bypassing us.
	foreign := filepath.Join(root, dir, "some-other-registry.example.com", "hosts.toml")
	if err := os.MkdirAll(filepath.Dir(foreign), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(foreign, []byte("server = \"https://some-other-registry.example.com\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// containerd config.toml with a correct config_path, so only the
	// shadowing warning (not the config_path one) should fire.
	if err := os.MkdirAll(filepath.Join(root, "etc", "containerd"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := "config_path = \"" + dir + "\"\n"
	if err := os.WriteFile(filepath.Join(root, "etc/containerd/config.toml"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	hx, _ := NewHostExec(root)
	h := NewHostsTOML(hx, dir, []string{"*"}, "http://127.0.0.1:18081")

	out := captureLog(t, h.Check)
	if !strings.Contains(out, "some-other-registry.example.com") {
		t.Fatalf("expected a shadowing warning naming the foreign registry dir, got:\n%s", out)
	}
}

func TestCheck_NoShadowingWarningForOwnPriorFile(t *testing.T) {
	root := t.TempDir()
	dir := "/etc/containerd/certs.d"
	// A per-registry file that IS ours (from an earlier, non-"*" config)
	// must not be reported as a foreign shadow.
	mine := filepath.Join(root, dir, "registry.internal-registry.example.com", "hosts.toml")
	if err := os.MkdirAll(filepath.Dir(mine), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mine, []byte(hostsMarker+"\nserver = \"https://registry.internal-registry.example.com\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "etc", "containerd"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := "config_path = \"" + dir + "\"\n"
	if err := os.WriteFile(filepath.Join(root, "etc/containerd/config.toml"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	hx, _ := NewHostExec(root)
	h := NewHostsTOML(hx, dir, []string{"*"}, "http://127.0.0.1:18081")

	out := captureLog(t, h.Check)
	if strings.Contains(out, "registry.internal-registry.example.com") {
		t.Fatalf("must not warn about our own previously-written file, got:\n%s", out)
	}
}

func TestCheck_NoShadowingWarningWhenNotCoveringDefault(t *testing.T) {
	// warnShadowing only matters when "*"/_default is in play; a config
	// that only manages specific registries has no _default to shadow.
	root := t.TempDir()
	dir := "/etc/containerd/certs.d"
	foreign := filepath.Join(root, dir, "some-other-registry.example.com", "hosts.toml")
	if err := os.MkdirAll(filepath.Dir(foreign), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(foreign, []byte("server = \"https://x\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "etc", "containerd"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := "config_path = \"" + dir + "\"\n"
	if err := os.WriteFile(filepath.Join(root, "etc/containerd/config.toml"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	hx, _ := NewHostExec(root)
	h := NewHostsTOML(hx, dir, []string{"registry.internal-registry.example.com"}, "http://127.0.0.1:18081")

	out := captureLog(t, h.Check)
	if strings.Contains(out, "some-other-registry.example.com") {
		t.Fatalf("must not warn about shadowing when not covering _default, got:\n%s", out)
	}
}
