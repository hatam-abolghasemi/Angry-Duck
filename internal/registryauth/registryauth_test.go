package registryauth

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}
	return path
}

func TestLoadWithAuthField(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("myuser:mypass"))
	content := `{"auths": {"registry.internal-registry.example.com": {"auth": "` + encoded + `"}}}`
	path := writeTempConfig(t, content)

	store, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, ok := store.CredentialsFor("registry.internal-registry.example.com")
	if !ok {
		t.Fatalf("expected credentials to be found")
	}
	if got != "myuser:mypass" {
		t.Errorf("got %q, want %q", got, "myuser:mypass")
	}
	if store.Count() != 1 {
		t.Errorf("got Count()=%d, want 1", store.Count())
	}
}

func TestLoadWithExplicitUsernamePassword(t *testing.T) {
	content := `{"auths": {"git.internal-registry.example.com": {"username": "u", "password": "p"}}}`
	path := writeTempConfig(t, content)

	store, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, ok := store.CredentialsFor("git.internal-registry.example.com")
	if !ok || got != "u:p" {
		t.Fatalf("got (%q, %v), want (\"u:p\", true)", got, ok)
	}
}

func TestLoadMultipleRegistries(t *testing.T) {
	enc1 := base64.StdEncoding.EncodeToString([]byte("user1:pass1"))
	enc2 := base64.StdEncoding.EncodeToString([]byte("user2:pass2"))
	content := `{"auths": {
		"registry.internal-registry.example.com": {"auth": "` + enc1 + `"},
		"registry.example.com": {"auth": "` + enc2 + `"}
	}}`
	path := writeTempConfig(t, content)

	store, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.Count() != 2 {
		t.Fatalf("got Count()=%d, want 2", store.Count())
	}
	if got, _ := store.CredentialsFor("registry.internal-registry.example.com"); got != "user1:pass1" {
		t.Errorf("registry.internal-registry.example.com: got %q", got)
	}
	if got, _ := store.CredentialsFor("registry.example.com"); got != "user2:pass2" {
		t.Errorf("registry.example.com: got %q", got)
	}
}

func TestLoadNormalizesSchemeInAuthsKey(t *testing.T) {
	// Some tools write the auths key with a scheme prefix — image
	// references never have one, so lookups must still succeed.
	encoded := base64.StdEncoding.EncodeToString([]byte("u:p"))
	content := `{"auths": {"https://registry.internal-registry.example.com/": {"auth": "` + encoded + `"}}}`
	path := writeTempConfig(t, content)

	store, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, ok := store.CredentialsFor("registry.internal-registry.example.com"); !ok || got != "u:p" {
		t.Fatalf("got (%q, %v), want (\"u:p\", true)", got, ok)
	}
}

func TestLoadMissingFileReturnsEmptyStore(t *testing.T) {
	store, err := Load("/nonexistent/path/config.json")
	if err != nil {
		t.Fatalf("expected no error for a missing file, got: %v", err)
	}
	if _, ok := store.CredentialsFor("anything.example.com"); ok {
		t.Fatalf("expected no credentials from an empty store")
	}
	if store.Count() != 0 {
		t.Errorf("got Count()=%d, want 0", store.Count())
	}
}

func TestLoadEmptyPathReturnsEmptyStore(t *testing.T) {
	store, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.Count() != 0 {
		t.Errorf("got Count()=%d, want 0", store.Count())
	}
}

func TestLoadInvalidJSONErrors(t *testing.T) {
	path := writeTempConfig(t, `not valid json`)
	if _, err := Load(path); err == nil {
		t.Fatalf("expected an error for invalid JSON")
	}
}

func TestCredentialsForMissingRegistry(t *testing.T) {
	store := Empty()
	if _, ok := store.CredentialsFor("unknown.example.com"); ok {
		t.Fatalf("expected no credentials for an unconfigured registry")
	}
}

func TestNilStoreIsSafe(t *testing.T) {
	var store *Store
	if _, ok := store.CredentialsFor("anything.example.com"); ok {
		t.Fatalf("expected a nil store to report no credentials, not panic or succeed")
	}
	if store.Count() != 0 {
		t.Errorf("expected a nil store's Count() to be 0")
	}
}
