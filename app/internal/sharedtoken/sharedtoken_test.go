package sharedtoken

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good")
	_ = os.WriteFile(good, []byte("0123456789abcdef0123456789abcdef\n"), 0o600)
	short := filepath.Join(dir, "short")
	_ = os.WriteFile(short, []byte("changeme"), 0o600)

	if tok, err := Load(good); err != nil || tok != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("good: %q %v", tok, err)
	}
	if _, err := Load(short); err == nil {
		t.Fatal("short token must be refused")
	}
	if _, err := Load(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("missing file must be refused")
	}
}

func TestValid(t *testing.T) {
	r := httptest.NewRequest("POST", "/", nil)
	if Valid(r, "tok") {
		t.Fatal("no header must be invalid")
	}
	Set(r, "tok")
	if !Valid(r, "tok") || Valid(r, "other") || Valid(r, "") {
		t.Fatal("wrong result")
	}
}
