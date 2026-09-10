package worker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckContainerdMirrorCompatibility(t *testing.T) {
	oldRead := osReadFile
	t.Cleanup(func() { osReadFile = oldRead })
	config := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(config, []byte(`[plugins."io.containerd.cri.v1.images"]\n  use_local_image_pull = false\n`), 0o600); err != nil {
		t.Fatal(err)
	}

	fake := filepath.Join(t.TempDir(), "containerd")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\necho 'containerd github.com/containerd/containerd 2.2.3'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := CheckContainerdMirrorCompatibility(fake, config); err == nil {
		t.Fatal("expected compatibility warning")
	}

	if err := os.WriteFile(config, []byte(`[plugins."io.containerd.cri.v1.images"]\n  use_local_image_pull = true\n`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckContainerdMirrorCompatibility(fake, config); err != nil {
		t.Fatalf("unexpected warning: %v", err)
	}
}
