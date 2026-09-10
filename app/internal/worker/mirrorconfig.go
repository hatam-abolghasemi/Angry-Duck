package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const angryDuckHostsMarker = "# managed-by: angryduck"

// EnsureContainerdMirrorConfig creates the _default hosts.toml used by
// containerd's CRI registry resolver. It is deliberately idempotent and only
// overwrites a file that AngryDuck previously created; an existing operator-
// managed _default file is never silently clobbered.
func EnsureContainerdMirrorConfig(certsDir, nodeIP, listenAddr string, enabled bool) error {
	if !enabled {
		return nil
	}
	if strings.TrimSpace(certsDir) == "" {
		return fmt.Errorf("containerd certs.d path is empty")
	}
	if strings.TrimSpace(nodeIP) == "" {
		return fmt.Errorf("node IP is empty")
	}
	port := registryListenPort(listenAddr)
	file := filepath.Join(certsDir, "_default", "hosts.toml")
	if existing, err := os.ReadFile(file); err == nil {
		if !strings.Contains(string(existing), angryDuckHostsMarker) {
			return fmt.Errorf("refusing to overwrite existing containerd mirror config %s that is not managed by AngryDuck", file)
		}
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		return fmt.Errorf("create containerd mirror config directory: %w", err)
	}
	content := fmt.Sprintf(`%s

# containerd automatically falls back to the original registry after this host fails.
[host."http://%s:%s"]
  capabilities = ["pull", "resolve"]
  dial_timeout = "200ms"
`, angryDuckHostsMarker, nodeIP, port)
	if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write containerd mirror config %s: %w", file, err)
	}
	return nil
}

func registryListenPort(listenAddr string) string {
	idx := strings.LastIndex(listenAddr, ":")
	if idx < 0 || idx == len(listenAddr)-1 {
		return "5000"
	}
	port := listenAddr[idx+1:]
	if port == "" {
		return "5000"
	}
	return port
}
