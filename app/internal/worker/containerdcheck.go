package worker

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

var containerdVersionRE = regexp.MustCompile(`(?:version\s+|v)?(\d+)\.(\d+)\.(\d+)`)

// CheckContainerdMirrorCompatibility returns an advisory error only when the
// node appears to run containerd 2.1+ without explicitly enabling CRI local
// image pulls. Starting in 2.1, CRI may use Transfer Service by default; that
// path does not honor the full hosts.toml mirror behavior we rely on here.
// Kubespray should set use_local_image_pull=true in its containerd template.
func CheckContainerdMirrorCompatibility(containerdPath, configPath string) error {
	if strings.TrimSpace(containerdPath) == "" {
		return nil
	}
	versionOut, err := exec.Command(containerdPath, "--version").Output()
	if err != nil {
		return nil // advisory check; don't make the worker depend on the CLI being callable
	}
	match := containerdVersionRE.FindStringSubmatch(string(versionOut))
	if len(match) != 4 {
		return nil
	}
	major, _ := strconv.Atoi(match[1])
	minor, _ := strconv.Atoi(match[2])
	if major < 2 || (major == 2 && minor < 1) {
		return nil
	}

	data, err := osReadFile(configPath)
	if err != nil {
		return nil
	}
	if strings.Contains(string(data), "use_local_image_pull = true") || strings.Contains(string(data), "use_local_image_pull=true") {
		return nil
	}
	return fmt.Errorf("containerd %s detected without use_local_image_pull=true in %s; CRI Transfer Service may bypass AngryDuck hosts.toml mirror", strings.TrimSpace(string(versionOut)), configPath)
}

// osReadFile is a tiny seam for tests without making the runtime checker carry
// state or a mock filesystem abstraction.
var osReadFile = func(path string) ([]byte, error) {
	return os.ReadFile(path)
}
