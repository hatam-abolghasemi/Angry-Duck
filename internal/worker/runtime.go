package worker

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

// Runtime is the minimal container-runtime interface Angry Duck needs on
// each node: pull an image, list images present locally, list images
// currently backing a running container, and remove an unused image.
//
// It is implemented by shelling out to the node's container tooling rather
// than linking against containerd/CRI client libraries directly, which
// keeps the worker binary small and lets operators pick whatever CLI is
// already present on their nodes (containerd's `ctr`, `crictl`, or plain
// `docker`) via CONTAINER_RUNTIME in the .env file.
type Runtime interface {
	PullImage(image string) error
	ListLocalImages() ([]string, error)
	ListRunningImages() ([]string, error)
	RemoveImage(image string) error
}

// NewRuntime builds a Runtime based on the given kind: "containerd" (default,
// uses `ctr -n k8s.io`), "crictl", or "docker".
func NewRuntime(kind string) Runtime {
	switch strings.ToLower(kind) {
	case "docker":
		return dockerRuntime{}
	case "crictl":
		return crictlRuntime{}
	default:
		return containerdRuntime{}
	}
}

func runCmd(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// --- containerd (ctr) ---

type containerdRuntime struct{}

func (containerdRuntime) PullImage(image string) error {
	_, err := runCmd("ctr", "-n", "k8s.io", "images", "pull", image)
	return err
}

func (containerdRuntime) ListLocalImages() ([]string, error) {
	out, err := runCmd("ctr", "-n", "k8s.io", "images", "list", "-q")
	if err != nil {
		return nil, err
	}
	return splitNonEmptyLines(out), nil
}

func (containerdRuntime) ListRunningImages() ([]string, error) {
	// %s prints the Image column only, header included; we filter it out.
	out, err := runCmd("ctr", "-n", "k8s.io", "task", "ls")
	if err != nil {
		return nil, err
	}
	// `ctr task ls` doesn't print images directly; fall back to `ctr -n k8s.io c ls`.
	out, err = runCmd("ctr", "-n", "k8s.io", "containers", "list", "-q")
	if err != nil {
		return nil, err
	}
	ids := splitNonEmptyLines(out)
	var images []string
	for _, id := range ids {
		info, err := runCmd("ctr", "-n", "k8s.io", "containers", "info", id)
		if err != nil {
			continue
		}
		images = append(images, extractField(info, `"Image":"`))
	}
	return images, nil
}

func (containerdRuntime) RemoveImage(image string) error {
	_, err := runCmd("ctr", "-n", "k8s.io", "images", "remove", image)
	return err
}

// --- crictl ---

type crictlRuntime struct{}

func (crictlRuntime) PullImage(image string) error {
	_, err := runCmd("crictl", "pull", image)
	return err
}

func (crictlRuntime) ListLocalImages() ([]string, error) {
	out, err := runCmd("crictl", "images", "-o", "json")
	if err != nil {
		return nil, err
	}
	return extractCrictlImageRefs(out), nil
}

func (crictlRuntime) ListRunningImages() ([]string, error) {
	out, err := runCmd("crictl", "ps", "-o", "json")
	if err != nil {
		return nil, err
	}
	return extractCrictlContainerImages(out), nil
}

func (crictlRuntime) RemoveImage(image string) error {
	_, err := runCmd("crictl", "rmi", image)
	return err
}

// --- docker ---

type dockerRuntime struct{}

func (dockerRuntime) PullImage(image string) error {
	_, err := runCmd("docker", "pull", image)
	return err
}

func (dockerRuntime) ListLocalImages() ([]string, error) {
	out, err := runCmd("docker", "images", "--format", "{{.Repository}}:{{.Tag}}")
	if err != nil {
		return nil, err
	}
	return splitNonEmptyLines(out), nil
}

func (dockerRuntime) ListRunningImages() ([]string, error) {
	out, err := runCmd("docker", "ps", "--format", "{{.Image}}")
	if err != nil {
		return nil, err
	}
	return splitNonEmptyLines(out), nil
}

func (dockerRuntime) RemoveImage(image string) error {
	_, err := runCmd("docker", "rmi", image)
	return err
}

// --- helpers ---

func splitNonEmptyLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

// extractField does a crude single-field pull out of ctr's non-JSON `info`
// text dump; it's intentionally tolerant since we only need a best-effort
// image reference for GC comparisons, not a strict parser.
func extractField(text, marker string) string {
	idx := strings.Index(text, marker)
	if idx < 0 {
		return ""
	}
	rest := text[idx+len(marker):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// extractCrictlImageRefs pulls "repoTags":["..."] entries out of
// `crictl images -o json` output without a full JSON schema dependency.
func extractCrictlImageRefs(jsonOut string) []string {
	var out []string
	marker := `"repoTags":[`
	idx := 0
	for {
		pos := strings.Index(jsonOut[idx:], marker)
		if pos < 0 {
			break
		}
		start := idx + pos + len(marker)
		end := strings.Index(jsonOut[start:], "]")
		if end < 0 {
			break
		}
		segment := jsonOut[start : start+end]
		for _, tag := range strings.Split(segment, ",") {
			tag = strings.Trim(strings.TrimSpace(tag), `"`)
			if tag != "" {
				out = append(out, tag)
			}
		}
		idx = start + end
	}
	return out
}

// extractCrictlContainerImages pulls "image":{"image":"..."} refs out of
// `crictl ps -o json` output.
func extractCrictlContainerImages(jsonOut string) []string {
	var out []string
	marker := `"image":"`
	idx := 0
	for {
		pos := strings.Index(jsonOut[idx:], marker)
		if pos < 0 {
			break
		}
		start := idx + pos + len(marker)
		end := strings.Index(jsonOut[start:], `"`)
		if end < 0 {
			break
		}
		out = append(out, jsonOut[start:start+end])
		idx = start + end
	}
	return out
}
