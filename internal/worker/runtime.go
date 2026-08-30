package worker

import (
	"bytes"
	"encoding/json"
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
//
// Every backend below parses its tool's output with encoding/json rather
// than hand-rolled substring scraping. An earlier version of this file used
// substring markers like `"Image":"` (no space) against ctr's and crictl's
// pretty-printed JSON, which actually renders as `"Image": "value"` (space
// after the colon) — meaning those markers never matched anything, ever.
// ListRunningImages() silently returned nothing for every container, on
// every check, which made GC (before it was scoped to self-managed images
// only) treat every image on the node as permanently unused and eventually
// delete it — including images actively backing running containers. Proper
// JSON parsing is not a style preference here; it was a real, confirmed,
// reproducible production bug that deleted a worker's own running image.
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

// ctrContainerInfo covers only the field we need from `ctr containers info`
// output. It's intentionally minimal — extra fields in the real JSON are
// ignored by encoding/json without any special handling.
type ctrContainerInfo struct {
	Image string `json:"Image"`
}

func (containerdRuntime) ListRunningImages() ([]string, error) {
	out, err := runCmd("ctr", "-n", "k8s.io", "containers", "list", "-q")
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
		var parsed ctrContainerInfo
		if err := json.Unmarshal([]byte(info), &parsed); err != nil {
			continue
		}
		if parsed.Image != "" {
			images = append(images, parsed.Image)
		}
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

// crictlImagesOutput matches the shape of `crictl images -o json`:
//
//	{"images": [{"id": "sha256:...", "repoTags": ["name:tag", ...], ...}]}
type crictlImagesOutput struct {
	Images []struct {
		ID       string   `json:"id"`
		RepoTags []string `json:"repoTags"`
	} `json:"images"`
}

func (crictlRuntime) ListLocalImages() ([]string, error) {
	out, err := runCmd("crictl", "images", "-o", "json")
	if err != nil {
		return nil, err
	}
	var parsed crictlImagesOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil, fmt.Errorf("parsing crictl images output: %w", err)
	}
	var refs []string
	for _, img := range parsed.Images {
		refs = append(refs, img.RepoTags...)
	}
	return refs, nil
}

// crictlPsOutput matches the shape of `crictl ps -o json`. Per the CRI
// ContainerStatus schema, the requested image reference lives at
// containers[].image.image (a nested object, not a bare string) — imageRef
// is a separate, usually digest-form field we deliberately don't use here
// since ListLocalImages() reports tag-form refs and we need both sides of
// the comparison in the same form.
type crictlPsOutput struct {
	Containers []struct {
		Image struct {
			Image string `json:"image"`
		} `json:"image"`
	} `json:"containers"`
}

func (crictlRuntime) ListRunningImages() ([]string, error) {
	out, err := runCmd("crictl", "ps", "-o", "json")
	if err != nil {
		return nil, err
	}
	var parsed crictlPsOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil, fmt.Errorf("parsing crictl ps output: %w", err)
	}
	var images []string
	for _, c := range parsed.Containers {
		if c.Image.Image != "" {
			images = append(images, c.Image.Image)
		}
	}
	return images, nil
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
