package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/registryauth"
)

// Runtime is the minimal container-runtime interface Angry Duck needs on
// each node: pull an image, list images present locally, list images
// currently backing a running container, resolve any reference to its
// canonical content digest, and remove an unused image.
//
// It is implemented by shelling out to the node's container tooling rather
// than linking against containerd/CRI client libraries directly, which
// keeps the worker binary small and lets operators pick whatever CLI is
// already present on their nodes (containerd's `ctr`, `crictl`, or plain
// `docker`) via CONTAINER_RUNTIME in the .env file.
//
// Every backend below parses its tool's output with encoding/json (or, for
// ctr's tabular `images list`, whitespace-delimited fields — see
// parseCtrImagesList) rather than hand-rolled substring scraping. An
// earlier version of this file used substring markers like `"Image":"` (no
// space) against ctr's and crictl's pretty-printed JSON, which actually
// renders as `"Image": "value"` (space after the colon) — meaning those
// markers never matched anything, ever. Proper parsing is not a style
// preference; it was a real, confirmed, reproducible production bug that
// deleted a worker's own running image.
//
// A second, separate bug survived that fix: containerd gives one piece of
// image content multiple valid reference aliases — a tag
// (`repo/name:tag`), a digest-pinned form (`repo/name@sha256:...`), and a
// bare digest (`sha256:...`, the "image ID"). A running container's
// reported Image field is only ever one of these aliases as a raw string,
// so comparing local images against running images by raw string equality
// correctly spares whichever single alias matches, but misjudges the
// image's OTHER aliases as separate, unused images — even though they're
// the exact same content backing that same running container. This was
// also confirmed in production: GC reported "6 local images, 12 running
// images" but only 2 spared as running, because only the tag-form alias of
// each in-use image matched; its @digest and bare-digest aliases did not.
// LocalImages() exists specifically to close this: it resolves every known
// alias to its canonical content digest, so GC can compare by digest —
// which is identical across all aliases of the same content — instead of
// by raw reference string.
type Runtime interface {
	PullImage(image string) error
	// LocalImages returns every local image reference (in any alias form)
	// AND a map of every one of those references to its canonical content
	// digest, from a single underlying runtime query. This used to be two
	// separate methods (ListLocalImages + ImageDigests) that each shelled
	// out and parsed the exact same command's output independently, every
	// single GC tick — on crictl that meant `crictl images -o json` ran
	// and was JSON-unmarshaled twice per tick for identical output.
	// Combined into one call/parse that serves both needs.
	LocalImages() (refs []string, digests map[string]string, err error)
	ListRunningImages() ([]string, error)
	RemoveImage(image string) error
}

// NewRuntime builds a Runtime based on the given kind: "containerd" (uses
// `ctr -n k8s.io`), "crictl" (default — see below), or "docker". creds
// resolves per-registry pull credentials — pass registryauth.Empty() if
// none are configured; every pull then proceeds anonymously, same as
// before this existed. endpoint is the CRI runtime socket, only used by
// the crictl backend (e.g. "unix:///run/containerd/containerd.sock",
// matching the socket every deploy manifest already mounts).
//
// crictl is the recommended default: ListRunningImages() for containerd
// (`ctr`) lists container IDs and then shells out to `ctr containers info
// <id>` once PER container — on a busy node this can mean well over a
// hundred subprocess spawns every single GC tick, which is enough to push
// a 200m-limit worker pod's CPU usage 2-3x over its own limit in
// production. crictl exposes the same information via one `crictl ps -o
// json` call for the whole node. Both `ctr` and `crictl` ship as fully
// static Go binaries (verified: neither is a dynamic executable), so this
// choice has no effect on the base image — distroless/static works for
// either.
func NewRuntime(kind string, creds *registryauth.Store, endpoint, ctrPath, crictlPath string) Runtime {
	switch strings.ToLower(kind) {
	case "docker":
		return dockerRuntime{creds: creds}
	case "containerd":
		logging.Warnf("angryduck-worker: CONTAINER_RUNTIME=containerd shells out once PER running container on the node every GC tick (ctr has no bulk-list-with-image equivalent to crictl's `ps -o json`) — this is known to spike CPU well past the container's own limit on busy nodes; prefer crictl unless you have a specific reason not to")
		return containerdRuntime{creds: creds, path: ctrPath}
	default:
		if kind != "" && strings.ToLower(kind) != "crictl" {
			logging.Warnf("angryduck-worker: unrecognized CONTAINER_RUNTIME=%q, defaulting to crictl", kind)
		}
		return crictlRuntime{creds: creds, endpoint: endpoint, path: crictlPath}
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

type containerdRuntime struct {
	creds *registryauth.Store
	path  string
}

func (r containerdRuntime) PullImage(image string) error {
	args := []string{"-n", "k8s.io", "images", "pull"}
	if userpass, ok := r.creds.CredentialsFor(imageref.Host(image)); ok {
		args = append(args, "--user", userpass)
	}
	args = append(args, image)
	_, err := runCmd(r.path, args...)
	return err
}

// LocalImages fetches `ctr images list` (the full table, not the `-q`
// ref-only form) exactly once, and derives both the ref list and the
// digest map from that single parse — the ref list used to come from a
// separate `-q` invocation, doubling the subprocess spawns for no reason,
// since every ref `-q` would return is already a key in the table's parse.
func (r containerdRuntime) LocalImages() ([]string, map[string]string, error) {
	out, err := runCmd(r.path, "-n", "k8s.io", "images", "list")
	if err != nil {
		return nil, nil, err
	}
	digests := parseCtrImagesList(out)
	refs := make([]string, 0, len(digests))
	for ref := range digests {
		refs = append(refs, ref)
	}
	return refs, digests, nil
}

// parseCtrImagesList extracts a ref->digest map from the tabular output of
// `ctr images list` (no -q — that flag prints only refs, dropping the
// digest column this function needs). The table looks like:
//
//	REF                              TYPE                                     DIGEST                                                                  SIZE      PLATFORMS    LABELS
//	repo/name:tag                    application/vnd.oci.image.index.v1+json sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 14.7 MiB  linux/amd64  io.cri-containerd.image=managed
//	repo/name@sha256:0ea5747ba9...   application/vnd.oci.image.index.v1+json sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 14.7 MiB  linux/amd64  io.cri-containerd.image=managed
//	sha256:87091cd49a20acee0972...   application/vnd.oci.image.index.v1+json sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 14.7 MiB  linux/amd64  io.cri-containerd.image=managed
//
// containerd lists the SAME content under multiple REF aliases (a tag, a
// digest-pinned ref, and a bare digest "image ID") — note all three rows
// above share one DIGEST. Splitting each line on whitespace runs correctly
// isolates the first three fields (REF, TYPE, DIGEST) even though a later
// column (SIZE, e.g. "14.7 MiB") itself contains an internal space, because
// none of REF, TYPE, or DIGEST can ever contain whitespace themselves —
// only columns after the one we need can, so they don't affect parsing.
func parseCtrImagesList(output string) map[string]string {
	digests := make(map[string]string)
	for _, line := range splitNonEmptyLines(output) {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		ref, typ, digest := fields[0], fields[1], fields[2]
		if ref == "REF" && typ == "TYPE" && digest == "DIGEST" {
			continue // header row
		}
		if !strings.HasPrefix(digest, "sha256:") {
			continue // defensive: not a real digest, skip rather than guess
		}
		digests[ref] = digest
	}
	return digests
}

// ctrContainerInfo covers only the field we need from `ctr containers info`
// output. It's intentionally minimal — extra fields in the real JSON are
// ignored by encoding/json without any special handling.
type ctrContainerInfo struct {
	Image string `json:"Image"`
}

func (r containerdRuntime) ListRunningImages() ([]string, error) {
	out, err := runCmd(r.path, "-n", "k8s.io", "containers", "list", "-q")
	if err != nil {
		return nil, err
	}
	ids := splitNonEmptyLines(out)
	var images []string
	for _, id := range ids {
		info, err := runCmd(r.path, "-n", "k8s.io", "containers", "info", id)
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

func (r containerdRuntime) RemoveImage(image string) error {
	_, err := runCmd(r.path, "-n", "k8s.io", "images", "remove", image)
	return err
}

// ExportImage streams a complete containerd image archive without buffering
// it in AngryDuck memory. It is intentionally containerd-specific because
// crictl has no image archive export/import command.
func (r containerdRuntime) ExportImage(dst io.Writer, image string) error {
	cmd := exec.Command(r.path, "-n", "k8s.io", "images", "export", "-", image)
	cmd.Stdout = dst
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s -n k8s.io images export - %s: %w: %s", r.path, image, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// ImportImage streams a complete containerd image archive directly into
// containerd; no temporary file or in-memory tar buffer is required.
func (r containerdRuntime) ImportImage(src io.Reader) error {
	cmd := exec.Command(r.path, "-n", "k8s.io", "images", "import", "-")
	cmd.Stdin = src
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s -n k8s.io images import -: %w: %s", r.path, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// --- crictl ---

type crictlRuntime struct {
	creds    *registryauth.Store
	endpoint string
	path     string
}

// withEndpoint prepends `-r <endpoint>` when one is configured, so crictl
// talks to the exact socket this worker has mounted rather than relying on
// its own version-dependent default-search behavior (older crictl tries a
// short list of well-known paths; newer versions require this to be set
// explicitly via flag or /etc/crictl.yaml).
func (r crictlRuntime) withEndpoint(args ...string) []string {
	if r.endpoint == "" {
		return args
	}
	return append([]string{"-r", r.endpoint}, args...)
}

func (r crictlRuntime) PullImage(image string) error {
	args := []string{"pull"}
	if userpass, ok := r.creds.CredentialsFor(imageref.Host(image)); ok {
		args = append(args, "--creds", userpass)
	}
	args = append(args, image)
	_, err := runCmd(r.path, r.withEndpoint(args...)...)
	return err
}

// crictlImagesOutput matches the shape of `crictl images -o json`:
//
//	{"images": [{"id": "sha256:...", "repoTags": [...], "repoDigests": [...]}]}
type crictlImagesOutput struct {
	Images []struct {
		ID          string   `json:"id"`
		RepoTags    []string `json:"repoTags"`
		RepoDigests []string `json:"repoDigests"`
	} `json:"images"`
}

// LocalImages runs `crictl images -o json` exactly once per call and
// derives both the ref list and the digest map from that single parse.
// This used to be two separate methods that each ran this exact same
// command and re-unmarshaled the exact same output independently, every
// single GC tick — on a master node with a large image/container count,
// that's the biggest single allocation spike this worker makes, doubled
// for no reason. One call, one parse, both results.
//
// The digest map covers every known alias of every local image — its
// repoTags (tag-form), its repoDigests (digest-pinned form), and its bare
// id (the "image ID") — mapped to that image's id. CRI defines id as the
// canonical content identifier, so this plays the same role
// parseCtrImagesList's DIGEST column does for the containerd backend: a
// stable value shared by every alias of one piece of content, letting GC
// match by digest instead of by whichever specific alias a running
// container happens to report.
func (r crictlRuntime) LocalImages() ([]string, map[string]string, error) {
	out, err := runCmd(r.path, r.withEndpoint("images", "-o", "json")...)
	if err != nil {
		return nil, nil, err
	}
	var parsed crictlImagesOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil, nil, fmt.Errorf("parsing crictl images output: %w", err)
	}

	var refs []string
	digests := make(map[string]string)
	for _, img := range parsed.Images {
		refs = append(refs, img.RepoTags...)
		if img.ID == "" {
			continue
		}
		digests[img.ID] = img.ID
		for _, tag := range img.RepoTags {
			digests[tag] = img.ID
		}
		for _, d := range img.RepoDigests {
			digests[d] = img.ID
		}
	}
	return refs, digests, nil
}

// crictlPsOutput matches the shape of `crictl ps -o json`. Per the CRI
// ContainerStatus schema, containers[].image.image is the reference the
// container was created with (usually tag-form), while containers[].imageRef
// is that image's resolved digest. We return both — GC resolves whichever
// one actually matches through LocalImages(), so it doesn't matter which
// alias form ends up being the one that's directly comparable.
type crictlPsOutput struct {
	Containers []struct {
		Image struct {
			Image string `json:"image"`
		} `json:"image"`
		ImageRef string `json:"imageRef"`
	} `json:"containers"`
}

func (r crictlRuntime) ListRunningImages() ([]string, error) {
	out, err := runCmd(r.path, r.withEndpoint("ps", "-o", "json")...)
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
		if c.ImageRef != "" {
			images = append(images, c.ImageRef)
		}
	}
	return images, nil
}

func (r crictlRuntime) RemoveImage(image string) error {
	_, err := runCmd(r.path, r.withEndpoint("rmi", image)...)
	return err
}

// --- docker ---

type dockerRuntime struct {
	creds *registryauth.Store
}

// PullImage logs in to the target registry first if credentials are
// configured for it, then pulls. Unlike ctr/crictl, docker has no
// per-invocation credential flag on `pull` itself — `docker login` is the
// only mechanism, and it persists into the shared Docker credential store
// for the lifetime of the daemon, not just this one call. The password is
// piped via stdin rather than passed as a CLI argument, since arguments
// are visible to anything that can list processes on the node.
func (r dockerRuntime) PullImage(image string) error {
	if userpass, ok := r.creds.CredentialsFor(imageref.Host(image)); ok {
		user, pass, found := strings.Cut(userpass, ":")
		if found {
			cmd := exec.Command("docker", "login", imageref.Host(image), "-u", user, "--password-stdin")
			cmd.Stdin = strings.NewReader(pass)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("docker login to %s failed: %w: %s", imageref.Host(image), err, strings.TrimSpace(stderr.String()))
			}
		}
	}
	_, err := runCmd("docker", "pull", image)
	return err
}

func (dockerRuntime) ListRunningImages() ([]string, error) {
	out, err := runCmd("docker", "ps", "--format", "{{.Image}}")
	if err != nil {
		return nil, err
	}
	return splitNonEmptyLines(out), nil
}

// LocalImages runs `docker images` exactly once (asking for both the
// tag-form ref and the ID in one --format string) and derives both the ref
// list and the digest map from that single parse — the ref-only list used
// to come from a second, separately-formatted `docker images` invocation.
// Docker's naming is far less alias-prone than containerd's — one tag
// generally maps to one image ID with no separate @digest/bare-digest rows
// the way `ctr images list` produces — but mapping by ID still lets GC
// compare by digest for consistency, and protects against `docker ps
// --format {{.Image}}` occasionally reporting a container's original image
// by ID instead of tag (e.g. after the tag has been moved or removed since
// the container started).
func (dockerRuntime) LocalImages() ([]string, map[string]string, error) {
	out, err := runCmd("docker", "images", "--format", "{{.Repository}}:{{.Tag}} {{.ID}}")
	if err != nil {
		return nil, nil, err
	}
	var refs []string
	digests := make(map[string]string)
	for _, line := range splitNonEmptyLines(out) {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		ref, id := fields[0], fields[1]
		refs = append(refs, ref)
		digests[ref] = id
		digests[id] = id
	}
	return refs, digests, nil
}

func (dockerRuntime) RemoveImage(image string) error {
	_, err := runCmd("docker", "rmi", image)
	return err
}
