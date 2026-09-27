package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/registryauth"
)

// Runtime is the minimal container-runtime interface Angry Duck needs on
// each node: pull an image, list images present locally, and list images
// currently backing a running container.
//
// It is implemented by shelling out to the node's container tooling rather
// than linking against containerd/CRI client libraries directly, which
// keeps the worker binary small and lets operators pick whatever CLI is
// already present on their nodes (containerd's `ctr`, `crictl`, or plain
// `docker`) via CONTAINER_RUNTIME in the .env file.
//
// Every backend below parses its tool's output with encoding/json (or, for
// ctr's tabular `images list`, whitespace-delimited fields) rather than
// hand-rolled substring scraping. An earlier version of this file used
// substring markers like `"Image":"` (no space) against ctr's and crictl's
// pretty-printed JSON, which actually renders as `"Image": "value"` (space
// after the colon) — meaning those markers never matched anything, ever.
// Proper parsing is not a style preference; it was a real, confirmed,
// reproducible production bug that deleted a worker's own running image,
// back when this worker also garbage-collected images. It does again (see
// ImageGC), and never touches an image a running container uses, so the
// same correct-parsing discipline matters as much as ever.
type Runtime interface {
	// PullImage pulls image; cancelling ctx kills the pull.
	PullImage(ctx context.Context, image string) error
	// LocalImages returns every local image reference (in any alias form
	// the runtime reports — a tag, a digest-pinned form, a bare digest)
	// present on this node. Used only for what the reporter sends the
	// controller (see Reporter.localInventory): the repo-locality signal
	// and the rescue source lookup. Nothing in this worker compares these
	// against running containers or deletes any of them.
	LocalImages() (refs []string, err error)
	ListRunningImages() ([]string, error)
	// RunningImageRepos counts currently-running containers on this node,
	// grouped by bare repository identity (imageref.Repo) — one count per
	// actual container. This is deliberately NOT built on top of
	// ListRunningImages(): that method intentionally returns every alias
	// of a running image (tag form AND digest form), and naively counting
	// entries from it would double-count every container on the crictl
	// backend, which reports both aliases per container. Used only by
	// PreheatMonitor's coarse sample.
	RunningImageRepos() (map[string]int, error)
}

// NewRuntime builds a Runtime based on the given kind: "containerd" (uses
// `ctr -n k8s.io`), "crictl" (default — see below), or "docker". creds
// resolves per-registry pull credentials — pass registryauth.Empty() if
// none are configured; every pull then proceeds anonymously, same as
// before this existed. endpoint is the CRI runtime socket, only used by
// the crictl backend (e.g. "unix:///run/containerd/containerd.sock",
// matching the socket every deploy manifest already mounts).
//
// crictl is the recommended default: ListRunningImages()/RunningImageRepos()
// for containerd (`ctr`) list container IDs and then shell out to `ctr
// containers info <id>` once PER container — on a busy node this can mean
// well over a hundred subprocess spawns on every call (PreheatMonitor's
// tick, or the image cleanup's sample), which is enough to
// push a 200m-limit worker pod's CPU usage 2-3x over its own limit in
// production. crictl exposes the same information via one `crictl ps -o
// json` call for the whole node.
//
// None of these binaries ship in the worker image. hx runs the node's own
// copies (see HostExec), so whatever version apt or Kubespray installed on
// the node is the version that runs — static or dynamically linked alike.
func NewRuntime(kind string, creds *registryauth.Store, endpoint string, hx *HostExec) Runtime {
	switch strings.ToLower(kind) {
	case "docker":
		return dockerRuntime{creds: creds, hx: hx}
	case "containerd":
		logging.Warnf("angryduck-worker: CONTAINER_RUNTIME=containerd shells out once PER running container on the node on every call to ListRunningImages/RunningImageRepos (ctr has no bulk-list-with-image equivalent to crictl's `ps -o json`) — this is known to spike CPU well past the container's own limit on busy nodes; prefer crictl unless you have a specific reason not to")
		return containerdRuntime{creds: creds, hx: hx}
	default:
		if kind != "" && strings.ToLower(kind) != "crictl" {
			logging.Warnf("angryduck-worker: unrecognized CONTAINER_RUNTIME=%q, defaulting to crictl", kind)
		}
		return crictlRuntime{creds: creds, endpoint: endpoint, hx: hx}
	}
}

// RuntimeBinary is the node CLI a given CONTAINER_RUNTIME needs, so main
// can fail fast at startup if the node doesn't have it.
func RuntimeBinary(kind string) string {
	switch strings.ToLower(kind) {
	case "docker":
		return "docker"
	case "containerd":
		return "ctr"
	default:
		return "crictl"
	}
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
	hx    *HostExec
}

func (r containerdRuntime) PullImage(ctx context.Context, image string) error {
	args := []string{"-n", "k8s.io", "images", "pull"}
	if userpass, ok := r.creds.CredentialsFor(imageref.Host(image)); ok {
		args = append(args, "--user", userpass)
	}
	args = append(args, image)
	_, err := r.hx.RunContext(ctx, "ctr", args...)
	return err
}

// LocalImages fetches `ctr images list` (the full table, not the `-q`
// ref-only form — ctr's `-q` and non-`-q` output cover the exact same
// rows, so there's no separate cheaper call to prefer here) and extracts
// just the REF column.
func (r containerdRuntime) LocalImages() ([]string, error) {
	out, err := r.hx.List("ctr", "-n", "k8s.io", "images", "list")
	if err != nil {
		return nil, err
	}
	return parseCtrImageRefs(out), nil
}

// parseCtrImageRefs extracts the REF column from the tabular output of
// `ctr images list`. The table looks like:
//
//	REF                              TYPE                                     DIGEST                                                                  SIZE      PLATFORMS    LABELS
//	repo/name:tag                    application/vnd.oci.image.index.v1+json sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 14.7 MiB  linux/amd64  io.cri-containerd.image=managed
//
// containerd lists the SAME content under multiple REF aliases at once (a
// tag, a digest-pinned ref, and a bare digest "image ID") — all returned
// here; the caller (Reporter.localInventory) already knows how to collapse
// aliases down to a bare repo identity and discard the ones with none.
// Splitting each line on whitespace and taking only fields[0] is safe even
// though later columns (SIZE, e.g. "14.7 MiB") contain an internal space,
// since REF itself can never contain whitespace.
func parseCtrImageRefs(output string) []string {
	seen := make(map[string]bool)
	var refs []string
	for _, line := range splitNonEmptyLines(output) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		ref := fields[0]
		if ref == "REF" {
			continue // header row
		}
		if seen[ref] {
			continue
		}
		seen[ref] = true
		refs = append(refs, ref)
	}
	return refs
}

// ctrContainerInfo covers only the field we need from `ctr containers info`
// output. It's intentionally minimal — extra fields in the real JSON are
// ignored by encoding/json without any special handling.
type ctrContainerInfo struct {
	Image string `json:"Image"`
}

func (r containerdRuntime) ListRunningImages() ([]string, error) {
	out, err := r.hx.List("ctr", "-n", "k8s.io", "containers", "list", "-q")
	if err != nil {
		return nil, err
	}
	ids := splitNonEmptyLines(out)
	var images []string
	for _, id := range ids {
		info, err := r.hx.List("ctr", "-n", "k8s.io", "containers", "info", id)
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

// RunningImageRepos shells out the same way ListRunningImages does (one
// `ctr containers info` per container — see NewRuntime's warning about
// this backend's cost), but groups by repo instead of returning raw
// aliases. Unlike crictl, ctr's `containers info` reports exactly one
// Image string per container already, so no alias-duplication concern
// applies here — this exists mainly for interface symmetry with the
// crictl backend, where it does matter.
func (r containerdRuntime) RunningImageRepos() (map[string]int, error) {
	out, err := r.hx.List("ctr", "-n", "k8s.io", "containers", "list", "-q")
	if err != nil {
		return nil, err
	}
	ids := splitNonEmptyLines(out)
	counts := make(map[string]int)
	for _, id := range ids {
		info, err := r.hx.List("ctr", "-n", "k8s.io", "containers", "info", id)
		if err != nil {
			continue
		}
		var parsed ctrContainerInfo
		if err := json.Unmarshal([]byte(info), &parsed); err != nil {
			continue
		}
		if parsed.Image == "" {
			continue
		}
		if repo := imageref.Repo(parsed.Image); repo != "" {
			counts[repo]++
		}
	}
	return counts, nil
}

// --- crictl ---

type crictlRuntime struct {
	creds    *registryauth.Store
	endpoint string
	hx       *HostExec
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

func (r crictlRuntime) PullImage(ctx context.Context, image string) error {
	args := []string{"pull"}
	if userpass, ok := r.creds.CredentialsFor(imageref.Host(image)); ok {
		args = append(args, "--creds", userpass)
	}
	args = append(args, image)
	_, err := r.hx.RunContext(ctx, "crictl", r.withEndpoint(args...)...)
	return err
}

// crictlImagesOutput matches the shape of `crictl images -o json`:
//
//	{"images": [{"repoTags": [...], "repoDigests": [...]}]}
type crictlImagesOutput struct {
	Images []struct {
		ID          string   `json:"id"`
		RepoTags    []string `json:"repoTags"`
		RepoDigests []string `json:"repoDigests"`
	} `json:"images"`
}

// LocalImages runs `crictl images -o json` and returns every image's
// tag-form and repo@digest references. The reporter's repo-locality signal
// skips the digest forms; the rescue source lookup needs them, for pods
// that pin an image by digest.
func (r crictlRuntime) LocalImages() ([]string, error) {
	out, err := r.hx.List("crictl", r.withEndpoint("images", "-o", "json")...)
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
		refs = append(refs, img.RepoDigests...)
	}
	return refs, nil
}

// crictlPsOutput matches the shape of `crictl ps -o json`. Per the CRI
// schema, containers[].image.image is the image the container was created
// with and containers[].imageRef is its resolved form. In practice kubelet
// creates every container from the resolved image ID, so on real nodes both
// fields are usually a bare "sha256:..." ID; the name the pod asked for is
// only in image.userSpecifiedImage (newer runtimes) or has to be looked up
// in `crictl images` by ID (see imageReposByID). ListRunningImages returns
// both forms per container (see below) since callers only ever do a loose
// substring check against the result, not an exact-match lookup that would
// care which alias form it's comparing against.
type crictlPsOutput struct {
	Containers []struct {
		Image struct {
			Image              string `json:"image"`
			UserSpecifiedImage string `json:"userSpecifiedImage"`
		} `json:"image"`
		ImageRef string `json:"imageRef"`
	} `json:"containers"`
}

func (r crictlRuntime) ListRunningImages() ([]string, error) {
	out, err := r.hx.List("crictl", r.withEndpoint("ps", "-o", "json")...)
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

// countRunningReposByContainer groups crictl's per-container ps output by
// bare repository identity, picking exactly one alias per container
// (Image.Image -- the tag-form reference the container was created with
// -- else ImageRef as a fallback) — pulled out as its own function so the
// one-alias-per-container fix is directly unit-testable against a parsed
// fixture, the same way parseCtrImageRefs is.
//
// Image.Image, not ImageRef, has to come first: crictl's ImageRef is
// almost always a bare content digest ("sha256:...", no registry/repo
// segment at all), and imageref.Repo() correctly returns "" for that —
// there's no repo name in a bare digest to extract. Preferring ImageRef
// therefore produced an empty repo (and got silently skipped below) for
// essentially every real container; Image.Image is the form that
// actually carries repo identity.
func countRunningReposByContainer(parsed crictlPsOutput, repoByID map[string]string) map[string]int {
	counts := make(map[string]int)
	for _, c := range parsed.Containers {
		repo := ""
		for _, ref := range []string{c.Image.UserSpecifiedImage, c.Image.Image, c.ImageRef} {
			if repo = imageref.Repo(ref); repo != "" {
				break
			}
		}
		// Containers created by kubelet usually carry only the image ID in
		// both fields, which imageref.Repo rightly refuses to turn into a
		// repo. Resolve the ID through the node's image list instead.
		if repo == "" {
			if repo = repoByID[c.ImageRef]; repo == "" {
				repo = repoByID[c.Image.Image]
			}
		}
		if repo != "" {
			counts[repo]++
		}
	}
	return counts
}

// imageReposByID maps every local image ID (as `crictl images -o json`
// reports it, "sha256:...") to its repo, taken from the first tag or digest
// reference that yields one. Best effort: on any error it returns nil and
// callers simply skip containers they can't name.
func (r crictlRuntime) imageReposByID() map[string]string {
	out, err := r.hx.List("crictl", r.withEndpoint("images", "-o", "json")...)
	if err != nil {
		return nil
	}
	var parsed crictlImagesOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil
	}
	return reposByImageID(parsed)
}

// reposByImageID is the parsing half of imageReposByID, split out so it can
// be tested without a node.
func reposByImageID(parsed crictlImagesOutput) map[string]string {
	m := make(map[string]string, len(parsed.Images))
	for _, img := range parsed.Images {
		if img.ID == "" {
			continue
		}
		refs := append(append([]string{}, img.RepoTags...), img.RepoDigests...)
		for _, ref := range refs {
			if repo := imageref.Repo(ref); repo != "" {
				m[img.ID] = repo
				break
			}
		}
	}
	return m
}

// RunningImageRepos parses the same `crictl ps -o json` output
// ListRunningImages does, but — unlike that method — picks exactly ONE
// alias per container before computing its repo (see
// countRunningReposByContainer). ListRunningImages deliberately returns
// BOTH aliases per container (the image cleanup only needs a
// substring match, where the duplication is harmless); counting
// containers from that same list would double almost every count here,
// since real containers almost always have both fields populated.
func (r crictlRuntime) RunningImageRepos() (map[string]int, error) {
	out, err := r.hx.List("crictl", r.withEndpoint("ps", "-o", "json")...)
	if err != nil {
		return nil, err
	}
	var parsed crictlPsOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil, fmt.Errorf("parsing crictl ps output: %w", err)
	}
	return countRunningReposByContainer(parsed, r.imageReposByID()), nil
}

// --- docker ---

type dockerRuntime struct {
	creds *registryauth.Store
	hx    *HostExec
}

// PullImage logs in to the target registry first if credentials are
// configured for it, then pulls. Unlike ctr/crictl, docker has no
// per-invocation credential flag on `pull` itself — `docker login` is the
// only mechanism, and it persists into the shared Docker credential store
// for the lifetime of the daemon, not just this one call. The password is
// piped via stdin rather than passed as a CLI argument, since arguments
// are visible to anything that can list processes on the node.
func (r dockerRuntime) PullImage(ctx context.Context, image string) error {
	if userpass, ok := r.creds.CredentialsFor(imageref.Host(image)); ok {
		user, pass, found := strings.Cut(userpass, ":")
		if found {
			if _, err := r.hx.RunInput([]byte(pass), "docker", "login", imageref.Host(image), "-u", user, "--password-stdin"); err != nil {
				return fmt.Errorf("docker login to %s failed: %w", imageref.Host(image), err)
			}
		}
	}
	_, err := r.hx.RunContext(ctx, "docker", "pull", image)
	return err
}

func (r dockerRuntime) ListRunningImages() ([]string, error) {
	out, err := r.hx.List("docker", "ps", "--format", "{{.Image}}")
	if err != nil {
		return nil, err
	}
	return splitNonEmptyLines(out), nil
}

// RunningImageRepos is a second `docker ps` call rather than deriving from
// ListRunningImages, purely for symmetry with the other two backends —
// docker ps already reports exactly one line per container, so there's no
// alias-duplication bug to work around here.
func (r dockerRuntime) RunningImageRepos() (map[string]int, error) {
	out, err := r.hx.List("docker", "ps", "--format", "{{.Image}}")
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for _, ref := range splitNonEmptyLines(out) {
		if repo := imageref.Repo(ref); repo != "" {
			counts[repo]++
		}
	}
	return counts, nil
}

// LocalImages runs `docker images` asking only for the tag-form ref — the
// reporter's repo-locality signal needs nothing else.
func (r dockerRuntime) LocalImages() ([]string, error) {
	out, err := r.hx.List("docker", "images", "--format", "{{.Repository}}:{{.Tag}}")
	if err != nil {
		return nil, err
	}
	return splitNonEmptyLines(out), nil
}
