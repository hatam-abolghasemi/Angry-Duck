package worker

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
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
	//
	// sizes maps every alias in refs/digests to that image's approximate
	// or exact size in bytes, feeding GC's freed-bytes metric. It comes
	// from whatever the same underlying listing call already reports —
	// crictl's JSON gives an exact byte count for free; ctr and docker
	// only give a human-rounded string ("14.7 MiB", "5.58MB") in their
	// bulk listing, so on those backends the value here is a best-effort
	// reconstruction of that rounded string, not the original exact size.
	// An alias missing from sizes means the backend reported nothing
	// parseable for it — callers should treat that as "unknown", not
	// zero.
	LocalImages() (refs []string, digests map[string]string, sizes map[string]int64, err error)
	ListRunningImages() ([]string, error)
	// RunningImageRepos counts currently-running containers on this node,
	// grouped by bare repository identity (imageref.Repo) — one count per
	// actual container. This is deliberately NOT built on top of
	// ListRunningImages(): that method intentionally returns every alias
	// of a running image (tag form AND digest form) for GC's set-membership
	// matching, and naively counting entries from it would double-count
	// every container on the crictl backend, which reports both aliases
	// per container. Used only by PreheatMonitor's coarse sample.
	RunningImageRepos() (map[string]int, error)
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
		logging.Warnf("angryduck-worker: CONTAINER_RUNTIME=containerd shells out once PER running container on the node every GC tick (ctr has no bulk-list-with-image equivalent to crictl's `ps -o json`) — this is known to spike CPU well past the container's own limit on busy nodes; prefer crictl unless you have a specific reason not to")
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

func (r containerdRuntime) PullImage(image string) error {
	args := []string{"-n", "k8s.io", "images", "pull"}
	if userpass, ok := r.creds.CredentialsFor(imageref.Host(image)); ok {
		args = append(args, "--user", userpass)
	}
	args = append(args, image)
	_, err := r.hx.Run("ctr", args...)
	return err
}

// LocalImages fetches `ctr images list` (the full table, not the `-q`
// ref-only form) exactly once, and derives both the ref list and the
// digest map from that single parse — the ref list used to come from a
// separate `-q` invocation, doubling the subprocess spawns for no reason,
// since every ref `-q` would return is already a key in the table's parse.
func (r containerdRuntime) LocalImages() ([]string, map[string]string, map[string]int64, error) {
	out, err := r.hx.Run("ctr", "-n", "k8s.io", "images", "list")
	if err != nil {
		return nil, nil, nil, err
	}
	digests, sizes := parseCtrImagesList(out)
	refs := make([]string, 0, len(digests))
	for ref := range digests {
		refs = append(refs, ref)
	}
	return refs, digests, sizes, nil
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
//
// The SIZE column (fields[3]+" "+fields[4], e.g. "14.7 MiB") is also
// parsed here, into an approximate byte count for GC's freed-bytes
// metric — approximate because ctr has already rounded it to one decimal
// place by the time it reaches us; see parseApproxBytes. A ref whose size
// couldn't be parsed (malformed line, missing column) simply has no entry
// in sizes.
func parseCtrImagesList(output string) (map[string]string, map[string]int64) {
	digests := make(map[string]string)
	sizes := make(map[string]int64)
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
		if len(fields) >= 5 {
			if b, ok := parseApproxBytes(fields[3]+" "+fields[4], binarySizeUnits, binarySizeBase); ok {
				sizes[ref] = b
			}
		}
	}
	return digests, sizes
}

// binarySizeUnits are the IEC (binary) units ctr prints in its SIZE column
// ("14.7 MiB", "312.9 KiB"), ordered smallest-to-largest so
// parseApproxBytes can walk them from the most specific suffix down to the
// bare "B" every one of them also ends with.
var binarySizeUnits = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}

const binarySizeBase = 1024.0

// decimalSizeUnits are the SI (decimal) units `docker images` prints in
// its SIZE column ("5.58MB", "312.9kB"), same ordering convention as
// binarySizeUnits.
var decimalSizeUnits = []string{"B", "kB", "MB", "GB", "TB", "PB"}

const decimalSizeBase = 1000.0

// parseApproxBytes reconstructs a byte count from a humanized size string
// like "14.7 MiB" or "5.58MB". It only exists because ctr's and docker's
// bulk image-listing commands round the real byte count to a
// human-friendly string before Angry Duck ever sees it — by the time this
// function runs, the original exact value is already gone, so the result
// is a best-effort approximation, not the precision crictl's own "size"
// field gives for free (see crictlRuntime.LocalImages). Good enough for a
// "how much disk did GC free" metric; not something to alert on down to
// the byte. units must be ordered smallest-to-largest (as
// binarySizeUnits/decimalSizeUnits are) and checked in reverse so e.g.
// "MiB" is matched before the trailing "B" every unit in the list shares.
func parseApproxBytes(s string, units []string, base float64) (int64, bool) {
	s = strings.TrimSpace(s)
	for exp := len(units) - 1; exp >= 0; exp-- {
		unit := units[exp]
		if !strings.HasSuffix(s, unit) {
			continue
		}
		numPart := strings.TrimSpace(strings.TrimSuffix(s, unit))
		val, err := strconv.ParseFloat(numPart, 64)
		if err != nil {
			continue
		}
		return int64(val * math.Pow(base, float64(exp))), true
	}
	return 0, false
}

// ctrContainerInfo covers only the field we need from `ctr containers info`
// output. It's intentionally minimal — extra fields in the real JSON are
// ignored by encoding/json without any special handling.
type ctrContainerInfo struct {
	Image string `json:"Image"`
}

func (r containerdRuntime) ListRunningImages() ([]string, error) {
	out, err := r.hx.Run("ctr", "-n", "k8s.io", "containers", "list", "-q")
	if err != nil {
		return nil, err
	}
	ids := splitNonEmptyLines(out)
	var images []string
	for _, id := range ids {
		info, err := r.hx.Run("ctr", "-n", "k8s.io", "containers", "info", id)
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
	out, err := r.hx.Run("ctr", "-n", "k8s.io", "containers", "list", "-q")
	if err != nil {
		return nil, err
	}
	ids := splitNonEmptyLines(out)
	counts := make(map[string]int)
	for _, id := range ids {
		info, err := r.hx.Run("ctr", "-n", "k8s.io", "containers", "info", id)
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

func (r containerdRuntime) RemoveImage(image string) error {
	_, err := r.hx.Run("ctr", "-n", "k8s.io", "images", "remove", image)
	return err
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

func (r crictlRuntime) PullImage(image string) error {
	args := []string{"pull"}
	if userpass, ok := r.creds.CredentialsFor(imageref.Host(image)); ok {
		args = append(args, "--creds", userpass)
	}
	args = append(args, image)
	_, err := r.hx.Run("crictl", r.withEndpoint(args...)...)
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
		// Size is a uint64 byte count, but CRI encodes it as a JSON
		// string (large enough to overflow a JSON number in some
		// encoders) — unlike ctr/docker, crictl gives this to us exact,
		// with no "14.7 MiB" rounding in between.
		Size string `json:"size"`
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
func (r crictlRuntime) LocalImages() ([]string, map[string]string, map[string]int64, error) {
	out, err := r.hx.Run("crictl", r.withEndpoint("images", "-o", "json")...)
	if err != nil {
		return nil, nil, nil, err
	}
	var parsed crictlImagesOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil, nil, nil, fmt.Errorf("parsing crictl images output: %w", err)
	}

	var refs []string
	digests := make(map[string]string)
	sizes := make(map[string]int64)
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
		if b, err := strconv.ParseInt(img.Size, 10, 64); err == nil && b > 0 {
			sizes[img.ID] = b
			for _, tag := range img.RepoTags {
				sizes[tag] = b
			}
			for _, d := range img.RepoDigests {
				sizes[d] = b
			}
		}
	}
	return refs, digests, sizes, nil
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
	out, err := r.hx.Run("crictl", r.withEndpoint("ps", "-o", "json")...)
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
// (ImageRef if the CRI runtime resolved one, else the requested
// Image.Image) — pulled out as its own function so the
// one-alias-per-container fix is directly unit-testable against a parsed
// fixture, the same way parseCtrImagesList is.
func countRunningReposByContainer(parsed crictlPsOutput) map[string]int {
	counts := make(map[string]int)
	for _, c := range parsed.Containers {
		ref := c.ImageRef
		if ref == "" {
			ref = c.Image.Image
		}
		if ref == "" {
			continue
		}
		if repo := imageref.Repo(ref); repo != "" {
			counts[repo]++
		}
	}
	return counts
}

// RunningImageRepos parses the same `crictl ps -o json` output
// ListRunningImages does, but — unlike that method — picks exactly ONE
// alias per container before computing its repo (see
// countRunningReposByContainer). ListRunningImages deliberately returns
// BOTH aliases per container for GC's set-membership matching, where the
// duplication is harmless; counting containers from that same list would
// double almost every count, since real containers almost always have
// both fields populated.
func (r crictlRuntime) RunningImageRepos() (map[string]int, error) {
	out, err := r.hx.Run("crictl", r.withEndpoint("ps", "-o", "json")...)
	if err != nil {
		return nil, err
	}
	var parsed crictlPsOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil, fmt.Errorf("parsing crictl ps output: %w", err)
	}
	return countRunningReposByContainer(parsed), nil
}

func (r crictlRuntime) RemoveImage(image string) error {
	_, err := r.hx.Run("crictl", r.withEndpoint("rmi", image)...)
	return err
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
func (r dockerRuntime) PullImage(image string) error {
	if userpass, ok := r.creds.CredentialsFor(imageref.Host(image)); ok {
		user, pass, found := strings.Cut(userpass, ":")
		if found {
			if _, err := r.hx.RunInput([]byte(pass), "docker", "login", imageref.Host(image), "-u", user, "--password-stdin"); err != nil {
				return fmt.Errorf("docker login to %s failed: %w", imageref.Host(image), err)
			}
		}
	}
	_, err := r.hx.Run("docker", "pull", image)
	return err
}

func (r dockerRuntime) ListRunningImages() ([]string, error) {
	out, err := r.hx.Run("docker", "ps", "--format", "{{.Image}}")
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
	out, err := r.hx.Run("docker", "ps", "--format", "{{.Image}}")
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
func (r dockerRuntime) LocalImages() ([]string, map[string]string, map[string]int64, error) {
	out, err := r.hx.Run("docker", "images", "--format", "{{.Repository}}:{{.Tag}} {{.ID}} {{.Size}}")
	if err != nil {
		return nil, nil, nil, err
	}
	var refs []string
	digests := make(map[string]string)
	sizes := make(map[string]int64)
	for _, line := range splitNonEmptyLines(out) {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		ref, id, sizeStr := fields[0], fields[1], fields[2]
		refs = append(refs, ref)
		digests[ref] = id
		digests[id] = id
		// Docker's Size field ("5.58MB") is already rounded by the time
		// it reaches us, same caveat as ctr — see parseApproxBytes.
		if b, ok := parseApproxBytes(sizeStr, decimalSizeUnits, decimalSizeBase); ok {
			sizes[ref] = b
			sizes[id] = b
		}
	}
	return refs, digests, sizes, nil
}

func (r dockerRuntime) RemoveImage(image string) error {
	_, err := r.hx.Run("docker", "rmi", image)
	return err
}
