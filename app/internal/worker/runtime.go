package worker

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"angryduck/internal/imageref"
	"angryduck/internal/registryauth"
)

// Runtime is the minimal container-runtime interface Angry Duck needs on
// each node: pull an image, list images present locally, and list images
// currently backing a running container.
//
// It is implemented over containerd's CRI API, on the same socket as the
// rest of the worker's containerd calls: the calls `crictl pull`, `crictl
// images` and `crictl ps` make, without running crictl. A seed pull is
// therefore exactly the pull kubelet would make, honoring the node's
// registry config (hosts.toml, mirrors, the Angry Duck mirror included).
//
// Responses are mapped onto the same shapes `crictl -o json` printed, so
// the parsing below (and its tests) is unchanged. Correct parsing matters
// here: an earlier substring-scraping version deleted a worker's own
// running image back when this worker garbage-collected images, and it
// does again (see ImageGC), never touching an image a running container
// uses.
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

// criRuntime is Runtime over CRI.
type criRuntime struct {
	creds  *registryauth.Store
	images runtimeapi.ImageServiceClient
	rt     runtimeapi.RuntimeServiceClient
}

// NewRuntime builds the CRI runtime on conn, containerd's gRPC connection.
// creds resolves per-registry pull credentials; pass registryauth.Empty()
// if none are configured, and every pull proceeds anonymously.
func NewRuntime(conn *grpc.ClientConn, creds *registryauth.Store) Runtime {
	return criRuntime{creds: creds, images: runtimeapi.NewImageServiceClient(conn), rt: runtimeapi.NewRuntimeServiceClient(conn)}
}

func (r criRuntime) PullImage(ctx context.Context, image string) error {
	req := &runtimeapi.PullImageRequest{Image: &runtimeapi.ImageSpec{Image: image}}
	if userpass, ok := r.creds.CredentialsFor(imageref.Host(image)); ok {
		user, pass, _ := strings.Cut(userpass, ":")
		req.Auth = &runtimeapi.AuthConfig{Username: user, Password: pass}
	}
	_, err := r.images.PullImage(ctx, req)
	return err
}

// crictlImagesOutput matches the shape of `crictl images -o json`:
//
//	{"images": [{"repoTags": [...], "repoDigests": [...]}]}
type crictlImagesOutput struct {
	Images []imageEntry `json:"images"`
}

type imageEntry struct {
	ID          string   `json:"id"`
	RepoTags    []string `json:"repoTags"`
	RepoDigests []string `json:"repoDigests"`
}

func (r criRuntime) listImages() (crictlImagesOutput, error) {
	ctx, cancel := context.WithTimeout(context.Background(), listingTimeout)
	defer cancel()
	release, err := acquireListing(ctx)
	if err != nil {
		return crictlImagesOutput{}, err
	}
	defer release()
	resp, err := r.images.ListImages(ctx, &runtimeapi.ListImagesRequest{})
	if err != nil {
		return crictlImagesOutput{}, fmt.Errorf("listing images: %w", err)
	}
	var out crictlImagesOutput
	for _, img := range resp.GetImages() {
		out.Images = append(out.Images, imageEntry{ID: img.GetId(), RepoTags: img.GetRepoTags(), RepoDigests: img.GetRepoDigests()})
	}
	return out, nil
}

// LocalImages returns every image's tag-form and repo@digest references.
// The reporter's repo-locality signal skips the digest forms; the rescue
// source lookup needs them, for pods that pin an image by digest.
func (r criRuntime) LocalImages() ([]string, error) {
	parsed, err := r.listImages()
	if err != nil {
		return nil, err
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
	Containers []psContainer `json:"containers"`
}

type psContainer struct {
	Image struct {
		Image              string `json:"image"`
		UserSpecifiedImage string `json:"userSpecifiedImage"`
	} `json:"image"`
	ImageRef string `json:"imageRef"`
}

// listRunning lists running containers, as `crictl ps` does.
func (r criRuntime) listRunning() (crictlPsOutput, error) {
	ctx, cancel := context.WithTimeout(context.Background(), listingTimeout)
	defer cancel()
	release, err := acquireListing(ctx)
	if err != nil {
		return crictlPsOutput{}, err
	}
	defer release()
	resp, err := r.rt.ListContainers(ctx, &runtimeapi.ListContainersRequest{Filter: &runtimeapi.ContainerFilter{
		State: &runtimeapi.ContainerStateValue{State: runtimeapi.ContainerState_CONTAINER_RUNNING},
	}})
	if err != nil {
		return crictlPsOutput{}, fmt.Errorf("listing containers: %w", err)
	}
	return toPsOutput(resp.GetContainers()), nil
}

func toPsOutput(cs []*runtimeapi.Container) crictlPsOutput {
	var out crictlPsOutput
	out.Containers = make([]psContainer, 0, len(cs))
	for _, c := range cs {
		var pc psContainer
		pc.Image.Image = c.GetImage().GetImage()
		pc.Image.UserSpecifiedImage = c.GetImage().GetUserSpecifiedImage()
		pc.ImageRef = c.GetImageRef()
		out.Containers = append(out.Containers, pc)
	}
	return out
}

// ListRunningImages returns both the image and imageRef of every running
// container (see crictlPsOutput); callers only do a loose match.
func (r criRuntime) ListRunningImages() ([]string, error) {
	parsed, err := r.listRunning()
	if err != nil {
		return nil, err
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

// RunningImageRepos counts running containers per repo, one alias per
// container (see countRunningReposByContainer).
func (r criRuntime) RunningImageRepos() (map[string]int, error) {
	parsed, err := r.listRunning()
	if err != nil {
		return nil, err
	}
	var repoByID map[string]string
	if imgs, err := r.listImages(); err == nil {
		repoByID = reposByImageID(imgs)
	}
	return countRunningReposByContainer(parsed, repoByID), nil
}
