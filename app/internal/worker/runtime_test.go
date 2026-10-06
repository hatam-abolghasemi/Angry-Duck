package worker

import (
	"encoding/json"
	"testing"
)

// TestCrictlImagesParsing uses the exact output captured from a real
// `crictl images -o json` call, confirming the double-space-after-colon
// format (`"repoTags":  [`) parses correctly. This is the exact format that
// broke the old extractCrictlImageRefs substring scraper, which searched
// for the zero-space marker `"repoTags":[`.
func TestCrictlImagesParsing(t *testing.T) {
	realOutput := `{
  "images":  [
    {
      "id":  "sha256:babca8157e8e6997ce9caaf6ab35c580970c74799c936fd5ea9ae7ebea60d3a8",
      "repoTags":  [
        "test.local/angryduck-testimg:v1"
      ],
      "repoDigests":  [],
      "size":  "4730554",
      "username":  "",
      "pinned":  false
    },
    {
      "id":  "sha256:c519bc7cb345cb8cce24a729eb89770c678b8f59c42de199d0b6b3063897de76",
      "repoTags":  [
        "test.local/pause:v1"
      ],
      "repoDigests":  [],
      "size":  "877983",
      "username":  "",
      "pinned":  false
    }
  ]
}`

	var parsed crictlImagesOutput
	if err := json.Unmarshal([]byte(realOutput), &parsed); err != nil {
		t.Fatalf("failed to parse real crictl images output: %v", err)
	}
	if len(parsed.Images) != 2 {
		t.Fatalf("got %d images, want 2", len(parsed.Images))
	}
	if len(parsed.Images[0].RepoTags) != 1 || parsed.Images[0].RepoTags[0] != "test.local/angryduck-testimg:v1" {
		t.Fatalf("unexpected repoTags for image 0: %v", parsed.Images[0].RepoTags)
	}
	if len(parsed.Images[1].RepoTags) != 1 || parsed.Images[1].RepoTags[0] != "test.local/pause:v1" {
		t.Fatalf("unexpected repoTags for image 1: %v", parsed.Images[1].RepoTags)
	}
}

// TestCrictlPsParsing exercises the nested image.image structure documented
// by the CRI ContainerStatus schema (image is an object, not a bare
// string), which the old flat-string extractCrictlContainerImages scraper
// could not correctly navigate in the first place.
func TestCrictlPsParsing(t *testing.T) {
	realShapeOutput := `{
  "containers":  [
    {
      "id":  "abc123",
      "image":  {
        "image":  "docker.io/library/ubuntu:latest"
      },
      "imageRef":  "sha256:597ce1600cf4ac5f449b66e75e840657bb53864434d6bd82f00b172544c32ee2",
      "state":  "CONTAINER_RUNNING"
    }
  ]
}`

	var parsed crictlPsOutput
	if err := json.Unmarshal([]byte(realShapeOutput), &parsed); err != nil {
		t.Fatalf("failed to parse crictl ps output: %v", err)
	}
	if len(parsed.Containers) != 1 {
		t.Fatalf("got %d containers, want 1", len(parsed.Containers))
	}
	want := "docker.io/library/ubuntu:latest"
	if parsed.Containers[0].Image.Image != want {
		t.Fatalf("parsed image = %q, want %q", parsed.Containers[0].Image.Image, want)
	}
}

// TestCrictlPsParsingEmpty confirms an empty container list (a node with no
// containers matching this worker's runtime yet) doesn't error.
func TestCrictlPsParsingEmpty(t *testing.T) {
	out := `{"containers":  []}`
	var parsed crictlPsOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("failed to parse empty crictl ps output: %v", err)
	}
	if len(parsed.Containers) != 0 {
		t.Fatalf("expected 0 containers, got %d", len(parsed.Containers))
	}
}

// realCtrImagesListOutput is the exact output captured from a real node
// during the alias-mismatch incident (`ctr -n k8s.io images list`, no -q).
// Used verbatim as a golden fixture: the SIZE column ("14.7 MiB", "312.9
// KiB") deliberately contains an internal space, which is exactly the case
// parseCtrImageRefs must handle correctly since it only needs the first
// three whitespace-delimited fields (REF, TYPE, DIGEST).
const realCtrImagesListOutput = `REF                                                                                                                            TYPE                                                      DIGEST                                                                  SIZE      PLATFORMS                                                                    LABELS                                                          
registry.example.com/devops/generic/angry-duck-worker:1.0.2                                                                   application/vnd.oci.image.index.v1+json                   sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 14.7 MiB  linux/amd64                                                                  io.cri-containerd.image=managed                                 
registry.example.com/devops/generic/angry-duck-worker@sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 application/vnd.oci.image.index.v1+json                   sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 14.7 MiB  linux/amd64                                                                  io.cri-containerd.image=managed                                 
registry.example.com/pause:3.10.1                                                                                         application/vnd.docker.distribution.manifest.list.v2+json sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c 312.9 KiB linux/amd64,linux/arm/v7,linux/arm64,linux/ppc64le,linux/s390x,windows/amd64 io.cri-containerd.image=managed,io.cri-containerd.pinned=pinned 
registry.example.com/pause@sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c                        application/vnd.docker.distribution.manifest.list.v2+json sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c 312.9 KiB linux/amd64,linux/arm/v7,linux/arm64,linux/ppc64le,linux/s390x,windows/amd64 io.cri-containerd.image=managed,io.cri-containerd.pinned=pinned 
sha256:87091cd49a20acee097a2c96c7ed21c56fc0349a21e674a4197f20a396ef321e                                                        application/vnd.oci.image.index.v1+json                   sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 14.7 MiB  linux/amd64                                                                  io.cri-containerd.image=managed                                 
sha256:cd073f4c5f6a8e9dc6f3125ba00cf60819cae95c1ec84a1f146ee4a9cf9e803f                                                        application/vnd.docker.distribution.manifest.list.v2+json sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c 312.9 KiB linux/amd64,linux/arm/v7,linux/arm64,linux/ppc64le,linux/s390x,windows/amd64 io.cri-containerd.image=managed,io.cri-containerd.pinned=pinned 
`

// TestCountRunningReposByContainer proves the crictl backend's
// RunningImageRepos counts each CONTAINER once, not once per alias —
// ListRunningImages deliberately returns both Image.Image and ImageRef
// per container, and naively counting that list would double every
// result, since real containers almost always have both fields set (as
// this fixture, with two containers of the same repo plus one of
// another, does).
func TestCountRunningReposByContainer(t *testing.T) {
	realShapeOutput := `{
  "containers": [
    {
      "id": "abc123",
      "image": {"image": "docker.io/library/nginx:1.25"},
      "imageRef": "sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4",
      "state": "CONTAINER_RUNNING"
    },
    {
      "id": "def456",
      "image": {"image": "docker.io/library/nginx:1.25"},
      "imageRef": "sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4",
      "state": "CONTAINER_RUNNING"
    },
    {
      "id": "ghi789",
      "image": {"image": "registry.example.com/pause:3.10.1"},
      "imageRef": "sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c",
      "state": "CONTAINER_RUNNING"
    }
  ]
}`
	var parsed crictlPsOutput
	if err := json.Unmarshal([]byte(realShapeOutput), &parsed); err != nil {
		t.Fatalf("failed to parse crictl ps output: %v", err)
	}

	counts := countRunningReposByContainer(parsed, nil)

	if got := counts["docker.io/library/nginx"]; got != 2 {
		t.Errorf("nginx repo count = %d, want 2 (one per container, not one per alias)", got)
	}
	if got := counts["registry.example.com/pause"]; got != 1 {
		t.Errorf("pause repo count = %d, want 1", got)
	}
	if len(counts) != 2 {
		t.Errorf("got %d distinct repos, want 2", len(counts))
	}
}

// TestCountRunningReposByContainerMissingImageRef falls back to
// Image.Image when the CRI runtime hasn't resolved an ImageRef.
func TestCountRunningReposByContainerMissingImageRef(t *testing.T) {
	out := `{
  "containers": [
    {"id": "abc123", "image": {"image": "docker.io/library/redis:7"}, "imageRef": "", "state": "CONTAINER_RUNNING"}
  ]
}`
	var parsed crictlPsOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("failed to parse crictl ps output: %v", err)
	}
	counts := countRunningReposByContainer(parsed, nil)
	if got := counts["docker.io/library/redis"]; got != 1 {
		t.Errorf("redis repo count = %d, want 1", got)
	}
}

// TestCountRunningReposByContainerKubeletShape covers what kubelet-created
// containers actually look like: kubelet hands the runtime the resolved
// image ID, so image.image and imageRef are both bare "sha256:..." IDs.
// The repo must come from userSpecifiedImage when present, otherwise from
// the node's image list. Without this, every container was skipped and
// angryduck_worker_preheated_containers_running never got a series.
func TestCountRunningReposByContainerKubeletShape(t *testing.T) {
	const appID = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	const dbID = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	const orphanID = "sha256:3333333333333333333333333333333333333333333333333333333333333333"

	psOutput := `{
  "containers": [
    {"id": "c1", "image": {"image": "` + appID + `"}, "imageRef": "` + appID + `"},
    {"id": "c2", "image": {"image": "` + appID + `"}, "imageRef": "` + appID + `"},
    {"id": "c3", "image": {"image": "` + dbID + `", "userSpecifiedImage": "registry.example.com/team/db:15"}, "imageRef": "` + dbID + `"},
    {"id": "c4", "image": {"image": "` + orphanID + `"}, "imageRef": "` + orphanID + `"}
  ]
}`
	imagesOutput := `{
  "images": [
    {"id": "` + appID + `", "repoTags": ["registry.example.com/team/app:1.2.3"], "repoDigests": ["registry.example.com/team/app@sha256:aaaa"]},
    {"id": "` + dbID + `", "repoTags": [], "repoDigests": ["registry.example.com/team/db@sha256:bbbb"]}
  ]
}`

	var ps crictlPsOutput
	if err := json.Unmarshal([]byte(psOutput), &ps); err != nil {
		t.Fatalf("parsing crictl ps output: %v", err)
	}
	var images crictlImagesOutput
	if err := json.Unmarshal([]byte(imagesOutput), &images); err != nil {
		t.Fatalf("parsing crictl images output: %v", err)
	}

	counts := countRunningReposByContainer(ps, reposByImageID(images))

	if got := counts["registry.example.com/team/app"]; got != 2 {
		t.Errorf("app repo count = %d, want 2 (resolved from the image list by ID)", got)
	}
	if got := counts["registry.example.com/team/db"]; got != 1 {
		t.Errorf("db repo count = %d, want 1 (from userSpecifiedImage)", got)
	}
	if len(counts) != 2 {
		t.Errorf("got %d distinct repos %v, want 2 (an image missing from the list is skipped, not counted as \"\")", len(counts), counts)
	}

	// Without an image list, ID-only containers are skipped rather than
	// miscounted, and userSpecifiedImage still works.
	counts = countRunningReposByContainer(ps, nil)
	if len(counts) != 1 || counts["registry.example.com/team/db"] != 1 {
		t.Errorf("without an image list got %v, want only the db repo", counts)
	}
}
