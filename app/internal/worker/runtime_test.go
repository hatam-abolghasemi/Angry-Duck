package worker

import (
	"encoding/json"
	"testing"
)

// TestCtrContainerInfoParsing uses the exact output captured from a real
// `ctr -n k8s.io containers info <id>` call against a live containerd
// instance, confirming the pretty-printed "Image": "value" (space after the
// colon) format parses correctly. This is the exact format that broke the
// old substring-based extractField helper.
func TestCtrContainerInfoParsing(t *testing.T) {
	realOutput := `{
    "ID": "mytestcontainer",
    "Labels": {
        "io.containerd.image.config.stop-signal": "SIGTERM",
        "io.cri-containerd.image": "managed"
    },
    "Image": "test.local/angryduck-testimg:v1",
    "Runtime": {
        "Name": "io.containerd.runc.v2"
    },
    "SnapshotKey": "mytestcontainer",
    "Snapshotter": "overlayfs"
}`

	var parsed ctrContainerInfo
	if err := json.Unmarshal([]byte(realOutput), &parsed); err != nil {
		t.Fatalf("failed to parse real ctr containers info output: %v", err)
	}
	want := "test.local/angryduck-testimg:v1"
	if parsed.Image != want {
		t.Fatalf("parsed.Image = %q, want %q", parsed.Image, want)
	}
}

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
// parseCtrImagesList must handle correctly since it only needs the first
// three whitespace-delimited fields (REF, TYPE, DIGEST).
const realCtrImagesListOutput = `REF                                                                                                                            TYPE                                                      DIGEST                                                                  SIZE      PLATFORMS                                                                    LABELS                                                          
registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2                                                                   application/vnd.oci.image.index.v1+json                   sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 14.7 MiB  linux/amd64                                                                  io.cri-containerd.image=managed                                 
registry.internal-registry.example.com/devops/generic/angry-duck-worker@sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 application/vnd.oci.image.index.v1+json                   sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 14.7 MiB  linux/amd64                                                                  io.cri-containerd.image=managed                                 
repo-sahand.internal-dev.example.com/pause:3.10.1                                                                                         application/vnd.docker.distribution.manifest.list.v2+json sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c 312.9 KiB linux/amd64,linux/arm/v7,linux/arm64,linux/ppc64le,linux/s390x,windows/amd64 io.cri-containerd.image=managed,io.cri-containerd.pinned=pinned 
repo-sahand.internal-dev.example.com/pause@sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c                        application/vnd.docker.distribution.manifest.list.v2+json sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c 312.9 KiB linux/amd64,linux/arm/v7,linux/arm64,linux/ppc64le,linux/s390x,windows/amd64 io.cri-containerd.image=managed,io.cri-containerd.pinned=pinned 
sha256:87091cd49a20acee097a2c96c7ed21c56fc0349a21e674a4197f20a396ef321e                                                        application/vnd.oci.image.index.v1+json                   sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4 14.7 MiB  linux/amd64                                                                  io.cri-containerd.image=managed                                 
sha256:cd073f4c5f6a8e9dc6f3125ba00cf60819cae95c1ec84a1f146ee4a9cf9e803f                                                        application/vnd.docker.distribution.manifest.list.v2+json sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c 312.9 KiB linux/amd64,linux/arm/v7,linux/arm64,linux/ppc64le,linux/s390x,windows/amd64 io.cri-containerd.image=managed,io.cri-containerd.pinned=pinned 
`

func TestParseCtrImagesList(t *testing.T) {
	digests, sizes := parseCtrImagesList(realCtrImagesListOutput)

	const workerDigest = "sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4"
	const pauseDigest = "sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c"

	// 14.7 MiB and 312.9 KiB, reconstructed from the same rounded strings
	// ctr printed — see parseApproxBytes for why this is approximate.
	const wantWorkerBytes = int64(14.7 * 1024 * 1024)
	const wantPauseBytes = int64(312.9 * 1024)
	if got := sizes["registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2"]; got != wantWorkerBytes {
		t.Errorf("sizes[worker ref] = %d, want %d", got, wantWorkerBytes)
	}
	if got := sizes["repo-sahand.internal-dev.example.com/pause:3.10.1"]; got != wantPauseBytes {
		t.Errorf("sizes[pause ref] = %d, want %d", got, wantPauseBytes)
	}

	wantWorker := []string{
		"registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2",
		"registry.internal-registry.example.com/devops/generic/angry-duck-worker@sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4",
		"sha256:87091cd49a20acee097a2c96c7ed21c56fc0349a21e674a4197f20a396ef321e",
	}
	for _, ref := range wantWorker {
		if got := digests[ref]; got != workerDigest {
			t.Errorf("digests[%q] = %q, want %q", ref, got, workerDigest)
		}
	}

	wantPause := []string{
		"repo-sahand.internal-dev.example.com/pause:3.10.1",
		"repo-sahand.internal-dev.example.com/pause@sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c",
		"sha256:cd073f4c5f6a8e9dc6f3125ba00cf60819cae95c1ec84a1f146ee4a9cf9e803f",
	}
	for _, ref := range wantPause {
		if got := digests[ref]; got != pauseDigest {
			t.Errorf("digests[%q] = %q, want %q", ref, got, pauseDigest)
		}
	}

	if _, ok := digests["REF"]; ok {
		t.Errorf("header row was incorrectly parsed as a real entry")
	}

	if len(digests) != len(wantWorker)+len(wantPause) {
		t.Errorf("got %d entries, want %d", len(digests), len(wantWorker)+len(wantPause))
	}
}

func TestParseCtrImagesListEmpty(t *testing.T) {
	digests, sizes := parseCtrImagesList("")
	if len(digests) != 0 {
		t.Errorf("expected no entries from empty input, got %d", len(digests))
	}
	if len(sizes) != 0 {
		t.Errorf("expected no size entries from empty input, got %d", len(sizes))
	}
}

func TestParseCtrImagesListHeaderOnly(t *testing.T) {
	digests, sizes := parseCtrImagesList("REF   TYPE   DIGEST   SIZE   PLATFORMS   LABELS\n")
	if len(digests) != 0 {
		t.Errorf("expected no entries from header-only input, got %d", len(digests))
	}
	if len(sizes) != 0 {
		t.Errorf("expected no size entries from header-only input, got %d", len(sizes))
	}
}

// TestParseApproxBytes covers both unit systems parseApproxBytes has to
// handle: ctr's binary (IEC) units and docker's decimal (SI) units, plus
// the "B" ambiguity that motivates checking longest-suffix-first (every
// unit in both lists ends in "B").
func TestParseApproxBytes(t *testing.T) {
	cases := []struct {
		s     string
		units []string
		base  float64
		want  int64
		ok    bool
	}{
		{"14.7 MiB", binarySizeUnits, binarySizeBase, int64(14.7 * 1024 * 1024), true},
		{"312.9 KiB", binarySizeUnits, binarySizeBase, int64(312.9 * 1024), true},
		{"512 B", binarySizeUnits, binarySizeBase, 512, true},
		{"5.58MB", decimalSizeUnits, decimalSizeBase, int64(5.58 * 1000 * 1000), true},
		{"312.9kB", decimalSizeUnits, decimalSizeBase, int64(312.9 * 1000), true},
		{"0B", decimalSizeUnits, decimalSizeBase, 0, true},
		{"not a size", binarySizeUnits, binarySizeBase, 0, false},
		{"", binarySizeUnits, binarySizeBase, 0, false},
	}
	for _, c := range cases {
		got, ok := parseApproxBytes(c.s, c.units, c.base)
		if ok != c.ok {
			t.Errorf("parseApproxBytes(%q) ok = %v, want %v", c.s, ok, c.ok)
			continue
		}
		if ok && got != c.want {
			t.Errorf("parseApproxBytes(%q) = %d, want %d", c.s, got, c.want)
		}
	}
}
