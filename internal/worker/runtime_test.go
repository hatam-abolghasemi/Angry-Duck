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
