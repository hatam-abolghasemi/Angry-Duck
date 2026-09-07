package imageref

import "testing"

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		// The exact case that broke in production.
		"nginx:latest": "docker.io/library/nginx:latest",
		"nginx":        "docker.io/library/nginx:latest",

		// Namespaced Docker Hub image, no host.
		"myuser/myimage:tag": "docker.io/myuser/myimage:tag",
		"myuser/myimage":     "docker.io/myuser/myimage:latest",

		// Already fully qualified with a real registry host — untouched
		// except for a missing tag.
		"registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2": "registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2",
		"gcr.io/distroless/static-debian13":                            "gcr.io/distroless/static-debian13:latest",

		// Host with an explicit port, no dot in the hostname.
		"localhost:5000/foo:tag": "localhost:5000/foo:tag",
		"localhost:5000/foo":     "localhost:5000/foo:latest",
		"localhost/foo":          "localhost/foo:latest",

		// Digest references are left completely alone — appending ":latest"
		// to one would be invalid.
		"nginx@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":                             "docker.io/library/nginx@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"registry.internal-registry.example.com/devops/generic/angry-duck-worker@sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36": "registry.internal-registry.example.com/devops/generic/angry-duck-worker@sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36",

		// Empty input stays empty rather than becoming a bogus reference.
		"": "",
	}

	for input, want := range cases {
		if got := Normalize(input); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNormalizeWhitespace(t *testing.T) {
	if got := Normalize("  nginx:latest  "); got != "docker.io/library/nginx:latest" {
		t.Errorf("Normalize did not trim whitespace: got %q", got)
	}
}

func TestHost(t *testing.T) {
	cases := map[string]string{
		"docker.io/library/nginx:latest":                               "docker.io",
		"registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2": "registry.internal-registry.example.com",
		"localhost:5000/foo:tag":                                       "localhost:5000",
		"nginx:latest":                                                 "nginx:latest", // not normalized first — no slash, so the whole string is returned as-is
	}
	for input, want := range cases {
		if got := Host(input); got != want {
			t.Errorf("Host(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestRepo(t *testing.T) {
	cases := map[string]string{
		// Same repo, different tags -> same repo identity.
		"registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.2": "registry.internal-registry.example.com/devops/generic/angry-duck-worker",
		"registry.internal-registry.example.com/devops/generic/angry-duck-worker:1.0.9": "registry.internal-registry.example.com/devops/generic/angry-duck-worker",

		// Digest-pinned form -> everything before "@".
		"registry.internal-registry.example.com/devops/generic/angry-duck-worker@sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36": "registry.internal-registry.example.com/devops/generic/angry-duck-worker",

		// Bare content digest ("image ID" alias) carries no repo name.
		"sha256:0ea5747ba9dd2dacae537ee2aa42f3883abb1508b36abdcea77152208e4a79b4": "",

		// Host with an explicit port must not be mistaken for a tag.
		"localhost:5000/foo:tag": "localhost:5000/foo",
		"localhost:5000/foo":     "localhost:5000/foo",

		// No tag at all -> the whole thing is already the repo.
		"docker.io/library/nginx": "docker.io/library/nginx",

		// Unqualified short form (raw runtime output, not Normalize()d).
		"nginx:latest": "nginx",
	}
	for input, want := range cases {
		if got := Repo(input); got != want {
			t.Errorf("Repo(%q) = %q, want %q", input, got, want)
		}
	}
}
