// Package imageref normalizes container image references the way `docker`
// does implicitly, but `ctr` does not.
//
// `docker pull nginx` silently expands to `docker.io/library/nginx:latest`
// before doing anything else. `ctr images pull nginx:latest` does no such
// expansion — it tries to parse "nginx" as a registry hostname and
// ":latest" as a port number, and fails with a confusing
// `invalid port ":latest" after host` error. Since Angry Duck's worker
// shells out to `ctr` by default, any caller (a CI pipeline, a person
// testing the webhook by hand) who reasonably expects Docker-style short
// references to just work will hit this. Normalize() closes that gap by
// applying Docker's own reference-qualification rules before the
// reference ever reaches `ctr`.
package imageref

import "strings"

// Normalize expands a short, Docker-style image reference into a fully
// qualified one, following the same rules `docker` applies implicitly:
//
//   - No registry host at all ("nginx", "nginx:latest") -> assume
//     docker.io and the "library/" namespace for official images
//     ("docker.io/library/nginx:latest").
//   - A namespace but no host ("myuser/myimage:tag") -> assume docker.io
//     ("docker.io/myuser/myimage:tag").
//   - An explicit host, detected by the first path segment containing a
//     "." (a domain), a ":" (a host:port), or being exactly "localhost"
//     -> left alone, since it's already fully qualified.
//   - No tag and no digest -> append ":latest", the same default Docker
//     assumes.
//   - A digest reference ("...@sha256:...") -> left alone entirely; it's
//     already maximally specific and appending a tag would be invalid.
//
// This mirrors the effect of Docker's reference.ParseNormalizedNamed
// without pulling in that dependency, since Angry Duck only needs this one
// behavior from it.
func Normalize(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ref
	}

	// Digest references are already fully specified; don't touch them.
	if strings.Contains(ref, "@sha256:") {
		return qualifyHost(ref)
	}

	qualified := qualifyHost(ref)
	return ensureTag(qualified)
}

// qualifyHost prepends "docker.io/" (and "library/" for single-segment
// official image names) if ref's first path segment doesn't already look
// like a registry host.
func qualifyHost(ref string) string {
	idx := strings.Index(ref, "/")
	if idx < 0 {
		// No slash at all, e.g. "nginx" or "nginx:latest" — a bare name
		// can never itself be a registry host, so this is always an
		// unqualified official image reference. (Checking the whole
		// string for a ":" here would wrongly treat the tag separator in
		// "nginx:latest" as a host:port separator.)
		return "docker.io/library/" + ref
	}

	firstSegment := ref[:idx]
	looksLikeHost := strings.Contains(firstSegment, ".") ||
		strings.Contains(firstSegment, ":") ||
		firstSegment == "localhost"

	if looksLikeHost {
		return ref // already qualified with an explicit registry host
	}

	// Has a namespace but no host, e.g. "myuser/myimage:tag".
	return "docker.io/" + ref
}

// ensureTag appends ":latest" if ref has neither a tag nor a digest.
// Digest references are detected and left untouched by the "@sha256:"
// check in Normalize before this is ever reached for that case, but this
// function is also safe to call standalone.
func ensureTag(ref string) string {
	if strings.Contains(ref, "@sha256:") {
		return ref
	}
	// A tag looks like "name:tag" — but a registry host can also contain a
	// ':' for its port (e.g. "localhost:5000/name"), so only a colon
	// appearing after the last '/' counts as a tag separator.
	lastSlash := strings.LastIndex(ref, "/")
	afterSlash := ref[lastSlash+1:]
	if strings.Contains(afterSlash, ":") {
		return ref // already has a tag
	}
	return ref + ":latest"
}

// Host extracts the registry host from a reference. Callers should
// normally pass a Normalize()d reference, since normalization guarantees
// an explicit host is present — Host makes no attempt to guess a default
// registry itself.
func Host(ref string) string {
	if idx := strings.Index(ref, "/"); idx >= 0 {
		return ref[:idx]
	}
	return ref
}

// Repo strips the tag or digest suffix off ref, returning the bare
// repository identity (registry host + path) that's shared by every tag
// ever pushed under that name. This is the identity the preheat ranker
// matches on: pulling a newly-pushed tag onto a node that already has ANY
// older tag of the same repo is usually much cheaper than a cold pull,
// because most of an image's layers (base image, language runtime,
// dependencies) are typically unchanged between tags of the same repo —
// only the top application layer usually moves. Repo() is deliberately
// this coarse (no attempt to check actual layer overlap via a registry
// manifest call) so the controller can make a same-repo decision from
// data it already has in memory, with zero extra network calls, keeping
// the preheat decision fast enough to matter against the ArgoCD sync
// window it's racing.
//
//   - A digest reference ("repo@sha256:...") -> everything before "@".
//   - A bare content digest with no repo name at all ("sha256:...", the
//     "image ID" alias containerd/crictl report alongside tag and @digest
//     forms) -> "", since there's no repository identity to extract; the
//     caller should skip it rather than treat the mangled digest text as
//     a fake repo name.
//   - Anything else -> everything before the last ":" that appears after
//     the final "/", the same tag-boundary rule ensureTag uses, so a
//     registry host's own ":port" is never mistaken for a tag separator.
func Repo(ref string) string {
	if idx := strings.Index(ref, "@sha256:"); idx >= 0 {
		return ref[:idx]
	}
	if strings.HasPrefix(ref, "sha256:") {
		return ""
	}
	lastSlash := strings.LastIndex(ref, "/")
	afterSlash := ref[lastSlash+1:]
	if colon := strings.Index(afterSlash, ":"); colon >= 0 {
		return ref[:lastSlash+1+colon]
	}
	return ref
}

// RegistryHost returns the registry host implied by ref, applying the
// same docker.io-default assumption Normalize applies when pulling: a
// reference with no explicit host segment ("nginx:latest",
// "myuser/myimage:tag") is assumed to belong to docker.io, exactly as
// `docker pull` would resolve it. Unlike Host, this never mistakes an
// unqualified reference's first path segment for a real host — it falls
// back to the default instead, using the same "does the first segment
// look like a host" check qualifyHost uses.
//
// A bare content digest ("sha256:...", the alias-only "image ID" form
// containerd/crictl report alongside a ref's tag and @digest aliases)
// carries no naming information at all, so this returns "" for it —
// callers should treat that as "unknown", the same way Repo does.
func RegistryHost(ref string) string {
	if strings.HasPrefix(ref, "sha256:") && !strings.Contains(ref, "/") {
		return ""
	}
	idx := strings.Index(ref, "/")
	if idx < 0 {
		return "docker.io"
	}
	firstSegment := ref[:idx]
	looksLikeHost := strings.Contains(firstSegment, ".") ||
		strings.Contains(firstSegment, ":") ||
		firstSegment == "localhost"
	if looksLikeHost {
		return firstSegment
	}
	return "docker.io"
}

// RegistryLabel returns RegistryHost(ref) when enabled is true, or ""
// otherwise. Metrics that label by registry host use this instead of
// calling RegistryHost directly, so that toggling the feature off (see
// METRICS_LABEL_REGISTRY in the worker/controller .env) collapses every
// series back to one constant label value instead of one per registry —
// the operator-facing cardinality kill switch, in one place shared by
// both binaries rather than duplicated at every counter call site.
func RegistryLabel(ref string, enabled bool) string {
	if !enabled {
		return ""
	}
	return RegistryHost(ref)
}
