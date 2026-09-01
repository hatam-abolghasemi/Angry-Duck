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
