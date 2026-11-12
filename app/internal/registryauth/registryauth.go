// Package registryauth reads a standard Kubernetes dockerconfigjson
// secret (the same format `imagePullSecrets` uses) and resolves
// credentials for a given registry host.
//
// It exists because kubelet only forwards a Pod's imagePullSecrets for
// the images kubelet itself pulls. A seed pull the worker makes through
// CRI, and the controller's manifest reads, carry no credentials unless
// something supplies them. This package is that "something."
package registryauth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// dockerConfigJSON mirrors the shape of a Kubernetes dockerconfigjson
// secret's .dockerconfigjson value (the same file format `docker login`
// writes to ~/.docker/config.json):
//
//	{"auths": {"registry.example.com": {"auth": "base64(user:pass)"}}}
type dockerConfigJSON struct {
	Auths map[string]authEntry `json:"auths"`
}

type authEntry struct {
	Auth     string `json:"auth"`     // base64("username:password") — the common form
	Username string `json:"username"` // rarely present directly, but handled if it is
	Password string `json:"password"`
}

// Store resolves registry host -> "username:password" credentials.
type Store struct {
	// creds maps registry host to "username:password" in plain form.
	creds map[string]string
}

// Empty returns a Store with no credentials — every lookup misses, so
// every pull proceeds anonymously. Used when no credentials file is
// configured, so the rest of the code never needs a nil check.
func Empty() *Store {
	return &Store{creds: map[string]string{}}
}

// Load reads a dockerconfigjson file from disk and builds a Store. It is
// not an error for the file to be missing — that just means no registry
// credentials are configured, and every pull will be attempted
// anonymously (fine for public images, expected to fail with a 401/403
// for private ones).
func Load(path string) (*Store, error) {
	if path == "" {
		return Empty(), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Empty(), nil
		}
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	var parsed dockerConfigJSON
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("parsing %s as dockerconfigjson: %w", path, err)
	}

	creds := make(map[string]string, len(parsed.Auths))
	for host, entry := range parsed.Auths {
		userpass, ok := resolveUserPass(entry)
		if !ok {
			continue
		}
		creds[normalizeHost(host)] = userpass
	}
	return &Store{creds: creds}, nil
}

// CredentialsFor returns "username:password" for the given registry host,
// and whether any credentials were found for it.
func (s *Store) CredentialsFor(host string) (string, bool) {
	if s == nil {
		return "", false
	}
	userpass, ok := s.creds[normalizeHost(host)]
	return userpass, ok
}

// Count returns how many registries have credentials loaded — used only
// for a friendly startup log line, not for any decision-making.
func (s *Store) Count() int {
	if s == nil {
		return 0
	}
	return len(s.creds)
}

func resolveUserPass(entry authEntry) (string, bool) {
	if entry.Username != "" {
		return entry.Username + ":" + entry.Password, true
	}
	if entry.Auth == "" {
		return "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(entry.Auth)
	if err != nil {
		return "", false
	}
	if !strings.Contains(string(decoded), ":") {
		return "", false
	}
	return string(decoded), true
}

// normalizeHost strips a leading scheme, if the config's auths key happens
// to include one (some tools write "https://registry.example.com" as the
// key; image references never have a scheme), so lookups by host alone
// still match.
func normalizeHost(host string) string {
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimSuffix(host, "/")
	return host
}
