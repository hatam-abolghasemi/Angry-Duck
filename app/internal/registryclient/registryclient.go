// Package registryclient reads an image's layer list straight from its
// registry: manifest (through an index when the image is multi-platform)
// and config, which gives every layer's digest, size and chainID. That is
// all the controller needs to score how much of a new image each node
// already holds. Layer blobs themselves are never downloaded here.
//
// It speaks the OCI Distribution API every registry serves (GitLab, Nexus,
// Harbor, Docker Hub, ...) and needs no per-vendor code: it follows the
// WWW-Authenticate challenge the registry answers with, exactly like
// containerd does. A Bearer challenge is answered by fetching a token from
// the realm it names (with the registry's credentials from the
// dockerconfigjson, if any); a Basic challenge by sending the credentials
// directly. Registries that allow anonymous pulls need no credentials.
package registryclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/imageref"
	"angryduck/internal/layerindex"
	"angryduck/internal/registryauth"
)

const acceptManifests = blobship.MediaTypeOCIIndex + ", " + blobship.MediaTypeDockerList + ", " +
	blobship.MediaTypeOCIManifest + ", " + blobship.MediaTypeDockerManifest

// maxMetadata caps manifest and config reads; real ones are a few KB.
const maxMetadata = 4 << 20

// Client resolves images to layer lists, caching results for a while.
type Client struct {
	creds    *registryauth.Store
	http     *http.Client
	cacheTTL time.Duration

	mu     sync.Mutex
	tokens map[string]token // host|scope -> bearer token
	cache  map[string]cached
}

type token struct {
	value   string
	expires time.Time
}

type cached struct {
	layers []layerindex.Layer
	err    error
	at     time.Time
}

// New builds a Client. creds may be nil (anonymous only).
func New(creds *registryauth.Store, timeout, cacheTTL time.Duration) *Client {
	return &Client{
		creds:    creds,
		http:     &http.Client{Timeout: timeout},
		cacheTTL: cacheTTL,
		tokens:   map[string]token{},
		cache:    map[string]cached{},
	}
}

// WithHTTPClient replaces the HTTP client (tests).
func (c *Client) WithHTTPClient(hc *http.Client) *Client { c.http = hc; return c }

// Ref is an image reference split for the distribution API.
type Ref struct {
	Host      string // as named in the reference ("docker.io", "registry.example.com:5000")
	APIHost   string // where the API lives (docker.io -> registry-1.docker.io)
	Repo      string // path inside the registry ("team/app", "library/nginx")
	Reference string // tag or "sha256:..." digest
}

// ParseRef splits a (normalized) image reference.
func ParseRef(image string) (Ref, error) {
	image = imageref.Normalize(image)
	slash := strings.Index(image, "/")
	if slash < 0 {
		return Ref{}, fmt.Errorf("image %q has no registry host", image)
	}
	r := Ref{Host: image[:slash]}
	rest := image[slash+1:]
	if at := strings.Index(rest, "@"); at >= 0 {
		r.Repo, r.Reference = rest[:at], rest[at+1:]
	} else if colon := strings.LastIndex(rest, ":"); colon >= 0 {
		r.Repo, r.Reference = rest[:colon], rest[colon+1:]
	} else {
		r.Repo, r.Reference = rest, "latest"
	}
	if r.Repo == "" || r.Reference == "" {
		return Ref{}, fmt.Errorf("cannot parse image %q", image)
	}
	r.APIHost = r.Host
	if r.Host == "docker.io" || r.Host == "index.docker.io" {
		r.APIHost = "registry-1.docker.io"
	}
	return r, nil
}

// Layers returns image's layers for platform, bottom first.
func (c *Client) Layers(ctx context.Context, image, platform string) ([]layerindex.Layer, error) {
	key := image + "|" + platform
	c.mu.Lock()
	if e, ok := c.cache[key]; ok && time.Since(e.at) < c.cacheTTL {
		c.mu.Unlock()
		return e.layers, e.err
	}
	c.mu.Unlock()

	layers, err := c.resolve(ctx, image, platform)
	c.mu.Lock()
	// Keep the cache small: drop what expired before adding.
	for k, e := range c.cache {
		if time.Since(e.at) >= c.cacheTTL {
			delete(c.cache, k)
		}
	}
	c.cache[key] = cached{layers: layers, err: err, at: time.Now()}
	c.mu.Unlock()
	return layers, err
}

func (c *Client) resolve(ctx context.Context, image, platform string) ([]layerindex.Layer, error) {
	ref, err := ParseRef(image)
	if err != nil {
		return nil, err
	}
	want, err := blobship.ParsePlatform(platform)
	if err != nil {
		return nil, err
	}
	body, mediaType, err := c.get(ctx, ref, "manifests/"+ref.Reference, acceptManifests)
	if err != nil {
		return nil, err
	}
	var doc struct {
		MediaType string                `json:"mediaType"`
		Manifests []blobship.Descriptor `json:"manifests"`
		Config    blobship.Descriptor   `json:"config"`
		Layers    []blobship.Descriptor `json:"layers"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}
	if mediaType == "" || strings.HasPrefix(mediaType, "text/") || mediaType == "application/json" {
		mediaType = doc.MediaType
	}
	if len(doc.Manifests) > 0 || mediaType == blobship.MediaTypeOCIIndex || mediaType == blobship.MediaTypeDockerList {
		chosen, err := pick(doc.Manifests, want)
		if err != nil {
			return nil, fmt.Errorf("image %s: %w", image, err)
		}
		body, _, err = c.get(ctx, ref, "manifests/"+chosen, acceptManifests)
		if err != nil {
			return nil, err
		}
		doc.Config, doc.Layers = blobship.Descriptor{}, nil
		if err := json.Unmarshal(body, &doc); err != nil {
			return nil, fmt.Errorf("parsing platform manifest: %w", err)
		}
	}
	if doc.Config.Digest == "" || len(doc.Layers) == 0 {
		return nil, fmt.Errorf("image %s: manifest has no config or no layers", image)
	}
	cfgBytes, _, err := c.get(ctx, ref, "blobs/"+doc.Config.Digest, "*/*")
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var cfg struct {
		RootFS struct {
			DiffIDs []string `json:"diff_ids"`
		} `json:"rootfs"`
	}
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if len(cfg.RootFS.DiffIDs) != len(doc.Layers) {
		return nil, fmt.Errorf("image %s: %d layers but %d diff_ids", image, len(doc.Layers), len(cfg.RootFS.DiffIDs))
	}
	chains := blobship.ChainIDs(cfg.RootFS.DiffIDs)
	out := make([]layerindex.Layer, len(doc.Layers))
	for i, l := range doc.Layers {
		out[i] = layerindex.Layer{Digest: l.Digest, ChainID: chains[i], Size: l.Size}
	}
	return out, nil
}

func pick(entries []blobship.Descriptor, want blobship.Platform) (string, error) {
	for _, e := range entries {
		if e.Platform == nil || e.Annotations["vnd.docker.reference.type"] == "attestation-manifest" {
			continue
		}
		if e.Platform.OS == want.OS && e.Platform.Architecture == want.Architecture &&
			(want.Variant == "" || e.Platform.Variant == "" || e.Platform.Variant == want.Variant) {
			return e.Digest, nil
		}
	}
	return "", fmt.Errorf("no manifest for %s/%s", want.OS, want.Architecture)
}

// get fetches /v2/<repo>/<path>, answering an auth challenge once.
func (c *Client) get(ctx context.Context, ref Ref, path, accept string) ([]byte, string, error) {
	u := "https://" + ref.APIHost + "/v2/" + ref.Repo + "/" + path
	scope := "repository:" + ref.Repo + ":pull"
	auth := c.cachedToken(ref.APIHost, scope)
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, "", err
		}
		req.Header.Set("Accept", accept)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, "", err
		}
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 {
			challenge := resp.Header.Get("WWW-Authenticate")
			resp.Body.Close()
			auth, err = c.answer(ctx, ref, challenge, scope)
			if err != nil {
				return nil, "", fmt.Errorf("GET %s: auth: %w", u, err)
			}
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
			return nil, "", fmt.Errorf("GET %s: status %d: %s", u, resp.StatusCode, strings.TrimSpace(string(b)))
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, maxMetadata+1))
		if err != nil {
			return nil, "", err
		}
		if len(b) > maxMetadata {
			return nil, "", fmt.Errorf("GET %s: larger than %d bytes", u, maxMetadata)
		}
		mt, _, _ := strings.Cut(resp.Header.Get("Content-Type"), ";")
		return b, strings.TrimSpace(mt), nil
	}
	return nil, "", fmt.Errorf("GET %s: still unauthorized after answering the challenge", u)
}

func (c *Client) cachedToken(host, scope string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.tokens[host+"|"+scope]; ok && time.Now().Before(t.expires) {
		return t.value
	}
	return ""
}

// answer turns a WWW-Authenticate challenge into an Authorization value.
func (c *Client) answer(ctx context.Context, ref Ref, challenge, scope string) (string, error) {
	scheme, params := ParseChallenge(challenge)
	userpass, haveCreds := c.creds.CredentialsFor(ref.Host)
	if !haveCreds && ref.APIHost != ref.Host {
		userpass, haveCreds = c.creds.CredentialsFor(ref.APIHost)
	}
	switch strings.ToLower(scheme) {
	case "basic":
		if !haveCreds {
			return "", fmt.Errorf("registry wants Basic auth and no credentials are configured for %s", ref.Host)
		}
		user, pass, _ := strings.Cut(userpass, ":")
		req, _ := http.NewRequest(http.MethodGet, "http://x", nil)
		req.SetBasicAuth(user, pass)
		return req.Header.Get("Authorization"), nil
	case "bearer":
		realm := params["realm"]
		if realm == "" {
			return "", fmt.Errorf("bearer challenge without a realm: %q", challenge)
		}
		q := url.Values{}
		if s := params["service"]; s != "" {
			q.Set("service", s)
		}
		q.Set("scope", scope)
		sep := "?"
		if strings.Contains(realm, "?") {
			sep = "&"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, realm+sep+q.Encode(), nil)
		if err != nil {
			return "", err
		}
		if haveCreds {
			user, pass, _ := strings.Cut(userpass, ":")
			req.SetBasicAuth(user, pass)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("token endpoint %s: status %d", realm, resp.StatusCode)
		}
		var tr struct {
			Token       string `json:"token"`
			AccessToken string `json:"access_token"`
			ExpiresIn   int    `json:"expires_in"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tr); err != nil {
			return "", fmt.Errorf("token endpoint %s: %w", realm, err)
		}
		tok := tr.Token
		if tok == "" {
			tok = tr.AccessToken
		}
		if tok == "" {
			return "", fmt.Errorf("token endpoint %s returned no token", realm)
		}
		ttl := time.Duration(tr.ExpiresIn) * time.Second
		if ttl <= 0 {
			ttl = 60 * time.Second // the spec's default
		}
		value := "Bearer " + tok
		c.mu.Lock()
		c.tokens[ref.APIHost+"|"+scope] = token{value: value, expires: time.Now().Add(ttl - 10*time.Second)}
		for k, t := range c.tokens {
			if time.Now().After(t.expires) {
				delete(c.tokens, k)
			}
		}
		c.mu.Unlock()
		return value, nil
	default:
		return "", fmt.Errorf("unsupported auth challenge %q", challenge)
	}
}

// ParseChallenge splits `Bearer realm="...",service="..."` into its scheme
// and parameters.
func ParseChallenge(h string) (scheme string, params map[string]string) {
	params = map[string]string{}
	h = strings.TrimSpace(h)
	scheme, rest, _ := strings.Cut(h, " ")
	for rest != "" {
		rest = strings.TrimLeft(rest, " ,")
		k, v, ok := strings.Cut(rest, "=")
		if !ok {
			break
		}
		k = strings.ToLower(strings.TrimSpace(k))
		if strings.HasPrefix(v, `"`) {
			end := strings.Index(v[1:], `"`)
			if end < 0 {
				params[k] = v[1:]
				break
			}
			params[k] = v[1 : end+1]
			rest = v[end+2:]
		} else {
			val, after, _ := strings.Cut(v, ",")
			params[k] = strings.TrimSpace(val)
			rest = after
		}
	}
	return scheme, params
}
