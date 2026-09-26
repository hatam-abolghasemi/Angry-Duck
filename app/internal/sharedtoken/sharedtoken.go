// Package sharedtoken guards the rescue endpoints with one bearer token,
// shared by the controller and every worker through a Kubernetes Secret.
//
// The worker's /blobs/* endpoints hand out image content, including
// private images, to anyone who can reach port 18081 on the node network.
// So when the token is missing the rescue feature turns itself off instead
// of serving unauthenticated.
package sharedtoken

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// minLength rejects obviously weak tokens, such as a placeholder left in a
// Secret. `openssl rand -hex 32` gives 64 characters.
const minLength = 32

// Load reads the token from path. Surrounding whitespace (a trailing
// newline from `echo` or `kubectl create secret --from-file`) is ignored.
func Load(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("no token path configured")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(b))
	if len(tok) < minLength {
		return "", fmt.Errorf("token in %s is %d characters, need at least %d", path, len(tok), minLength)
	}
	return tok, nil
}

// Set adds the token to an outgoing request.
func Set(r *http.Request, token string) {
	r.Header.Set("Authorization", "Bearer "+token)
}

// Valid reports whether r carries token, comparing in constant time.
func Valid(r *http.Request, token string) bool {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}

// Require wraps h so it only runs for requests carrying token.
func Require(token string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !Valid(r, token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h(w, r)
	}
}
