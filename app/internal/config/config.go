// Package config provides a minimal, dependency-free .env loader plus typed
// accessors with sane defaults. Every tunable in the Angry Duck design doc
// (intervals, thresholds, top-N, grace periods, ...) is read through here so
// it can all live in one .env file.
package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"time"
)

// Load reads a .env file (if present) and sets any variables it defines into
// the process environment, without overriding variables already set there
// (real environment variables / k8s env / secrets always win). It is not an
// error for the file to be missing.
func Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, "=")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		val = strings.Trim(val, `"'`)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}
	return scanner.Err()
}

// String returns the env var value or a default.
func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// Int returns the env var parsed as int, or a default if unset/unparsable.
func Int(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// Float returns the env var parsed as float64, or a default.
func Float(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}

// Duration returns the env var parsed as seconds (plain integer) turned into
// a time.Duration, or a default. Every interval in the design doc is
// specified in seconds, so this keeps the .env file simple (RANK_INTERVAL_S=10
// rather than requiring Go duration syntax).
func Duration(key string, defSeconds int) time.Duration {
	return time.Duration(Int(key, defSeconds)) * time.Second
}

// Bool returns the env var parsed as bool, or a default.
func Bool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// StringSlice returns the env var split on commas, trimming whitespace and
// dropping empty entries, or def if the env var is unset/empty. Used for
// tunables that are naturally a short list rather than a single value
// (e.g. RANK_EXCLUDE_NODE_SUBSTRINGS=master,control-plane).
func StringSlice(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	var out []string
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
