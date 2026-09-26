package worker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// excludedFstypes mirrors the fstype!~"tmpfs|overlay|fuse.lxcfs" matcher from
// the design doc's PromQL expression.
var excludedFstypes = map[string]bool{
	"tmpfs":      true,
	"overlay":    true,
	"fuse.lxcfs": true,
}

// UtilizationResult carries both the final ratio and the raw inputs it was
// computed from, so callers can log the actual math (not just the answer)
// when troubleshooting a suspicious utilization reading.
type UtilizationResult struct {
	SizeBytes   float64
	FreeBytes   float64
	UsedBytes   float64
	Utilization float64 // (SizeBytes - FreeBytes) / SizeBytes
}

// FetchRootUtilization scrapes a node-exporter /metrics endpoint and
// computes (size-free)/size for the root filesystem ("/"), replicating:
//
//	(node_filesystem_size_bytes{fstype!~"tmpfs|overlay|fuse.lxcfs", mountpoint="/"}
//	  - node_filesystem_free_bytes{fstype!~"tmpfs|overlay|fuse.lxcfs", mountpoint="/"})
//	  / node_filesystem_size_bytes{fstype!~"tmpfs|overlay|fuse.lxcfs", mountpoint="/"}
//
// It does a lightweight line-based scan of the Prometheus text exposition
// format rather than pulling in a full client_golang/prometheus parser
// dependency, since we only need two specific metric families.
//
// It asks node-exporter for the filesystem collector only
// (collect[]=filesystem), so node-exporter skips its other collectors and
// the page is a few KB instead of hundreds. An endpoint that rejects the
// filter is asked for the full page from then on.
func FetchRootUtilization(metricsURL string, timeout time.Duration) (UtilizationResult, error) {
	if !collectFilterRejected.Load() {
		res, status, err := fetchRootUtilization(withCollectFilter(metricsURL), timeout)
		if status == http.StatusOK || status == 0 {
			return res, err
		}
		collectFilterRejected.Store(true)
	}
	res, _, err := fetchRootUtilization(metricsURL, timeout)
	return res, err
}

// collectFilterRejected records that the endpoint didn't accept
// collect[]=filesystem.
var collectFilterRejected atomic.Bool

// utilizationClient is reused so the connection to node-exporter stays open.
var utilizationClient = &http.Client{}

func withCollectFilter(metricsURL string) string {
	u, err := url.Parse(metricsURL)
	if err != nil {
		return metricsURL
	}
	q := u.Query()
	if len(q["collect[]"]) > 0 {
		return metricsURL // the operator chose their own filter
	}
	q.Set("collect[]", "filesystem")
	u.RawQuery = q.Encode()
	return u.String()
}

// fetchRootUtilization returns the HTTP status (0 if the request failed
// before one arrived) alongside the result.
func fetchRootUtilization(metricsURL string, timeout time.Duration) (UtilizationResult, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return UtilizationResult{}, 0, err
	}
	resp, err := utilizationClient.Do(req)
	if err != nil {
		return UtilizationResult{}, 0, fmt.Errorf("fetching %s: %w", metricsURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return UtilizationResult{}, resp.StatusCode, fmt.Errorf("fetching %s: unexpected status %d", metricsURL, resp.StatusCode)
	}
	res, err := parseRootUtilization(resp.Body)
	// Drain what's left (bounded) so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<20))
	return res, resp.StatusCode, err
}

func parseRootUtilization(r io.Reader) (UtilizationResult, error) {
	var size, free float64
	var sawSize, sawFree bool

	sizePrefix := []byte("node_filesystem_size_bytes{")
	freePrefix := []byte("node_filesystem_free_bytes{")
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		// Bytes, not Text: most lines are skipped, and converting each to
		// a string would allocate for every line of a large page.
		b := scanner.Bytes()

		var family string
		switch {
		case bytes.HasPrefix(b, sizePrefix):
			family = "size"
		case bytes.HasPrefix(b, freePrefix):
			family = "free"
		default:
			continue
		}
		line := string(b)

		labelsEnd := strings.Index(line, "}")
		if labelsEnd < 0 {
			continue
		}
		labels := line[:labelsEnd+1]

		if !strings.Contains(labels, `mountpoint="/"`) {
			continue
		}
		if hasExcludedFstype(labels) {
			continue
		}

		valStr := strings.TrimSpace(line[labelsEnd+1:])
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}

		switch family {
		case "size":
			size = val
			sawSize = true
		case "free":
			free = val
			sawFree = true
		}
		if sawSize && sawFree {
			break // both found; skip the rest of the page
		}
	}
	if err := scanner.Err(); err != nil {
		return UtilizationResult{}, err
	}
	if !sawSize || !sawFree {
		return UtilizationResult{}, fmt.Errorf("root filesystem metrics not found (mountpoint=\"/\")")
	}
	if size == 0 {
		return UtilizationResult{}, fmt.Errorf("root filesystem size is zero")
	}
	used := size - free
	return UtilizationResult{
		SizeBytes:   size,
		FreeBytes:   free,
		UsedBytes:   used,
		Utilization: used / size,
	}, nil
}

// hasExcludedFstype checks the fstype="..." label against the excluded set.
func hasExcludedFstype(labels string) bool {
	const key = `fstype="`
	idx := strings.Index(labels, key)
	if idx < 0 {
		return false
	}
	rest := labels[idx+len(key):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return false
	}
	return excludedFstypes[rest[:end]]
}
