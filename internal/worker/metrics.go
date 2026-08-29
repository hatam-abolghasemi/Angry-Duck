package worker

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// excludedFstypes mirrors the fstype!~"tmpfs|overlay|fuse.lxcfs" matcher from
// the design doc's PromQL expression.
var excludedFstypes = map[string]bool{
	"tmpfs":      true,
	"overlay":    true,
	"fuse.lxcfs": true,
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
func FetchRootUtilization(metricsURL string, timeout time.Duration) (float64, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(metricsURL)
	if err != nil {
		return 0, fmt.Errorf("fetching %s: %w", metricsURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("fetching %s: unexpected status %d", metricsURL, resp.StatusCode)
	}
	return parseRootUtilization(resp.Body)
}

func parseRootUtilization(r io.Reader) (float64, error) {
	var size, free float64
	var sawSize, sawFree bool

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		var family string
		switch {
		case strings.HasPrefix(line, "node_filesystem_size_bytes{"):
			family = "size"
		case strings.HasPrefix(line, "node_filesystem_free_bytes{"):
			family = "free"
		default:
			continue
		}

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
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	if !sawSize || !sawFree {
		return 0, fmt.Errorf("root filesystem metrics not found (mountpoint=\"/\")")
	}
	if size == 0 {
		return 0, fmt.Errorf("root filesystem size is zero")
	}
	return (size - free) / size, nil
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
