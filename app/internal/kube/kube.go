// Package kube is the smallest Kubernetes API client Angry Duck needs:
// the controller lists and watches pods with a field selector, and a worker checks on
// shutdown whether its DaemonSet is being deleted. It is written
// against net/http directly to keep Angry Duck free of client-go.
package kube

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// Client lists pods through the API server.
type Client struct {
	baseURL   string
	tokenPath string
	http      *http.Client
}

// InCluster builds a Client from the pod's service account, the same way
// client-go's rest.InClusterConfig does.
func InCluster() (*Client, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("not running in a cluster (KUBERNETES_SERVICE_HOST/PORT unset)")
	}
	ca, err := os.ReadFile(saDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("reading service account CA (is automountServiceAccountToken off?): %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("no certificates in %s/ca.crt", saDir)
	}
	return &Client{
		baseURL:   "https://" + net.JoinHostPort(host, port),
		tokenPath: saDir + "/token",
		http: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		},
	}, nil
}

// New builds a Client for an explicit base URL, for tests.
func New(baseURL, tokenPath string, hc *http.Client) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), tokenPath: tokenPath, http: hc}
}

// Pod is the part of a pod object the rescuer reads.
type Pod struct {
	Metadata struct {
		Namespace       string `json:"namespace"`
		Name            string `json:"name"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Spec struct {
		NodeName       string      `json:"nodeName"`
		Containers     []Container `json:"containers"`
		InitContainers []Container `json:"initContainers"`
	} `json:"spec"`
	Status struct {
		ContainerStatuses     []ContainerStatus `json:"containerStatuses"`
		InitContainerStatuses []ContainerStatus `json:"initContainerStatuses"`
	} `json:"status"`
}

// Container is the part of a container spec the rescuer reads.
type Container struct {
	Name            string `json:"name"`
	Image           string `json:"image"`
	ImagePullPolicy string `json:"imagePullPolicy"`
}

// ContainerStatus is the part of a container status the rescuer reads.
type ContainerStatus struct {
	Name  string `json:"name"`
	Image string `json:"image"`
	State struct {
		Waiting *struct {
			Reason string `json:"reason"`
		} `json:"waiting"`
	} `json:"state"`
}

type podList struct {
	Items    []Pod `json:"items"`
	Metadata struct {
		Continue        string `json:"continue"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
}

// ListPods lists pods in every namespace matching fieldSelector, following
// pagination.
func (c *Client) ListPods(ctx context.Context, fieldSelector string) ([]Pod, error) {
	pods, _, err := c.listPods(ctx, fieldSelector)
	return pods, err
}

// listPods also returns the list's resourceVersion, where a watch resumes.
// Every page of a paginated list is served at the first page's version.
func (c *Client) listPods(ctx context.Context, fieldSelector string) ([]Pod, string, error) {
	var all []Pod
	cont, rv := "", ""
	for {
		q := url.Values{}
		q.Set("fieldSelector", fieldSelector)
		q.Set("limit", "500")
		if cont != "" {
			q.Set("continue", cont)
		}
		var page podList
		if err := c.get(ctx, "/api/v1/pods?"+q.Encode(), &page); err != nil {
			return nil, "", err
		}
		if rv == "" {
			rv = page.Metadata.ResourceVersion
		}
		all = append(all, page.Items...)
		if page.Metadata.Continue == "" {
			return all, rv, nil
		}
		cont = page.Metadata.Continue
	}
}

func (c *Client) get(ctx context.Context, path string, into interface{}) error {
	resp, err := c.open(ctx, c.http, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(into)
}

// open sends an authenticated GET and returns the 200 response; any other
// status is a *StatusError.
func (c *Client) open(ctx context.Context, hc *http.Client, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	// Re-read every call: projected service account tokens are rotated by
	// kubelet (hourly by default), and a cached one would start failing.
	if c.tokenPath != "" {
		tok, err := os.ReadFile(c.tokenPath)
		if err != nil {
			return nil, fmt.Errorf("reading service account token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	}
	req.Header.Set("Accept", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, &StatusError{Code: resp.StatusCode, msg: fmt.Sprintf("GET %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(b)))}
	}
	return resp, nil
}

// StatusError is a non-200 answer from the API server.
type StatusError struct {
	Code int
	msg  string
}

func (e *StatusError) Error() string { return e.msg }

// Namespace is the pod's own namespace, from the service account mount.
func Namespace() string {
	b, err := os.ReadFile(saDir + "/namespace")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// DaemonSetGone reports whether a DaemonSet is deleted (404) or being
// deleted (deletionTimestamp set). Any other failure is an error: the
// caller can't tell.
func (c *Client) DaemonSetGone(ctx context.Context, namespace, name string) (bool, error) {
	var ds struct {
		Metadata struct {
			DeletionTimestamp *string `json:"deletionTimestamp"`
		} `json:"metadata"`
	}
	err := c.get(ctx, "/apis/apps/v1/namespaces/"+url.PathEscape(namespace)+"/daemonsets/"+url.PathEscape(name), &ds)
	var se *StatusError
	if errors.As(err, &se) && se.Code == http.StatusNotFound {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return ds.Metadata.DeletionTimestamp != nil, nil
}
