package blobship

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
)

// MemStore is an in-memory Store for tests. Its Import behaves like
// containerd's import for one platform: it verifies every blob against its
// digest, keeps blobs it already had, registers the name from index.json,
// then unpacks: a layer whose chainID snapshot exists is skipped, any
// other layer needs its blob, and a missing one fails the import.
type MemStore struct {
	mu         sync.Mutex
	blobs      map[string][]byte
	images     map[string][2]string // name -> {mediaType, digest}
	snaps      map[string]memSnap   // chainID -> snapshot
	Imports    [][]string           // blob digests carried by each Import, in order
	Applied    []string             // chainIDs created by ApplySnapshot, in order
	Deleted    []string             // image names removed by DeleteImages, in order
	Pinned     map[string]bool      // chainIDs currently pinned
	FailApply  bool                 // make ApplySnapshot fail before committing
	FailImport bool                 // make Import fail after reading the archive
}

type memSnap struct {
	parent string
	data   []byte
}

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{blobs: map[string][]byte{}, images: map[string][2]string{}, snaps: map[string]memSnap{}, Pinned: map[string]bool{}}
}

// HasSnapshot reports whether chainID exists.
func (m *MemStore) HasSnapshot(chainID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.snaps[chainID]
	return ok
}

// Snapshots implements Store.
func (m *MemStore) Snapshots(context.Context) (map[string]bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]bool, len(m.snaps))
	for k := range m.snaps {
		out[k] = true
	}
	return out, nil
}

// ExportSnapshot implements Store.
func (m *MemStore) ExportSnapshot(_ context.Context, chainID, parent string, w io.Writer) error {
	m.mu.Lock()
	sn, ok := m.snaps[chainID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("snapshot %s not found", chainID)
	}
	if sn.parent != parent {
		return fmt.Errorf("snapshot %s has parent %q, expected %q", chainID, sn.parent, parent)
	}
	_, err := w.Write(sn.data)
	return err
}

// ApplySnapshot implements Store.
func (m *MemStore) ApplySnapshot(_ context.Context, chainID, parent string, r io.Reader, verify func() error) error {
	if parent != "" && !m.HasSnapshot(parent) {
		return fmt.Errorf("parent snapshot %s not found", parent)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := verify(); err != nil {
		return err
	}
	if m.FailApply {
		return fmt.Errorf("apply failed (test)")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.snaps[chainID]; !exists {
		m.snaps[chainID] = memSnap{parent: parent, data: data}
		m.Applied = append(m.Applied, chainID)
	}
	m.Pinned[chainID] = true
	return nil
}

// Unpin implements Store.
func (m *MemStore) Unpin(_ context.Context, chainID string) error {
	m.mu.Lock()
	delete(m.Pinned, chainID)
	m.mu.Unlock()
	return nil
}

// RemoveSnapshot implements Store.
func (m *MemStore) RemoveSnapshot(_ context.Context, key string) error {
	m.DeleteSnapshot(key)
	return nil
}

// AddRawSnapshot adds a snapshot under any key (tests).
func (m *MemStore) AddRawSnapshot(key string) {
	m.mu.Lock()
	m.snaps[key] = memSnap{}
	m.mu.Unlock()
}

// DeleteSnapshot removes a snapshot (tests).
func (m *MemStore) DeleteSnapshot(chainID string) {
	m.mu.Lock()
	delete(m.snaps, chainID)
	m.mu.Unlock()
}

// unpack mimics containerd's unpack: skip layers whose snapshot exists,
// apply the blob for the rest.
func (m *MemStore) unpack(ctx context.Context, name, platform string) error {
	plan, err := BuildPlan(ctx, m, name, platform)
	if err != nil {
		return err
	}
	if len(plan.Layers) == 0 {
		have, _ := m.Digests(ctx)
		if gaps := plan.Missing(have); len(gaps) > 0 {
			return fmt.Errorf("content digest %s: not found", gaps[0].Digest)
		}
		return nil
	}
	parent := ""
	for _, l := range plan.Layers {
		m.mu.Lock()
		_, exists := m.snaps[l.ChainID]
		blob, haveBlob := m.blobs[l.Blob.Digest]
		if !exists {
			if !haveBlob {
				m.mu.Unlock()
				return fmt.Errorf("content digest %s: not found", l.Blob.Digest)
			}
			m.snaps[l.ChainID] = memSnap{parent: parent, data: blob}
		}
		m.mu.Unlock()
		parent = l.ChainID
	}
	return nil
}

// Put stores data and returns its digest.
func (m *MemStore) Put(data []byte) string {
	sum := sha256.Sum256(data)
	d := "sha256:" + hex.EncodeToString(sum[:])
	m.mu.Lock()
	m.blobs[d] = append([]byte(nil), data...)
	m.mu.Unlock()
	return d
}

// Delete removes a blob.
func (m *MemStore) Delete(digest string) {
	m.mu.Lock()
	delete(m.blobs, digest)
	m.mu.Unlock()
}

// DeleteImages implements Store.
func (m *MemStore) DeleteImages(_ context.Context, names ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, n := range names {
		delete(m.images, n)
		m.Deleted = append(m.Deleted, n)
	}
	return nil
}

// HasImage reports whether name is registered.
func (m *MemStore) HasImage(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.images[name]
	return ok
}

// Tag points name at digest.
func (m *MemStore) Tag(name, mediaType, digest string) {
	m.mu.Lock()
	m.images[name] = [2]string{mediaType, digest}
	m.mu.Unlock()
}

// Resolve implements Store.
func (m *MemStore) Resolve(_ context.Context, image string) (string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	img, ok := m.images[image]
	if !ok {
		return "", "", fmt.Errorf("image %s not found", image)
	}
	return img[0], img[1], nil
}

// ReadBlob implements Store.
func (m *MemStore) ReadBlob(_ context.Context, digest string, max int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobs[digest]
	if !ok {
		return nil, fmt.Errorf("blob %s not found", digest)
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("blob %s too large", digest)
	}
	return append([]byte(nil), b...), nil
}

// StreamBlob implements Store.
func (m *MemStore) StreamBlob(_ context.Context, digest string, size int64, w io.Writer) error {
	m.mu.Lock()
	b, ok := m.blobs[digest]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("blob %s not found", digest)
	}
	n, err := w.Write(b)
	if err != nil {
		return err
	}
	if int64(n) != size {
		return fmt.Errorf("blob %s: got %d bytes, expected %d", digest, n, size)
	}
	return nil
}

// StreamContent writes a whole blob to w.
func (m *MemStore) StreamContent(_ context.Context, digest string, w io.Writer) error {
	m.mu.Lock()
	b, ok := m.blobs[digest]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("blob %s not found", digest)
	}
	_, err := w.Write(b)
	return err
}

// Digests implements Store.
func (m *MemStore) Digests(context.Context) (map[string]bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]bool, len(m.blobs))
	for d := range m.blobs {
		out[d] = true
	}
	return out, nil
}

// Import implements Store.
func (m *MemStore) Import(ctx context.Context, r io.Reader, platform string) error {
	tr := tar.NewReader(r)
	var index []byte
	var carried []string
	staged := map[string][]byte{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading archive: %w", err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return fmt.Errorf("reading %s: %w", h.Name, err)
		}
		switch {
		case h.Name == "index.json":
			index = data
		case strings.HasPrefix(h.Name, "blobs/sha256/") && h.Typeflag == tar.TypeReg:
			want := "sha256:" + strings.TrimPrefix(h.Name, "blobs/sha256/")
			sum := sha256.Sum256(data)
			if got := "sha256:" + hex.EncodeToString(sum[:]); got != want {
				return fmt.Errorf("digest mismatch for %s: got %s", want, got)
			}
			staged[want] = data
			carried = append(carried, want)
		}
	}
	if index == nil {
		return fmt.Errorf("archive has no index.json")
	}
	if m.FailImport {
		return fmt.Errorf("import failed (test)")
	}
	var idx struct {
		Manifests []Descriptor `json:"manifests"`
	}
	if err := json.Unmarshal(index, &idx); err != nil || len(idx.Manifests) != 1 {
		return fmt.Errorf("bad index.json")
	}
	top := idx.Manifests[0]
	name := top.Annotations["io.containerd.image.name"]
	if name == "" {
		return fmt.Errorf("index.json has no image name")
	}

	m.mu.Lock()
	for d, b := range staged {
		m.blobs[d] = b
	}
	m.images[name] = [2]string{top.MediaType, top.Digest}
	m.Imports = append(m.Imports, carried)
	m.mu.Unlock()

	if err := m.unpack(ctx, name, platform); err != nil {
		m.mu.Lock()
		delete(m.images, name)
		m.mu.Unlock()
		return err
	}
	return nil
}

// TestImage is a synthetic multi-platform image for tests.
type TestImage struct {
	Index, AMD64Manifest, ARM64Manifest, Attestation, Config string
	Layers                                                   []string // amd64 layers, bottom first
	Chains                                                   []string // their chainIDs
}

// AddTestImage builds an OCI index with an amd64 manifest, an arm64
// manifest and a buildx attestation manifest, stores all of it except the
// attestation (like a real pull, which skips it) and the arm64 manifest's
// children, unpacks it into snapshots, and tags it as name. Layer blobs
// double as their own uncompressed form, so a layer's diff_id is its
// digest.
func (m *MemStore) AddTestImage(name string, layers ...string) TestImage {
	var t TestImage
	var layerDescs []Descriptor
	var diffIDs []string
	for _, l := range layers {
		d := m.Put([]byte(l))
		t.Layers = append(t.Layers, d)
		diffIDs = append(diffIDs, d)
		layerDescs = append(layerDescs, Descriptor{MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Digest: d, Size: int64(len(l))})
	}
	cfg, _ := json.Marshal(map[string]interface{}{"architecture": "amd64", "os": "linux", "rootfs": map[string]interface{}{"type": "layers", "diff_ids": diffIDs}})
	t.Config = m.Put(cfg)
	t.Chains = ChainIDs(diffIDs)
	manifest, _ := json.Marshal(manifestJSON{
		SchemaVersion: 2, MediaType: MediaTypeOCIManifest,
		Config: Descriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: t.Config, Size: int64(len(cfg))},
		Layers: layerDescs,
	})
	t.AMD64Manifest = m.Put(manifest)

	armManifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"sha256:arm"},"layers":[]}`)
	t.ARM64Manifest = digestOf(armManifest) // referenced, never stored
	attestation := []byte(`{"schemaVersion":2,"attestation":true}`)
	t.Attestation = digestOf(attestation) // referenced, never stored

	index, _ := json.Marshal(indexJSON{MediaType: MediaTypeOCIIndex, Manifests: []Descriptor{
		{MediaType: MediaTypeOCIManifest, Digest: t.Attestation, Size: int64(len(attestation)),
			Platform:    &Platform{OS: "unknown", Architecture: "unknown"},
			Annotations: map[string]string{"vnd.docker.reference.type": "attestation-manifest"}},
		{MediaType: MediaTypeOCIManifest, Digest: t.ARM64Manifest, Size: int64(len(armManifest)), Platform: &Platform{OS: "linux", Architecture: "arm64"}},
		{MediaType: MediaTypeOCIManifest, Digest: t.AMD64Manifest, Size: int64(len(manifest)), Platform: &Platform{OS: "linux", Architecture: "amd64"}},
	}})
	t.Index = m.Put(index)
	m.Tag(name, MediaTypeOCIIndex, t.Index)
	if err := m.unpack(context.Background(), name, "linux/amd64"); err != nil {
		panic(err)
	}
	return t
}

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

var _ Store = (*MemStore)(nil)
