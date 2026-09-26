// Package blobship ships one image's blobs from a node that has them to a
// node that doesn't, sending only the blobs the receiver is missing.
//
// It works in two steps, both driven by the receiving side:
//
//  1. Plan: the source walks the image the same way containerd does on a
//     pull — top-level object (index or manifest) -> the manifest for the
//     receiver's platform -> config + layers — and returns every blob of
//     that walk with its digest and size.
//  2. Export: the receiver drops the digests it already has and asks the
//     source for the rest. The source streams them back as a partial OCI
//     image layout tar (oci-layout + index.json + only those blobs), which
//     the receiver pipes straight into `ctr images import`.
//
// A partial archive is enough because `ctr images import --platform X`
// only walks the children it needs for X and takes any blob the local
// content store already has from there. That was checked by hand on a
// real node before this was written: an archive missing three layers the
// receiver already had, and the buildx attestation manifest nobody had,
// imported and unpacked fine.
//
// containerd does all the verifying: every blob is committed under its
// expected digest and size, so a corrupted or truncated transfer fails
// the import instead of landing a bad image.
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
	"time"
)

// Media types the walk understands. Docker schema2 and OCI are handled the
// same way; Docker schema1 (long deprecated) is refused.
const (
	MediaTypeOCIIndex       = "application/vnd.oci.image.index.v1+json"
	MediaTypeOCIManifest    = "application/vnd.oci.image.manifest.v1+json"
	MediaTypeDockerList     = "application/vnd.docker.distribution.manifest.list.v2+json"
	MediaTypeDockerManifest = "application/vnd.docker.distribution.manifest.v2+json"
)

// maxMetadataBlob caps how much of an index, manifest or config blob is
// read into memory. Real ones are a few KB; this only stops a malformed
// store from growing the worker's memory.
const maxMetadataBlob = 4 << 20

// Descriptor points at one blob, as in the OCI spec.
type Descriptor struct {
	MediaType   string            `json:"mediaType,omitempty"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Platform    *Platform         `json:"platform,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	URLs        []string          `json:"urls,omitempty"`
}

// Platform is the platform object of an index entry.
type Platform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant,omitempty"`
}

// Plan is every blob of one image for one platform, top-level object
// first. Only Blobs are ever shipped; Top is repeated as Blobs[0].
//
// Absent lists blobs of the plan the source itself doesn't have. That is
// normal: containerd skips downloading a layer whose unpacked snapshot
// already exists, so a node can run an image while lacking some of its
// layer blobs. Such a source can still serve every receiver that already
// has those blobs.
type Plan struct {
	Image    string       `json:"image"`
	Platform string       `json:"platform"`
	Top      Descriptor   `json:"top"`
	Blobs    []Descriptor `json:"blobs"`
	Absent   []string     `json:"absent,omitempty"`
	// Layers describes each layer bottom-first, with the snapshot name
	// (chainID) containerd unpacks it under. Sources from 1.7.0 fill it;
	// older ones leave it empty, and receivers then fall back to shipping
	// every missing blob.
	Layers []Layer `json:"layers,omitempty"`
}

// Layer is one image layer, both as a blob and as a snapshot.
type Layer struct {
	Blob    Descriptor `json:"blob"`
	DiffID  string     `json:"diff_id"`  // digest of the uncompressed layer tar
	ChainID string     `json:"chain_id"` // snapshot key: this layer plus every layer below it
	// Filled in by the source when it answers a plan request.
	BlobPresent     bool `json:"blob_present"`
	SnapshotPresent bool `json:"snapshot_present"`
}

// Size is the total size of every blob in the plan.
func (p Plan) Size() int64 {
	var n int64
	for _, b := range p.Blobs {
		n += b.Size
	}
	return n
}

// Unservable returns the digests among need that the source lacks.
func (p Plan) Unservable(need []Descriptor) []string {
	absent := make(map[string]bool, len(p.Absent))
	for _, d := range p.Absent {
		absent[d] = true
	}
	var out []string
	for _, b := range need {
		if absent[b.Digest] {
			out = append(out, b.Digest)
		}
	}
	return out
}

// Missing returns the blobs of the plan that are not in have.
func (p Plan) Missing(have map[string]bool) []Descriptor {
	var out []Descriptor
	for _, b := range p.Blobs {
		if !have[b.Digest] {
			out = append(out, b)
		}
	}
	return out
}

// Store is the node's containerd content store, as far as shipping needs
// it. The worker implements it with the node's own `ctr`.
type Store interface {
	// Resolve returns the media type and digest the image name points at.
	Resolve(ctx context.Context, image string) (mediaType, digest string, err error)
	// ReadBlob returns a whole blob. Only used for small metadata blobs.
	ReadBlob(ctx context.Context, digest string, max int64) ([]byte, error)
	// StreamBlob writes exactly size bytes of a blob to w, or fails.
	StreamBlob(ctx context.Context, digest string, size int64, w io.Writer) error
	// Digests lists every blob digest in the content store.
	Digests(ctx context.Context) (map[string]bool, error)
	// Import imports an OCI archive read from r. Unpacking skips every
	// layer whose chainID snapshot already exists, so it needs no blob
	// for those layers.
	Import(ctx context.Context, r io.Reader, platform string) error

	// Snapshots lists every snapshot key.
	Snapshots(ctx context.Context) (map[string]bool, error)
	// SnapshotDirs returns the on-disk directories of chainID and of every
	// snapshot below it, bottom first; depth is how many to expect.
	SnapshotDirs(ctx context.Context, chainID string, depth int) ([]string, error)
	// ExportSnapshot writes a tar of one snapshot directory to w,
	// preserving ownership, device files and extended attributes.
	ExportSnapshot(ctx context.Context, dir string, w io.Writer) error
	// ApplySnapshot creates snapshot chainID on top of parent ("" for the
	// bottom layer) from a tar read from r. verify runs after the tar is
	// unpacked and before the commit; an error from it discards the
	// snapshot. The committed snapshot stays pinned against garbage
	// collection until Unpin.
	ApplySnapshot(ctx context.Context, chainID, parent string, r io.Reader, verify func() error) error
	// Unpin removes ApplySnapshot's garbage-collection pin.
	Unpin(ctx context.Context, chainID string) error
	// RemoveSnapshot deletes a snapshot by key. Used to clean up temporary
	// snapshots a crashed worker left behind.
	RemoveSnapshot(ctx context.Context, key string) error
}

// ParsePlatform parses "os/arch[/variant]".
func ParsePlatform(s string) (Platform, error) {
	parts := strings.Split(s, "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] == "" || parts[1] == "" {
		return Platform{}, fmt.Errorf("invalid platform %q, want os/arch[/variant]", s)
	}
	p := Platform{OS: parts[0], Architecture: parts[1]}
	if len(parts) == 3 {
		p.Variant = parts[2]
	}
	return p, nil
}

func (p Platform) matches(want Platform) bool {
	if p.OS != want.OS || p.Architecture != want.Architecture {
		return false
	}
	// Only compare variants when both sides name one. An entry with no
	// variant matches any request, and a request with no variant accepts
	// any entry; containerd's own matcher is laxer still for arm.
	return want.Variant == "" || p.Variant == "" || p.Variant == want.Variant
}

type indexJSON struct {
	MediaType string       `json:"mediaType"`
	Manifests []Descriptor `json:"manifests"`
}

type manifestJSON struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	Config        Descriptor   `json:"config"`
	Layers        []Descriptor `json:"layers"`
}

// BuildPlan walks image in store and lists the blobs a node of platform
// needs to run it.
func BuildPlan(ctx context.Context, store Store, image, platform string) (Plan, error) {
	want, err := ParsePlatform(platform)
	if err != nil {
		return Plan{}, err
	}
	mediaType, digest, err := store.Resolve(ctx, image)
	if err != nil {
		return Plan{}, err
	}
	topBytes, err := store.ReadBlob(ctx, digest, maxMetadataBlob)
	if err != nil {
		return Plan{}, fmt.Errorf("reading %s: %w", digest, err)
	}
	top := Descriptor{MediaType: mediaType, Digest: digest, Size: int64(len(topBytes))}
	plan := Plan{Image: image, Platform: platform, Top: top, Blobs: []Descriptor{top}}

	manifestBytes := topBytes
	switch mediaType {
	case MediaTypeOCIIndex, MediaTypeDockerList:
		var idx indexJSON
		if err := json.Unmarshal(topBytes, &idx); err != nil {
			return Plan{}, fmt.Errorf("parsing index %s: %w", digest, err)
		}
		chosen, err := pickManifest(idx.Manifests, want)
		if err != nil {
			return Plan{}, fmt.Errorf("image %s: %w", image, err)
		}
		manifestBytes, err = store.ReadBlob(ctx, chosen.Digest, maxMetadataBlob)
		if err != nil {
			return Plan{}, fmt.Errorf("reading manifest %s: %w", chosen.Digest, err)
		}
		if int64(len(manifestBytes)) != chosen.Size {
			return Plan{}, fmt.Errorf("manifest %s: store has %d bytes, index says %d", chosen.Digest, len(manifestBytes), chosen.Size)
		}
		plan.Blobs = append(plan.Blobs, stripDescriptor(chosen))
	case MediaTypeOCIManifest, MediaTypeDockerManifest:
		// A single-platform image: the top-level object is the manifest.
	default:
		return Plan{}, fmt.Errorf("image %s: unsupported media type %q", image, mediaType)
	}

	var m manifestJSON
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		return Plan{}, fmt.Errorf("parsing manifest: %w", err)
	}
	plan.Layers = layersOf(ctx, store, m)
	if m.SchemaVersion != 2 {
		return Plan{}, fmt.Errorf("image %s: manifest schemaVersion %d is not supported", image, m.SchemaVersion)
	}
	if m.Config.Digest == "" {
		return Plan{}, fmt.Errorf("image %s: manifest has no config", image)
	}
	plan.Blobs = append(plan.Blobs, stripDescriptor(m.Config))
	for _, l := range m.Layers {
		// Foreign / non-distributable layers (URLs set) are never in the
		// content store; containerd skips them on pull, so the receiver
		// doesn't need them either.
		if len(l.URLs) > 0 {
			continue
		}
		plan.Blobs = append(plan.Blobs, stripDescriptor(l))
	}
	plan.Blobs = dedupe(plan.Blobs)
	return plan, nil
}

// layersOf pairs each manifest layer with its diff_id and chainID from the
// config. It returns nil (blob-only shipping) when that can't be done
// reliably: an unreadable config, a count mismatch, or foreign layers.
func layersOf(ctx context.Context, store Store, m manifestJSON) []Layer {
	cfgBytes, err := store.ReadBlob(ctx, m.Config.Digest, maxMetadataBlob)
	if err != nil {
		return nil
	}
	var cfg struct {
		RootFS struct {
			DiffIDs []string `json:"diff_ids"`
		} `json:"rootfs"`
	}
	if json.Unmarshal(cfgBytes, &cfg) != nil || len(m.Layers) == 0 || len(cfg.RootFS.DiffIDs) != len(m.Layers) {
		return nil
	}
	chains := ChainIDs(cfg.RootFS.DiffIDs)
	layers := make([]Layer, len(m.Layers))
	for i, l := range m.Layers {
		if len(l.URLs) > 0 {
			return nil
		}
		layers[i] = Layer{Blob: stripDescriptor(l), DiffID: cfg.RootFS.DiffIDs[i], ChainID: chains[i]}
	}
	return layers
}

// ChainIDs computes containerd's snapshot keys for a layer stack, bottom
// first: the first is the first diff_id, and each next one is
// sha256(previous chainID + " " + diff_id).
func ChainIDs(diffIDs []string) []string {
	out := make([]string, len(diffIDs))
	for i, d := range diffIDs {
		if i == 0 {
			out[i] = d
			continue
		}
		sum := sha256.Sum256([]byte(out[i-1] + " " + d))
		out[i] = "sha256:" + hex.EncodeToString(sum[:])
	}
	return out
}

// Decision is what a receiver will fetch from one source.
type Decision struct {
	// Snapshots to ship, bottom first. Each one's parent is the layer
	// right below it, which already exists or comes earlier in this list.
	Snapshots []Layer
	// Blobs to ship in the import archive: missing metadata blobs, plus
	// missing layer blobs for layers above every shipped snapshot.
	Blobs []Descriptor
	// Present counts layers the receiver already has as snapshots.
	Present int
}

// Decide works out, layer by layer, the cheapest way for a receiver to
// end up with plan's image, the same way containerd's own pull does:
//
//   - a layer whose chainID snapshot the receiver has needs nothing;
//   - otherwise its compressed blob, if the receiver or the source has it
//     (import applies it and verifies it against its digest);
//   - otherwise the source's snapshot, shipped as a directory tar.
//
// A snapshot can only be created on top of its parent, while blobs are
// applied later during import. So once some layer must go by snapshot,
// every missing layer below it goes by snapshot too.
//
// It returns an error when this source can't provide something the
// receiver needs.
func Decide(plan Plan, haveBlobs, haveSnaps map[string]bool) (Decision, error) {
	var d Decision
	if len(plan.Layers) == 0 {
		// A source older than 1.7.0: no layer information, so ship every
		// missing blob.
		d.Blobs = plan.Missing(haveBlobs)
		if gaps := plan.Unservable(d.Blobs); len(gaps) > 0 {
			return Decision{}, fmt.Errorf("source lacks %d blob(s) this node needs, first %s", len(gaps), gaps[0])
		}
		return d, nil
	}

	isLayer := make(map[string]bool, len(plan.Layers))
	for _, l := range plan.Layers {
		isLayer[l.Blob.Digest] = true
	}
	absent := make(map[string]bool, len(plan.Absent))
	for _, a := range plan.Absent {
		absent[a] = true
	}
	// Metadata (index, manifest, config) always travels as blobs.
	for _, b := range plan.Blobs {
		if isLayer[b.Digest] || haveBlobs[b.Digest] {
			continue
		}
		if absent[b.Digest] {
			return Decision{}, fmt.Errorf("source lacks metadata blob %s", b.Digest)
		}
		d.Blobs = append(d.Blobs, b)
	}

	// The highest missing layer no blob is available for.
	top := -1
	for i, l := range plan.Layers {
		if !haveSnaps[l.ChainID] && !haveBlobs[l.Blob.Digest] && !l.BlobPresent {
			top = i
		}
	}
	for i, l := range plan.Layers {
		switch {
		case haveSnaps[l.ChainID]:
			d.Present++
		case i <= top:
			if !l.SnapshotPresent {
				return Decision{}, fmt.Errorf("source has neither blob nor snapshot for layer %d (%s)", i, l.ChainID)
			}
			d.Snapshots = append(d.Snapshots, l)
		case !haveBlobs[l.Blob.Digest]:
			d.Blobs = append(d.Blobs, l.Blob)
		}
	}
	return d, nil
}

// pickManifest returns the index entry for want, never an attestation.
func pickManifest(entries []Descriptor, want Platform) (Descriptor, error) {
	for _, e := range entries {
		if e.Platform == nil {
			continue
		}
		if e.Annotations["vnd.docker.reference.type"] == "attestation-manifest" {
			continue
		}
		if e.Platform.matches(want) {
			return e, nil
		}
	}
	return Descriptor{}, fmt.Errorf("no manifest for platform %s/%s", want.OS, want.Architecture)
}

func stripDescriptor(d Descriptor) Descriptor {
	return Descriptor{MediaType: d.MediaType, Digest: d.Digest, Size: d.Size}
}

// dedupe keeps the first occurrence of each digest. The same layer can be
// listed twice in one manifest (two identical Dockerfile steps).
func dedupe(in []Descriptor) []Descriptor {
	seen := make(map[string]bool, len(in))
	out := in[:0]
	for _, d := range in {
		if seen[d.Digest] {
			continue
		}
		seen[d.Digest] = true
		out = append(out, d)
	}
	return out
}

// Select returns the blobs of plan named in digests, in plan order. It
// refuses any digest that isn't part of the plan, so an export request
// can only ever read blobs of the image it names.
func Select(plan Plan, digests []string) ([]Descriptor, error) {
	inPlan := make(map[string]Descriptor, len(plan.Blobs))
	for _, b := range plan.Blobs {
		inPlan[b.Digest] = b
	}
	wanted := make(map[string]bool, len(digests))
	for _, d := range digests {
		if _, ok := inPlan[d]; !ok {
			return nil, fmt.Errorf("digest %s is not part of %s for %s", d, plan.Image, plan.Platform)
		}
		wanted[d] = true
	}
	var out []Descriptor
	for _, b := range plan.Blobs {
		if wanted[b.Digest] {
			out = append(out, b)
		}
	}
	return out, nil
}

// WriteArchive writes a partial OCI image layout to w: oci-layout,
// index.json naming plan.Top as plan.Image, then blobs. Blob sizes come
// from the plan, and StreamBlob must deliver exactly that many bytes, so
// a blob that changed or shrank under us fails the archive rather than
// producing a tar that lies.
//
// An empty blobs list is valid: the receiver already has every blob and
// only lacks the image name, which index.json alone provides.
func WriteArchive(ctx context.Context, w io.Writer, store Store, plan Plan, blobs []Descriptor) error {
	tw := tar.NewWriter(w)
	modTime := time.Unix(0, 0)

	writeFile := func(name string, data []byte) error {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o444, Size: int64(len(data)), ModTime: modTime, Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		_, err := tw.Write(data)
		return err
	}

	if err := writeFile("oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`)); err != nil {
		return err
	}
	top := plan.Top
	top.Annotations = map[string]string{
		// ctr import names the image from this annotation. Without it the
		// blobs would land but kubelet would still not find the image.
		"io.containerd.image.name": plan.Image,
	}
	index, err := json.Marshal(struct {
		SchemaVersion int          `json:"schemaVersion"`
		MediaType     string       `json:"mediaType"`
		Manifests     []Descriptor `json:"manifests"`
	}{2, MediaTypeOCIIndex, []Descriptor{top}})
	if err != nil {
		return err
	}
	if err := writeFile("index.json", index); err != nil {
		return err
	}
	if len(blobs) > 0 {
		if err := tw.WriteHeader(&tar.Header{Name: "blobs/sha256/", Mode: 0o555, ModTime: modTime, Typeflag: tar.TypeDir}); err != nil {
			return err
		}
	}
	for _, b := range blobs {
		hex, ok := strings.CutPrefix(b.Digest, "sha256:")
		if !ok {
			return fmt.Errorf("unsupported digest algorithm in %s", b.Digest)
		}
		if err := tw.WriteHeader(&tar.Header{Name: "blobs/sha256/" + hex, Mode: 0o444, Size: b.Size, ModTime: modTime, Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		if err := store.StreamBlob(ctx, b.Digest, b.Size, tw); err != nil {
			return fmt.Errorf("streaming %s: %w", b.Digest, err)
		}
	}
	return tw.Close()
}
