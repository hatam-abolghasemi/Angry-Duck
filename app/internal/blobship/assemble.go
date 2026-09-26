package blobship

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// Assembly is how a receiver builds an image from one or more sources that
// all hold the same image (same top-level digest), preferring blobs over
// snapshots everywhere it can:
//
//   - a layer whose snapshot the receiver already has needs nothing;
//   - otherwise its compressed blob, from the receiver itself or from ANY
//     source that has it — a blob is digest-verified on import, a
//     snapshot copy is not;
//   - only when no source has the blob, the layer's snapshot directory.
//
// A shipped snapshot has to sit on its parent's snapshot, while blobs are
// only unpacked by an import. So when layers that do have blobs lie below
// a layer that must go by snapshot, the receiver first imports a small
// synthetic "base" image made of exactly those lower layers (a Base step):
// containerd unpacks them from their blobs, and the snapshot then has its
// parent. Every step's snapshots land under the same chainIDs a normal
// pull would create, because chainIDs depend only on the layers below.
type Assembly struct {
	Primary int    // index of the plan whose Top the final import registers
	Steps   []Step // run in order, before the final import
	// Final lists, per source index, the blobs the final import needs:
	// missing metadata blobs and missing layer blobs above every snapshot.
	Final   map[int][]Descriptor
	Present int // layers the receiver already had as snapshots
	// Layer counts by how they arrive, for metrics.
	BlobLayers, SnapshotLayers int
}

// Step is one action before the final import.
type Step struct {
	// Base: import layers [0, UpTo] as a synthetic image, shipping Blobs
	// (per source index) for the ones the receiver lacks.
	Base  bool
	UpTo  int
	Blobs map[int][]Descriptor
	// Snapshot: ship Layer's snapshot directory from Source.
	Layer  Layer
	Source int
}

// SnapshotCount is how many snapshot steps there are.
func (a Assembly) SnapshotCount() int {
	n := 0
	for _, s := range a.Steps {
		if !s.Base {
			n++
		}
	}
	return n
}

// BlobCount is how many blobs travel in total, across base and final
// imports.
func (a Assembly) BlobCount() int {
	n := 0
	for _, bs := range a.Final {
		n += len(bs)
	}
	for _, s := range a.Steps {
		for _, bs := range s.Blobs {
			n += len(bs)
		}
	}
	return n
}

// Assemble works out an Assembly. plans[primary] names the image; any
// other plan is only used as a source of blobs and snapshots, and is
// ignored unless its Top digest matches the primary's (a tag that moved
// between sources is a different image).
func Assemble(plans []Plan, primary int, haveBlobs, haveSnaps map[string]bool) (Assembly, error) {
	a := Assembly{Primary: primary, Final: map[int][]Descriptor{}}
	p := plans[primary]
	usable := []int{primary}
	for i, q := range plans {
		if i != primary && q.Top.Digest == p.Top.Digest && q.Top.Digest != "" {
			usable = append(usable, i)
		}
	}
	absent := make([]map[string]bool, len(plans))
	for _, i := range usable {
		absent[i] = make(map[string]bool, len(plans[i].Absent))
		for _, d := range plans[i].Absent {
			absent[i][d] = true
		}
	}
	// blobSource: which source can serve digest, -1 if none.
	blobSource := func(d string) int {
		for _, i := range usable {
			if !absent[i][d] {
				return i
			}
		}
		return -1
	}
	add := func(m map[int][]Descriptor, src int, b Descriptor) {
		m[src] = append(m[src], b)
	}

	if len(p.Layers) == 0 {
		// A source older than 1.7.0: no layer information, so ship every
		// missing blob, from whichever source has it.
		for _, b := range p.Missing(haveBlobs) {
			src := blobSource(b.Digest)
			if src < 0 {
				return Assembly{}, fmt.Errorf("no source has blob %s", b.Digest)
			}
			add(a.Final, src, b)
		}
		return a, nil
	}

	isLayer := make(map[string]bool, len(p.Layers))
	for _, l := range p.Layers {
		isLayer[l.Blob.Digest] = true
	}
	for _, b := range p.Blobs {
		if isLayer[b.Digest] || haveBlobs[b.Digest] {
			continue
		}
		src := blobSource(b.Digest)
		if src < 0 {
			return Assembly{}, fmt.Errorf("no source has metadata blob %s", b.Digest)
		}
		add(a.Final, src, b)
	}

	// Per layer: how it arrives.
	const (
		present = iota
		local   // blob already on the receiver
		blob    // blob from a source
		snap    // snapshot from a source
	)
	how := make([]int, len(p.Layers))
	from := make([]int, len(p.Layers))
	top := -1 // highest layer that must go by snapshot
	for i, l := range p.Layers {
		switch {
		case haveSnaps[l.ChainID]:
			how[i] = present
			a.Present++
		case haveBlobs[l.Blob.Digest]:
			how[i] = local
		default:
			if src := layerBlobSource(plans, usable, i); src >= 0 {
				how[i], from[i] = blob, src
				break
			}
			src := layerSnapSource(plans, usable, i)
			if src < 0 {
				return Assembly{}, fmt.Errorf("no source has blob or snapshot for layer %d (%s)", i, l.ChainID)
			}
			how[i], from[i] = snap, src
			top = i
		}
	}

	// Walk up to the highest snapshot layer: blob layers below a snapshot
	// go through a base import first.
	pendingBase := false
	var baseBlobs map[int][]Descriptor
	for i := 0; i <= top; i++ {
		switch how[i] {
		case local:
			pendingBase = true
			a.BlobLayers++
		case blob:
			pendingBase = true
			a.BlobLayers++
			if baseBlobs == nil {
				baseBlobs = map[int][]Descriptor{}
			}
			add(baseBlobs, from[i], p.Layers[i].Blob)
		case snap:
			if pendingBase {
				a.Steps = append(a.Steps, Step{Base: true, UpTo: i - 1, Blobs: baseBlobs})
				pendingBase, baseBlobs = false, nil
			}
			a.Steps = append(a.Steps, Step{Layer: p.Layers[i], Source: from[i]})
			a.SnapshotLayers++
		}
	}
	for i := top + 1; i < len(p.Layers); i++ {
		switch how[i] {
		case local:
			a.BlobLayers++
		case blob:
			a.BlobLayers++
			add(a.Final, from[i], p.Layers[i].Blob)
		}
	}
	return a, nil
}

func layerBlobSource(plans []Plan, usable []int, layer int) int {
	for _, i := range usable {
		if layer < len(plans[i].Layers) && plans[i].Layers[layer].BlobPresent {
			return i
		}
	}
	return -1
}

func layerSnapSource(plans []Plan, usable []int, layer int) int {
	for _, i := range usable {
		if layer < len(plans[i].Layers) && plans[i].Layers[layer].SnapshotPresent {
			return i
		}
	}
	return -1
}

// BaseImage builds the synthetic image a Base step imports: an OCI
// manifest of layers [0, upTo] of plan, with a minimal config naming only
// their diff_ids. It returns the manifest descriptor (with platform) and
// the manifest and config bytes to put in the archive.
func BaseImage(plan Plan, upTo int, platform string) (top Descriptor, blobs map[string][]byte, err error) {
	want, err := ParsePlatform(platform)
	if err != nil {
		return Descriptor{}, nil, err
	}
	if upTo < 0 || upTo >= len(plan.Layers) {
		return Descriptor{}, nil, fmt.Errorf("base image: layer %d out of range", upTo)
	}
	diffIDs := make([]string, upTo+1)
	layers := make([]Descriptor, upTo+1)
	for i := 0; i <= upTo; i++ {
		diffIDs[i] = plan.Layers[i].DiffID
		layers[i] = plan.Layers[i].Blob
	}
	cfg := map[string]interface{}{
		"architecture": want.Architecture,
		"os":           want.OS,
		"rootfs":       map[string]interface{}{"type": "layers", "diff_ids": diffIDs},
	}
	if want.Variant != "" {
		cfg["variant"] = want.Variant
	}
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		return Descriptor{}, nil, err
	}
	cfgDesc := Descriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: digestOf(cfgBytes), Size: int64(len(cfgBytes))}
	manBytes, err := json.Marshal(manifestJSON{SchemaVersion: 2, MediaType: MediaTypeOCIManifest, Config: cfgDesc, Layers: layers})
	if err != nil {
		return Descriptor{}, nil, err
	}
	top = Descriptor{MediaType: MediaTypeOCIManifest, Digest: digestOf(manBytes), Size: int64(len(manBytes)), Platform: &want}
	return top, map[string][]byte{cfgDesc.Digest: cfgBytes, top.Digest: manBytes}, nil
}

// ArchiveWriter writes an OCI image layout to import, with blobs that can
// come from several places: bytes built here (a base image's manifest and
// config) and the blob entries of other archives (sources' exports),
// streamed through without buffering.
type ArchiveWriter struct {
	tw      *tar.Writer
	written map[string]bool
	dirDone bool
}

// NewArchiveWriter writes oci-layout and an index.json naming top as name.
func NewArchiveWriter(w io.Writer, top Descriptor, name string) (*ArchiveWriter, error) {
	a := &ArchiveWriter{tw: tar.NewWriter(w), written: map[string]bool{}}
	if err := a.file("oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`)); err != nil {
		return nil, err
	}
	top.Annotations = map[string]string{"io.containerd.image.name": name}
	index, err := json.Marshal(struct {
		SchemaVersion int          `json:"schemaVersion"`
		MediaType     string       `json:"mediaType"`
		Manifests     []Descriptor `json:"manifests"`
	}{2, MediaTypeOCIIndex, []Descriptor{top}})
	if err != nil {
		return nil, err
	}
	if err := a.file("index.json", index); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *ArchiveWriter) file(name string, data []byte) error {
	if err := a.tw.WriteHeader(&tar.Header{Name: name, Mode: 0o444, Size: int64(len(data)), ModTime: time.Unix(0, 0), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err := a.tw.Write(data)
	return err
}

func (a *ArchiveWriter) blobHeader(digest string, size int64) error {
	hex, ok := strings.CutPrefix(digest, "sha256:")
	if !ok {
		return fmt.Errorf("unsupported digest algorithm in %s", digest)
	}
	if !a.dirDone {
		if err := a.tw.WriteHeader(&tar.Header{Name: "blobs/sha256/", Mode: 0o555, ModTime: time.Unix(0, 0), Typeflag: tar.TypeDir}); err != nil {
			return err
		}
		a.dirDone = true
	}
	return a.tw.WriteHeader(&tar.Header{Name: "blobs/sha256/" + hex, Mode: 0o444, Size: size, ModTime: time.Unix(0, 0), Typeflag: tar.TypeReg})
}

// AddBytes adds one blob held in memory.
func (a *ArchiveWriter) AddBytes(digest string, data []byte) error {
	if a.written[digest] {
		return nil
	}
	if err := a.blobHeader(digest, int64(len(data))); err != nil {
		return err
	}
	if _, err := a.tw.Write(data); err != nil {
		return err
	}
	a.written[digest] = true
	return nil
}

// CopyBlobsFrom streams every blob entry of the archive read from r into
// this one, and fails unless exactly the digests in want arrived. The
// other archive's oci-layout and index.json are dropped.
func (a *ArchiveWriter) CopyBlobsFrom(r io.Reader, want []Descriptor) error {
	expect := make(map[string]int64, len(want))
	for _, d := range want {
		expect[d.Digest] = d.Size
	}
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading source archive: %w", err)
		}
		hex, ok := strings.CutPrefix(h.Name, "blobs/sha256/")
		if !ok || hex == "" || h.Typeflag != tar.TypeReg {
			continue
		}
		d := "sha256:" + hex
		size, ok := expect[d]
		if !ok || size != h.Size {
			return fmt.Errorf("source archive carries unexpected blob %s (%d bytes)", d, h.Size)
		}
		delete(expect, d)
		if a.written[d] {
			if _, err := io.Copy(io.Discard, tr); err != nil {
				return err
			}
			continue
		}
		if err := a.blobHeader(d, h.Size); err != nil {
			return err
		}
		if _, err := io.Copy(a.tw, tr); err != nil {
			return fmt.Errorf("copying %s: %w", d, err)
		}
		a.written[d] = true
	}
	if len(expect) > 0 {
		for d := range expect {
			return fmt.Errorf("source archive is missing blob %s", d)
		}
	}
	return nil
}

// Close ends the archive.
func (a *ArchiveWriter) Close() error { return a.tw.Close() }
