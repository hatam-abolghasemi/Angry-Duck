package blobship

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

// Snapshot wire formats for /snapshots/export.
//
// FormatOCILayer is an uncompressed OCI layer tar, as containerd's differ
// writes and applies it: whiteouts are ".wh.<name>" files and an opaque
// directory holds ".wh..wh..opq". Producing and applying it happens inside
// containerd, so the worker needs no extra capabilities.
//
// FormatOverlayDir (the empty string, for compatibility) is what workers
// before 1.8.6 send and expect: GNU tar of the overlayfs snapshot directory,
// where a whiteout is a 0/0 character device and an opaque directory
// carries the trusted.overlay.opaque=y xattr.
const (
	FormatOverlayDir = ""
	FormatOCILayer   = "oci-layer"
)

const (
	whiteoutPrefix = ".wh."
	whiteoutOpaque = ".wh..wh..opq"
	paxXattr       = "SCHILY.xattr."
	overlayXattr   = paxXattr + "trusted.overlay."
	opaqueXattr    = overlayXattr + "opaque"
)

// ErrNotConvertible means a snapshot uses an overlayfs feature with no
// equivalent on the other side of the conversion.
var ErrNotConvertible = errors.New("snapshot can't be converted between formats")

// OverlayDirToOCI converts an overlayfs snapshot directory tar (from a
// pre-1.8.6 source) into an OCI layer tar. It is a pure stream rewrite of
// tar headers: file contents pass through untouched.
func OverlayDirToOCI(r io.Reader) io.ReadCloser {
	return convert(r, func(tw *tar.Writer, h *tar.Header, body io.Reader) error {
		name := strings.TrimPrefix(h.Name, "./")
		if h.Typeflag == tar.TypeChar && h.Devmajor == 0 && h.Devminor == 0 {
			if name == "" || name == "." {
				return fmt.Errorf("%w: whiteout of the root", ErrNotConvertible)
			}
			dir, base := path.Split(strings.TrimSuffix(name, "/"))
			return tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg, Name: dir + whiteoutPrefix + base, Mode: 0o644,
				Uid: h.Uid, Gid: h.Gid, ModTime: h.ModTime, Format: tar.FormatPAX,
			})
		}
		opaque := false
		if h.PAXRecords != nil {
			if _, ok := h.PAXRecords[overlayXattr+"redirect"]; ok {
				return fmt.Errorf("%w: %s uses overlayfs redirect_dir", ErrNotConvertible, h.Name)
			}
			opaque = h.PAXRecords[opaqueXattr] == "y"
		}
		h.PAXRecords = withoutOverlayXattrs(h.PAXRecords)
		h.Xattrs = nil //nolint:staticcheck // the reader fills both; PAXRecords is what's written
		h.Format = tar.FormatPAX
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if h.Size > 0 && body != nil {
			if _, err := io.Copy(tw, body); err != nil {
				return err
			}
		}
		if opaque && h.Typeflag == tar.TypeDir {
			dir := strings.TrimSuffix(name, "/")
			if dir == "." {
				dir = ""
			}
			return tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg, Name: path.Join(dir, whiteoutOpaque), Mode: 0o644,
				Uid: h.Uid, Gid: h.Gid, ModTime: h.ModTime, Format: tar.FormatPAX,
			})
		}
		return nil
	})
}

// OCIToOverlayDir converts an OCI layer tar into the overlayfs directory
// format, for pre-1.8.6 receivers. An opaque-directory marker can't be
// expressed after its directory has been written, so a layer with one
// fails with ErrNotConvertible. containerd's walking differ, which
// produces the OCI side here, records deletions as individual whiteouts
// and doesn't emit opaque markers.
func OCIToOverlayDir(r io.Reader) io.ReadCloser {
	return convert(r, func(tw *tar.Writer, h *tar.Header, body io.Reader) error {
		dir, base := path.Split(strings.TrimSuffix(h.Name, "/"))
		switch {
		case base == whiteoutOpaque:
			return fmt.Errorf("%w: opaque directory %s for a pre-1.8.6 receiver", ErrNotConvertible, dir)
		case strings.HasPrefix(base, whiteoutPrefix):
			return tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeChar, Name: dir + strings.TrimPrefix(base, whiteoutPrefix),
				Uid: 0, Gid: 0, ModTime: h.ModTime, Format: tar.FormatPAX,
			})
		}
		h.Xattrs = nil //nolint:staticcheck
		h.Format = tar.FormatPAX
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if h.Size > 0 && body != nil {
			_, err := io.Copy(tw, body)
			return err
		}
		return nil
	})
}

// convert runs fn over every entry of the tar in r and returns the
// rewritten tar as a stream. Closing the result early stops the
// conversion.
func convert(r io.Reader, fn func(tw *tar.Writer, h *tar.Header, body io.Reader) error) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		tr := tar.NewReader(r)
		tw := tar.NewWriter(pw)
		for {
			h, err := tr.Next()
			if err == io.EOF {
				pw.CloseWithError(tw.Close())
				return
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if err := fn(tw, h, tr); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
	}()
	return pr
}

func withoutOverlayXattrs(rec map[string]string) map[string]string {
	if len(rec) == 0 {
		return rec
	}
	out := make(map[string]string, len(rec))
	for k, v := range rec {
		if !strings.HasPrefix(k, overlayXattr) {
			out[k] = v
		}
	}
	return out
}
