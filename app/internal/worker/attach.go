package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/continuity/fs"
	"golang.org/x/sys/unix"
)

// attachExtracted commits a layer the worker unpacked on its own
// (ExtractLayerStream) onto the layer below, on any containerd: it prepares
// a snapshot on the layer below, moves the unpacked files into it (renames
// within containerd's snapshots directory: instant, whatever their size),
// gives the directories the layer only implies the owner, mode and xattrs
// they have in the layers below, as containerd's unpack onto them would
// have, then commits it under the chainID and removes the extracted
// snapshot. errParentMissing (nothing changed) when the layer below isn't
// there yet; applied false when the layer was unpacked meanwhile.
//
// The files were checked when they were unpacked (their tar hashes to the
// diffID the chainID names); moving them doesn't touch their content.
func (s *ContainerdStore) attachExtracted(ctx context.Context, lease string, l LayerSpec, x ExtractedLayer) (applied bool, err error) {
	ctx = leases.WithLease(s.ns(ctx), lease)
	sn := s.snapshotterSvc()
	defer s.invalidate()
	if _, err := sn.Stat(ctx, l.ChainID); err == nil {
		removeQuietly(ctx, sn, x.Key)
		return false, nil
	} else if !errdefs.IsNotFound(err) {
		return false, err
	}
	srcMounts, err := sn.Mounts(ctx, x.Key)
	if err != nil {
		return false, fmt.Errorf("finding extracted snapshot %s: %w", x.Key, err)
	}
	src, _, err := writableRoot(srcMounts)
	if err != nil {
		return false, err
	}
	key := "angryduck-attach-" + randomSuffix()
	mounts, err := sn.Prepare(ctx, key, l.Parent)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return false, errParentMissing
		}
		return false, fmt.Errorf("preparing a snapshot on %q: %w", l.Parent, err)
	}
	committed := false
	defer func() {
		if !committed {
			removeQuietly(ctx, sn, key)
		}
	}()
	dst, parents, err := writableRoot(mounts)
	if err != nil {
		return false, err
	}
	if err := moveTree(src, dst, x.RootEntry); err != nil {
		return false, fmt.Errorf("attaching %s onto %q: %w", l.ChainID, l.Parent, err)
	}
	if err := fixImplied(dst, parents, x.Implied); err != nil {
		return false, fmt.Errorf("attaching %s onto %q: %w", l.ChainID, l.Parent, err)
	}
	if err := sn.Commit(ctx, l.ChainID, key); err != nil {
		if errdefs.IsAlreadyExists(err) {
			removeQuietly(ctx, sn, x.Key)
			return false, nil
		}
		return false, fmt.Errorf("committing %s: %w", l.ChainID, err)
	}
	committed = true
	removeQuietly(ctx, sn, x.Key)
	s.labelUncompressed(ctx, l, x.DiffID)
	return true, nil
}

// moveTree moves every entry of src's top directory into dst, an empty
// directory on the same filesystem. The root directory itself isn't moved:
// its overlay attributes (an opaque marker: trusted.overlay.*) always come
// along, and its owner, mode and other xattrs too when the layer had an
// entry for it.
func moveTree(src, dst string, rootEntry bool) error {
	ents, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range ents {
		err := unix.Renameat2(unix.AT_FDCWD, filepath.Join(src, e.Name()), unix.AT_FDCWD, filepath.Join(dst, e.Name()), unix.RENAME_NOREPLACE)
		if err != nil {
			return fmt.Errorf("moving %s: %w", e.Name(), err)
		}
	}
	if rootEntry {
		fi, err := os.Lstat(src)
		if err != nil {
			return err
		}
		if err := copyOwnerMode(fi, dst); err != nil {
			return err
		}
	}
	return copyXattrs(src, dst, true, rootEntry)
}

// fixImplied gives each directory the layer only implies the owner, mode
// and xattrs of the same path in the layers below (top first), as
// containerd's unpack does when it creates them on the way (archive's
// mkparent): the first layer where the path exists (symlinks followed
// inside that layer) decides; if it isn't a directory there, or no layer
// has it, the default (root, 0755) stays.
// Times aren't copied: containerd's are those of the last write into the
// directory, as are these.
func fixImplied(dst string, parents, implied []string) error {
	for _, dir := range implied {
		p := filepath.Join(dst, dir)
		if fi, err := os.Lstat(p); err != nil || !fi.IsDir() {
			continue // not created (a skipped entry), or replaced
		}
		for _, parent := range parents {
			// Resolved inside that layer, symlinks followed, as
			// containerd does: a directory implied under a path that is a
			// symlink below takes the attributes of where it points.
			pp, err := fs.RootPath(parent, dir)
			if err != nil {
				return err
			}
			fi, err := os.Lstat(pp)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if fi.IsDir() {
				if err := copyOwnerMode(fi, p); err != nil {
					return err
				}
				if err := copyXattrs(pp, p, false, true); err != nil {
					return err
				}
			}
			break
		}
	}
	return nil
}

// copyOwnerMode gives path fi's owner and mode (owner first: chown clears
// setuid and setgid bits).
func copyOwnerMode(fi os.FileInfo, path string) error {
	st := fi.Sys().(*syscall.Stat_t)
	if err := os.Lchown(path, int(st.Uid), int(st.Gid)); err != nil {
		return err
	}
	return os.Chmod(path, fi.Mode())
}

// copyXattrs copies src's xattrs onto dst: overlay's own (trusted.overlay.*,
// an opaque marker) with overlay, the others with others, except any other
// trusted.*, which containerd's copy-up skips too.
func copyXattrs(src, dst string, overlay, others bool) error {
	keys, err := listXattrs(src)
	if err != nil {
		return err
	}
	for _, k := range keys {
		isOverlay := strings.HasPrefix(k, "trusted.overlay.")
		if isOverlay && !overlay || !isOverlay && (!others || strings.HasPrefix(k, "trusted.")) {
			continue
		}
		v, err := getXattr(src, k)
		if err != nil {
			if errors.Is(err, unix.ENODATA) {
				continue
			}
			return fmt.Errorf("reading xattr %s of %s: %w", k, src, err)
		}
		if err := unix.Lsetxattr(dst, k, v, 0); err != nil {
			return fmt.Errorf("setting xattr %s on %s: %w", k, dst, err)
		}
	}
	return nil
}

func listXattrs(path string) ([]string, error) {
	n, err := unix.Llistxattr(path, nil)
	if err != nil {
		if errors.Is(err, unix.ENOTSUP) {
			return nil, nil
		}
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	n, err = unix.Llistxattr(path, buf)
	if err != nil {
		return nil, err
	}
	var keys []string
	for _, k := range strings.Split(string(buf[:n]), "\x00") {
		if k != "" {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func getXattr(path, key string) ([]byte, error) {
	n, err := unix.Lgetxattr(path, key, nil)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, n)
	n, err = unix.Lgetxattr(path, key, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
