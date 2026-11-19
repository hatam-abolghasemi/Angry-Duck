package worker

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// DiskUtilization reads the usage of the filesystem holding path with one
// statfs call: no request, no parsing, nothing allocated. Given containerd's
// content store, it measures the disk images actually fill, which is what
// cleanup and the spread's room check care about. Free counts the blocks
// reserved for root as free, as node-exporter's node_filesystem_free_bytes
// does, so the numbers match what the dashboard used to show for "/" when
// containerd's data lives there.
func DiskUtilization(path string) (UtilizationResult, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return UtilizationResult{}, fmt.Errorf("statfs %s: %w", path, err)
	}
	size := float64(st.Blocks) * float64(st.Bsize)
	free := float64(st.Bfree) * float64(st.Bsize)
	if size == 0 {
		return UtilizationResult{}, fmt.Errorf("statfs %s: filesystem size is zero", path)
	}
	used := size - free
	return UtilizationResult{SizeBytes: size, FreeBytes: free, UsedBytes: used, Utilization: used / size}, nil
}
