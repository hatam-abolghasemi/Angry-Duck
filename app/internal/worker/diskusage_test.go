package worker

import "testing"

func TestDiskUtilization(t *testing.T) {
	r, err := DiskUtilization(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if r.SizeBytes <= 0 || r.Utilization < 0 || r.Utilization > 1 || r.UsedBytes+r.FreeBytes != r.SizeBytes {
		t.Fatalf("odd reading: %+v", r)
	}
	if _, err := DiskUtilization("/does/not/exist"); err == nil {
		t.Fatal("no error for a missing path")
	}
}
