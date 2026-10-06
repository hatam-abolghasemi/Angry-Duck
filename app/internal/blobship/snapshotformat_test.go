package blobship

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"testing"
)

type entry struct {
	name  string
	typ   byte
	body  string
	xattr map[string]string
}

func mkTar(t *testing.T, es ...entry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range es {
		h := &tar.Header{Name: e.name, Typeflag: e.typ, Mode: 0o755, Size: int64(len(e.body)), Uid: 1000, Gid: 1000, Format: tar.FormatPAX}
		for k, v := range e.xattr {
			if h.PAXRecords == nil {
				h.PAXRecords = map[string]string{}
			}
			h.PAXRecords[paxXattr+k] = v
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(e.body)); err != nil {
			t.Fatal(err)
		}
	}
	tw.Close()
	return buf.Bytes()
}

type got struct {
	typ   byte
	body  string
	xattr map[string]string
	uid   int
}

func readTar(t *testing.T, r io.Reader) (map[string]got, []string, error) {
	t.Helper()
	out := map[string]got{}
	var order []string
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return out, order, nil
		}
		if err != nil {
			return out, order, err
		}
		b, _ := io.ReadAll(tr)
		x := map[string]string{}
		for k, v := range h.PAXRecords {
			if len(k) > len(paxXattr) && k[:len(paxXattr)] == paxXattr {
				x[k[len(paxXattr):]] = v
			}
		}
		out[h.Name] = got{h.Typeflag, string(b), x, h.Uid}
		order = append(order, h.Name)
	}
}

func TestOverlayDirToOCI(t *testing.T) {
	in := mkTar(t,
		entry{name: "./", typ: tar.TypeDir},
		entry{name: "./etc/", typ: tar.TypeDir, xattr: map[string]string{"trusted.overlay.opaque": "y", "user.keep": "1"}},
		entry{name: "./etc/app.conf", typ: tar.TypeReg, body: "x=1", xattr: map[string]string{"security.capability": "cap"}},
		entry{name: "./usr/old-binary", typ: tar.TypeChar},
	)
	m, order, err := readTar(t, OverlayDirToOCI(bytes.NewReader(in)))
	if err != nil {
		t.Fatal(err)
	}
	if e := m["./etc/"]; e.typ != tar.TypeDir || e.xattr["trusted.overlay.opaque"] != "" || e.xattr["user.keep"] != "1" {
		t.Errorf("opaque dir header = %+v", e)
	}
	if e, ok := m["etc/.wh..wh..opq"]; !ok || e.typ != tar.TypeReg {
		t.Errorf("missing opaque marker, got %v", order)
	}
	if e := m["./etc/app.conf"]; e.body != "x=1" || e.xattr["security.capability"] != "cap" || e.uid != 1000 {
		t.Errorf("regular file = %+v", e)
	}
	if e, ok := m["usr/.wh.old-binary"]; !ok || e.typ != tar.TypeReg {
		t.Errorf("char-device whiteout not converted, got %v", order)
	}
	if _, ok := m["./usr/old-binary"]; ok {
		t.Error("char device still present")
	}
}

func TestOverlayDirToOCI_RejectsRedirect(t *testing.T) {
	in := mkTar(t, entry{name: "./d/", typ: tar.TypeDir, xattr: map[string]string{"trusted.overlay.redirect": "/x"}})
	_, _, err := readTar(t, OverlayDirToOCI(bytes.NewReader(in)))
	if !errors.Is(err, ErrNotConvertible) {
		t.Fatalf("err = %v", err)
	}
}

func TestOCIToOverlayDir(t *testing.T) {
	in := mkTar(t,
		entry{name: "bin/", typ: tar.TypeDir},
		entry{name: "bin/tool", typ: tar.TypeReg, body: "elf"},
		entry{name: "bin/.wh.gone", typ: tar.TypeReg},
	)
	m, order, err := readTar(t, OCIToOverlayDir(bytes.NewReader(in)))
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := m["bin/gone"]; !ok || e.typ != tar.TypeChar || e.uid != 0 {
		t.Errorf("whiteout not converted to a char device: %v", order)
	}
	if m["bin/tool"].body != "elf" {
		t.Errorf("body lost: %+v", m["bin/tool"])
	}
}

func TestOCIToOverlayDir_RejectsOpaque(t *testing.T) {
	in := mkTar(t, entry{name: "d/", typ: tar.TypeDir}, entry{name: "d/.wh..wh..opq", typ: tar.TypeReg})
	_, _, err := readTar(t, OCIToOverlayDir(bytes.NewReader(in)))
	if !errors.Is(err, ErrNotConvertible) {
		t.Fatalf("err = %v", err)
	}
}

// Overlay → OCI → overlay keeps whiteouts and contents.
func TestSnapshotFormatsRoundTrip(t *testing.T) {
	in := mkTar(t,
		entry{name: "./a", typ: tar.TypeReg, body: "hello"},
		entry{name: "./b", typ: tar.TypeChar},
	)
	m, _, err := readTar(t, OCIToOverlayDir(OverlayDirToOCI(bytes.NewReader(in))))
	if err != nil {
		t.Fatal(err)
	}
	if m["./a"].body != "hello" || m["b"].typ != tar.TypeChar {
		t.Fatalf("round trip = %+v", m)
	}
}
