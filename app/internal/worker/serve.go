package worker

import (
	"io"
	"net/http"
	"strconv"
	"sync"
)

// serveBody answers with a blob of a known size: Content-Length first, so
// the response isn't chunked, then the body handed straight to the
// ResponseWriter's ReadFrom. A blob file then leaves by sendfile, never
// passing through userspace (io.Copy would prefer the file's own WriteTo
// and lose that). It returns the bytes sent.
func serveBody(w http.ResponseWriter, body io.Reader, size int64) (int64, error) {
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	if rf, ok := w.(io.ReaderFrom); ok {
		return rf.ReadFrom(fileOf(body))
	}
	return io.Copy(w, body)
}

// relayBuffer is the copy size for relaying a body from one socket to
// another (the mirror passing a peer's blob on to containerd). io.Copy
// relays them 32 KiB at a time: a read and a write call for each, the
// largest share of the mirror's CPU. Reads still return what has arrived;
// writes then go out in up to 256 KiB.
const relayBuffer = 256 << 10

var relayBuffers = sync.Pool{New: func() any {
	b := make([]byte, relayBuffer)
	return &b
}}

// relay copies r to w through a pooled relayBuffer. Neither side's own
// ReadFrom or WriteTo is used: between two sockets they would fall back
// to io.Copy's 32 KiB buffer.
func relay(w io.Writer, r io.Reader) (int64, error) {
	bp := relayBuffers.Get().(*[]byte)
	defer relayBuffers.Put(bp)
	return io.CopyBuffer(struct{ io.Writer }{w}, struct{ io.Reader }{r}, *bp)
}
