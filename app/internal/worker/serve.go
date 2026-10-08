package worker

import (
	"io"
	"net/http"
	"strconv"
)

// serveBody answers with a blob of a known size: Content-Length first, so
// the response isn't chunked, then the body handed straight to the
// ResponseWriter's ReadFrom. A blob file then leaves by sendfile, never
// passing through userspace (io.Copy would prefer the file's own WriteTo
// and lose that). It returns the bytes sent.
func serveBody(w http.ResponseWriter, body io.Reader, size int64) (int64, error) {
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	if rf, ok := w.(io.ReaderFrom); ok {
		return rf.ReadFrom(body)
	}
	return io.Copy(w, body)
}
