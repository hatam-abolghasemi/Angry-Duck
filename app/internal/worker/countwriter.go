package worker

import "io"

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// ReadFrom keeps the underlying writer's fast path (sendfile to a socket)
// when copying from a file.
func (c *countWriter) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := c.w.(io.ReaderFrom); ok {
		n, err := rf.ReadFrom(r)
		c.n += n
		return n, err
	}
	n, err := io.Copy(struct{ io.Writer }{c.w}, r)
	c.n += n
	return n, err
}

// limitWriter stops writing after left bytes but keeps draining, so the
// child never blocks on a full pipe; over records that the cap was hit.
