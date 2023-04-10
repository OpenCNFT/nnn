package backup

import (
	"io"
)

// LazyWriter is a WriteCloser that will call Create when on the first call to
// Write. This means it will only create a file if there will be data written
// to it.
type LazyWriter struct {
	create func() (io.WriteCloser, error)
	w      io.WriteCloser
}

// NewLazyWriter initializes a new LazyWriter. create is called on the first
// call of Write, any errors will be returned by this call.
func NewLazyWriter(create func() (io.WriteCloser, error)) *LazyWriter {
	return &LazyWriter{
		create: create,
	}
}

func (w *LazyWriter) Write(p []byte) (int, error) {
	if w.w == nil {
		var err error
		w.w, err = w.create()
		if err != nil {
			return 0, err
		}
	}

	return w.w.Write(p)
}

// Close calls Close on the WriteCloser returned by Create, passing on any
// returned error. Close must be called to properly clean up resources.
func (w *LazyWriter) Close() error {
	if w.w == nil {
		return nil
	}
	return w.w.Close()
}
