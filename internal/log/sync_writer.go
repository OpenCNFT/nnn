package log

import (
	"io"
	"sync"
)

type syncWriter struct {
	io.Writer
	m sync.Mutex
}

func (w *syncWriter) Write(data []byte) (int, error) {
	w.m.Lock()
	defer w.m.Unlock()

	return w.Writer.Write(data)
}

// NewSyncWriter returns Writer wrapped with a mutex that is acquired
// before each Write call.
func NewSyncWriter(w io.Writer) io.Writer {
	return &syncWriter{Writer: w}
}
