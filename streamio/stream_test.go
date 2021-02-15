package streamio

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestReceiveSources(t *testing.T) {
	testData := "Hello this is the test data that will be received"
	testCases := []struct {
		desc string
		r    io.Reader
	}{
		{desc: "base", r: strings.NewReader(testData)},
		{desc: "dataerr", r: iotest.DataErrReader(strings.NewReader(testData))},
		{desc: "onebyte", r: iotest.OneByteReader(strings.NewReader(testData))},
		{desc: "dataerr(onebyte)", r: iotest.DataErrReader(iotest.OneByteReader(strings.NewReader(testData)))},
	}

	for _, tc := range testCases {
		data, err := ioutil.ReadAll(&opaqueReader{NewReader(receiverFromReader(tc.r))})
		require.NoError(t, err, tc.desc)
		require.Equal(t, testData, string(data), tc.desc)
	}
}

func TestReadSizes(t *testing.T) {
	readSizes := func(t *testing.T, newReader func(string) io.Reader) {
		testData := "Hello this is the test data that will be received. It goes on for a while bla bla bla."

		for n := 1; n < 100; n *= 3 {
			desc := fmt.Sprintf("reads of size %d", n)
			result := &bytes.Buffer{}
			reader := &opaqueReader{NewReader(receiverFromReader(newReader(testData)))}
			n, err := io.CopyBuffer(&opaqueWriter{result}, reader, make([]byte, n))

			require.NoError(t, err, desc)
			require.Equal(t, testData, result.String())
			require.EqualValues(t, len(testData), n)
		}
	}

	t.Run("normal reader", func(t *testing.T) {
		readSizes(t, func(s string) io.Reader{
			return strings.NewReader(s)
		})
	})

	t.Run("err reader", func(t *testing.T) {
		readSizes(t, func(s string) io.Reader{
			return iotest.DataErrReader(strings.NewReader(s))
		})
	})
}

func TestWriterTo(t *testing.T) {
	testData := "Hello this is the test data that will be received. It goes on for a while bla bla bla."
	testCases := []struct {
		desc string
		r    io.Reader
	}{
		{desc: "base", r: strings.NewReader(testData)},
		{desc: "dataerr", r: iotest.DataErrReader(strings.NewReader(testData))},
		{desc: "onebyte", r: iotest.OneByteReader(strings.NewReader(testData))},
		{desc: "dataerr(onebyte)", r: iotest.DataErrReader(iotest.OneByteReader(strings.NewReader(testData)))},
	}

	for _, tc := range testCases {
		result := &bytes.Buffer{}
		reader := NewReader(receiverFromReader(tc.r))
		n, err := reader.(io.WriterTo).WriteTo(result)

		require.NoError(t, err, tc.desc)
		require.Equal(t, int64(len(testData)), n, tc.desc)
		require.Equal(t, testData, result.String(), tc.desc)
	}
}

func receiverFromReader(r io.Reader) func() ([]byte, error) {
	return func() ([]byte, error) {
		data := make([]byte, 10)
		n, err := r.Read(data)
		return data[:n], err
	}
}

// Hide io.WriteTo if it exists
type opaqueReader struct {
	io.Reader
}

// Hide io.ReadFrom if it exists
type opaqueWriter struct {
	io.Writer
}

func TestWriterChunking(t *testing.T) {
	defer func(oldBufferSize int) {
		WriteBufferSize = oldBufferSize
	}(WriteBufferSize)
	WriteBufferSize = 5

	testData := "Hello this is some test data"
	ts := &testSender{}
	w := NewWriter(ts.send)
	_, err := io.CopyBuffer(&opaqueWriter{w}, strings.NewReader(testData), make([]byte, 10))

	require.NoError(t, err)
	require.Equal(t, testData, string(bytes.Join(ts.sends, nil)))
	for _, send := range ts.sends {
		require.True(t, len(send) <= WriteBufferSize, "send calls may not exceed WriteBufferSize")
	}
}

func TestNewSyncWriter(t *testing.T) {
	var m sync.Mutex
	testData := "Hello this is some test data"
	ts := &testSender{}

	w := NewSyncWriter(&m, func(p []byte) error {
		// As there is no way to check whether a mutex is locked already, we can just try to
		// unlock it here. If the mutex wasn't locked, it would cause a runtime error. As
		// there's no concurrent writers in this test, this is safe to do.
		m.Unlock()
		m.Lock()

		return ts.send(p)
	})

	_, err := io.CopyBuffer(&opaqueWriter{w}, strings.NewReader(testData), make([]byte, 10))
	require.NoError(t, err)

	require.Equal(t, testData, string(bytes.Join(ts.sends, nil)))
}

type testSender struct {
	sends [][]byte
}

func (ts *testSender) send(p []byte) error {
	buf := make([]byte, len(p))
	copy(buf, p)
	ts.sends = append(ts.sends, buf)
	return nil
}

func TestReadFrom(t *testing.T) {
	defer func(oldBufferSize int) {
		WriteBufferSize = oldBufferSize
	}(WriteBufferSize)
	WriteBufferSize = 5

	testData := "Hello this is the test data that will be received. It goes on for a while bla bla bla."
	testCases := []struct {
		desc string
		r    io.Reader
	}{
		{desc: "base", r: strings.NewReader(testData)},
		{desc: "dataerr", r: iotest.DataErrReader(strings.NewReader(testData))},
		{desc: "onebyte", r: iotest.OneByteReader(strings.NewReader(testData))},
		{desc: "dataerr(onebyte)", r: iotest.DataErrReader(iotest.OneByteReader(strings.NewReader(testData)))},
	}

	for _, tc := range testCases {
		ts := &testSender{}
		n, err := NewWriter(ts.send).(io.ReaderFrom).ReadFrom(tc.r)

		require.NoError(t, err, tc.desc)
		require.Equal(t, int64(len(testData)), n, tc.desc)
		require.Equal(t, testData, string(bytes.Join(ts.sends, nil)), tc.desc)
		for _, send := range ts.sends {
			require.True(t, len(send) <= WriteBufferSize, "send calls may not exceed WriteBufferSize")
		}
	}
}
