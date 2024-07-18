package log

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
)

// SocketPath returns the log socket's path in runtime directory.
func SocketPath(runtimeDir string) string {
	return filepath.Join(runtimeDir, "log.socket")
}

// Server handles proxying logs from Gitaly's subprocesses, such as 'gitaly-hooks' and
// 'gitaly-lfs-smudge' into the log output. It exposes a Unix socket in the runtime
// directory that the subprocesses connect to and relay their fully formatted log lines.
// The server reads the log lines from the socket and prints them to the log output.
type Server struct {
	logger Logger

	listener net.Listener
	output   io.Writer
}

// NewServer returns a new Server. It opens the socket it listens for the logs on.
func NewServer(logger Logger, runtimeDir string, output io.Writer) (*Server, error) {
	ln, err := net.Listen("unix", SocketPath(runtimeDir))
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	return &Server{
		logger:   logger.WithField("component", "log-server"),
		listener: ln,
		output:   output,
	}, nil
}

// Run starts accepting connections on the log socket and relays the logs into the
// log output. It waits for all active connections to close before returning.
func (s *Server) Run() (returnedErr error) {
	defer func() {
		if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close listener: %w", err))
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				return fmt.Errorf("accept: %w", err)
			}

			return nil
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := s.handleConn(conn); err != nil {
				s.logger.WithError(err).Error("handling subprocess logs failed")
			}
		}()
	}
}

// Close closes the log socket and leads to Run returning.
func (s *Server) Close() error {
	return s.listener.Close()
}

func (s *Server) handleConn(conn net.Conn) (returnedErr error) {
	defer func() {
		if err := conn.Close(); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close conn: %w", err))
		}
	}()

	// Close the writing side to signal to the client the connection has been
	// accepted and is ready for writing.
	if err := conn.(*net.UnixConn).CloseWrite(); err != nil {
		return fmt.Errorf("close write: %w", err)
	}

	r := bufio.NewReader(conn)

	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(line) != 0 {
					// Partial lines without a line ending must not be logged. They
					// would only occur if the subprocess is killed before outputting
					// the proper log line and would lead to possibly printing invalid
					// JSON in the logs.
					return fmt.Errorf("incomplete log line: %q", line)
				}

				return nil
			}

			return fmt.Errorf("read line: %w", err)
		}

		if _, err := s.output.Write(line); err != nil {
			return fmt.Errorf("write: %w", err)
		}
	}
}
