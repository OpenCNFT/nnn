package client

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"net/url"

	"google.golang.org/grpc"
)

// DefaultDialOpts hold the default DialOptions for connection to Gitaly over UNIX-socket
var DefaultDialOpts = []grpc.DialOption{}

type connectionType int

const (
	invalidConnection connectionType = iota
	tcpConnection
	tlsConnection
	unixConnection
)

// Dial gitaly
func Dial(rawAddress string, connOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	var canonicalAddress string
	var err error

	switch getConnectionType(rawAddress) {
	case invalidConnection:
		return nil, fmt.Errorf("invalid connection string: %s", rawAddress)

	case tlsConnection:
		canonicalAddress, err = extractHostFromRemoteURL(rawAddress) // Ensure the form: "host:port" ...
		if err != nil {
			return nil, err
		}

		certPool, err := systemCertPool()
		if err != nil {
			return nil, err
		}

		creds := credentials.NewClientTLSFromCert(certPool, "")
		connOpts = append(connOpts, grpc.WithTransportCredentials(creds))

	case tcpConnection:
		canonicalAddress, err = extractHostFromRemoteURL(rawAddress) // Ensure the form: "host:port" ...
		if err != nil {
			return nil, err
		}
		connOpts = append(connOpts, grpc.WithInsecure())

	case unixConnection:
		canonicalAddress = rawAddress // This will be overridden by the custom dialer...
		connOpts = append(
			connOpts,
			grpc.WithInsecure(),
			// Use a custom dialer to ensure that we don't experience
			// issues in environments that have proxy configurations
			// https://gitlab.com/gitlab-org/gitaly/merge_requests/1072#note_140408512
			grpc.WithContextDialer(func(ctx context.Context, addr string) (conn net.Conn, err error) {
				path, err := extractPathFromSocketURL(addr)
				if err != nil {
					return nil, err
				}

				d := net.Dialer{}
				return d.DialContext(ctx, "unix", path)
			}),
		)
	}

	connOpts = append(connOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                20 * time.Second,
		PermitWithoutStream: true,
	}))

	conn, err := grpc.Dial(canonicalAddress, connOpts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func getConnectionType(rawAddress string) connectionType {
	u, err := url.Parse(rawAddress)
	if err != nil {
		return invalidConnection
	}

	switch u.Scheme {
	case "tls":
		return tlsConnection
	case "unix":
		return unixConnection
	case "tcp":
		return tcpConnection
	default:
		return invalidConnection
	}
}
