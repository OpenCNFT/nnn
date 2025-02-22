package bootstrap

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudflare/tableflip"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/env"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"golang.org/x/sys/unix"
)

const (
	// EnvPidFile is the name of the environment variable containing the pid file path
	EnvPidFile = "GITALY_PID_FILE"
	// EnvUpgradesEnabled is an environment variable that when defined gitaly must enable graceful upgrades on SIGHUP
	EnvUpgradesEnabled     = "GITALY_UPGRADES_ENABLED"
	socketReusePortWarning = "Unable to set SO_REUSEPORT: zero downtime upgrades will not work"
)

// Listener is an interface of the bootstrap manager.
type Listener interface {
	// RegisterStarter adds starter to the pool.
	RegisterStarter(starter Starter)
	// Start starts all registered starters to accept connections.
	Start() error
	// Wait terminates all registered starters.
	Wait(gracePeriodTicker helper.Ticker, stopAction func()) error
}

// Bootstrap handles graceful upgrades
type Bootstrap struct {
	logger         log.Logger
	upgrader       upgrader
	listenFunc     ListenFunc
	errChan        chan error
	starters       []Starter
	connTotal      *prometheus.CounterVec
	allServersDone chan struct{}
}

type upgrader interface {
	Exit() <-chan struct{}
	HasParent() bool
	Ready() error
	Upgrade() error
	Stop()
}

// New performs tableflip initialization
//
//	pidFile is optional, if provided it will always contain the current process PID
//	upgradesEnabled controls the upgrade process on SIGHUP signal
//
// first boot:
// * gitaly starts as usual, we will refer to it as p1
// * New will build a tableflip.Upgrader, we will refer to it as upg
// * sockets and files must be opened with upg.Fds
// * p1 will trap SIGHUP and invoke upg.Upgrade()
// * when ready to accept incoming connections p1 will call upg.Ready()
// * upg.Exit() channel will be closed when an upgrades completed successfully and the process must terminate
//
// graceful upgrade:
//   - user replaces gitaly binary and/or config file
//   - user sends SIGHUP to p1
//   - p1 will fork and exec the new gitaly, we will refer to it as p2
//   - from now on p1 will ignore other SIGHUP
//   - if p2 terminates with a non-zero exit code, SIGHUP handling will be restored
//   - p2 will follow the "first boot" sequence but upg.Fds will provide sockets and files from p1, when available
//   - when p2 invokes upg.Ready() all the shared file descriptors not claimed by p2 will be closed
//   - upg.Exit() channel in p1 will be closed now and p1 can gracefully terminate already accepted connections
//   - upgrades cannot starts again if p1 and p2 are both running, an hard termination should be scheduled to overcome
//     freezes during a graceful shutdown
//
// gitaly-wrapper is supposed to set EnvUpgradesEnabled in order to enable graceful upgrades
func New(logger log.Logger, totalConn *prometheus.CounterVec) (*Bootstrap, error) {
	pidFile := os.Getenv(EnvPidFile)
	upgradesEnabled, _ := env.GetBool(EnvUpgradesEnabled, false)

	// PIDFile is optional, if provided tableflip will keep it updated
	upg, err := tableflip.New(tableflip.Options{
		PIDFile: pidFile,
		ListenConfig: &net.ListenConfig{
			Control: func(network, address string, c syscall.RawConn) error {
				var opErr error
				err := c.Control(func(fd uintptr) {
					opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
				})
				if err != nil {
					logger.WithError(err).Warn(socketReusePortWarning)
				}
				if opErr != nil {
					logger.WithError(opErr).Warn(socketReusePortWarning)
				}
				return nil
			},
		},
	})
	if err != nil {
		return nil, err
	}

	return _new(logger, upg, upg.Fds.Listen, upgradesEnabled, totalConn)
}

func _new(logger log.Logger, upg upgrader, listenFunc ListenFunc, upgradesEnabled bool, totalConn *prometheus.CounterVec) (*Bootstrap, error) {
	if upgradesEnabled {
		go func() {
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, syscall.SIGHUP)

			for range sig {
				err := upg.Upgrade()
				if err != nil {
					logger.WithError(err).Error("Upgrade failed")
					continue
				}

				logger.Info("Upgrade succeeded")
			}
		}()
	}

	return &Bootstrap{
		logger:     logger,
		upgrader:   upg,
		listenFunc: listenFunc,
		connTotal:  totalConn,
	}, nil
}

// ListenFunc is a net.Listener factory
type ListenFunc func(net, addr string) (net.Listener, error)

// Starter is function to initialize a net.Listener
// it receives a ListenFunc to be used for net.Listener creation and a chan<- error to signal runtime errors
// It must serve incoming connections asynchronously and signal errors on the channel
// the return value is for setup errors
type Starter func(ListenFunc, chan<- error, *prometheus.CounterVec) error

func (b *Bootstrap) isFirstBoot() bool { return !b.upgrader.HasParent() }

// RegisterStarter adds a new starter
func (b *Bootstrap) RegisterStarter(starter Starter) {
	b.starters = append(b.starters, starter)
}

// Start will invoke all the registered starters and wait asynchronously for runtime errors
// in case a Starter fails then the error is returned and the function is aborted
func (b *Bootstrap) Start() error {
	b.errChan = make(chan error, len(b.starters))

	for _, start := range b.starters {
		if err := start(b.listen, b.errChan, b.connTotal); err != nil {
			return err
		}
	}

	return nil
}

// Wait will signal process readiness to the parent and than wait for an exit condition
// SIGTERM, SIGINT and a runtime error will trigger an immediate shutdown
// in case of an upgrade there will be a grace period to complete the ongoing requests
// stopAction will be invoked during a graceful stop. It must wait until the shutdown is completed.
func (b *Bootstrap) Wait(gracePeriodTicker helper.Ticker, stopAction func()) error {
	signals := []os.Signal{syscall.SIGTERM, syscall.SIGINT}
	shutdown := make(chan os.Signal, len(signals))
	signal.Notify(shutdown, signals...)

	if err := b.upgrader.Ready(); err != nil {
		return err
	}

	select {
	case <-b.upgrader.Exit():
		// this is the old process and a graceful upgrade is in progress
		// the new process signaled its readiness and we started a graceful stop
		// however no further upgrades can be started until this process is running
		// we set a grace period and then we force a termination.
		b.logger.Info("[graceful upgrade] stopping old Gitaly")
		if waitError := b.waitGracePeriod(gracePeriodTicker, shutdown, stopAction); waitError != nil {
			return fmt.Errorf("graceful upgrade: %w", waitError)
		}
		b.logger.Info("[graceful upgrade] stopped old Gitaly")
	case s := <-shutdown:
		b.logger.Info("[shutdown] stopping Gitaly")
		if waitError := b.waitGracePeriod(gracePeriodTicker, shutdown, stopAction); waitError != nil {
			return fmt.Errorf("received signal %q: wait: %w", s, waitError)
		}
		b.logger.Info("[shutdown] stopped Gitaly gracefully")
		b.upgrader.Stop()
	case err := <-b.errChan:
		return err
	}

	return nil
}

func (b *Bootstrap) waitGracePeriod(gracePeriodTicker helper.Ticker, kill <-chan os.Signal, stopAction func()) error {
	b.logger.Warn("starting grace period")

	b.allServersDone = make(chan struct{})
	go func() {
		if stopAction != nil {
			stopAction()
		}
		close(b.allServersDone)
	}()

	gracePeriodTicker.Reset()

	select {
	case <-gracePeriodTicker.C():
		return fmt.Errorf("grace period expired")
	case <-kill:
		return fmt.Errorf("force shutdown")
	case <-b.allServersDone:
		return nil
	}
}

func (b *Bootstrap) listen(network, path string) (net.Listener, error) {
	if network == "unix" && b.isFirstBoot() {
		if err := os.RemoveAll(path); err != nil {
			return nil, err
		}
	}

	return b.listenFunc(network, path)
}

// Noop is a bootstrapper that does no additional configurations.
type Noop struct {
	starters  []Starter
	shutdown  chan struct{}
	errChan   chan error
	connTotal *prometheus.CounterVec
}

// NewNoop returns initialized instance of the *Noop.
func NewNoop(connTotal *prometheus.CounterVec) *Noop {
	return &Noop{shutdown: make(chan struct{}), connTotal: connTotal}
}

// RegisterStarter adds starter to the pool.
func (n *Noop) RegisterStarter(starter Starter) {
	n.starters = append(n.starters, starter)
}

// Start starts all registered starters to accept connections.
func (n *Noop) Start() error {
	n.errChan = make(chan error, len(n.starters))

	for _, start := range n.starters {
		if err := start(
			net.Listen,
			n.errChan,
			n.connTotal,
		); err != nil {
			return err
		}
	}
	return nil
}

// Wait terminates all registered starters.
func (n *Noop) Wait(_ helper.Ticker, stopAction func()) error {
	select {
	case <-n.shutdown:
		if stopAction != nil {
			stopAction()
		}
	case err := <-n.errChan:
		return err
	}

	return nil
}

// Terminate unblocks Wait method and executes stopAction call-back passed into it.
func (n *Noop) Terminate() {
	close(n.shutdown)
}
