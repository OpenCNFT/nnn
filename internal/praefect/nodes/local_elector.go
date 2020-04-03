package nodes

import (
	"context"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
)

type nodeCandidate struct {
	node     Node
	primary  bool
	statuses []bool
}

// localElector relies on an in-memory datastore to track the primary
// and secondaries. A single election strategy pertains to a single
// shard. It does NOT support multiple Praefect nodes or have any
// persistence. This is used mostly for testing and development.
type localElector struct {
	m               sync.RWMutex
	failoverEnabled bool
	shardName       string
	nodes           []*nodeCandidate
	primaryNode     *nodeCandidate
}

// healthcheckThreshold is the number of consecutive healthpb.HealthCheckResponse_SERVING necessary
// for deeming a node "healthy"
const healthcheckThreshold = 3

func (n *nodeCandidate) isHealthy() bool {
	if len(n.statuses) < healthcheckThreshold {
		return false
	}

	for _, status := range n.statuses[len(n.statuses)-healthcheckThreshold:] {
		if !status {
			return false
		}
	}

	return true
}

func newLocalElector(name string, failoverEnabled bool) *localElector {
	return &localElector{
		shardName:       name,
		failoverEnabled: failoverEnabled,
	}
}

// addNode registers a primary or secondary in the internal
// datastore.
func (s *localElector) addNode(node Node, primary bool) {
	localNode := nodeCandidate{
		node:     node,
		primary:  primary,
		statuses: make([]bool, 0),
	}

	s.m.Lock()
	defer s.m.Unlock()

	if primary {
		s.primaryNode = &localNode
	}

	s.nodes = append(s.nodes, &localNode)
}

// Start launches a Goroutine to check the state of the nodes and
// continuously monitor their health via gRPC health checks.
func (s *localElector) start(bootstrapInterval, monitorInterval time.Duration) error {
	s.bootstrap(bootstrapInterval)
	go s.monitor(monitorInterval)

	return nil
}

func (s *localElector) bootstrap(d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	for i := 0; i < healthcheckThreshold; i++ {
		<-timer.C

		ctx := context.TODO()
		s.checkNodes(ctx)
		timer.Reset(d)
	}
}

func (s *localElector) monitor(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		<-ticker.C

		ctx := context.Background()
		s.checkNodes(ctx)
	}
}

// checkNodes issues a gRPC health check for each node managed by the
// shard.
func (s *localElector) checkNodes(ctx context.Context) {
	s.m.Lock()
	defer s.updateMetrics()
	defer s.m.Unlock()

	for _, n := range s.nodes {
		status, _ := n.node.check(ctx)
		n.statuses = append(n.statuses, status)

		if len(n.statuses) > healthcheckThreshold {
			n.statuses = n.statuses[1:]
		}
	}

	if s.primaryNode != nil && s.primaryNode.isHealthy() {
		return
	}

	var newPrimary *nodeCandidate

	for _, node := range s.nodes {
		if !node.primary && node.isHealthy() {
			newPrimary = node
			break
		}
	}

	if newPrimary == nil {
		return
	}

	s.primaryNode.primary = false
	s.primaryNode = newPrimary
	newPrimary.primary = true
}

// GetPrimary gets the primary of a shard. If no primary exists, it will
// be nil. If a primary has been elected but is down, err will be
// ErrPrimaryNotHealthy.
func (s *localElector) GetPrimary() (Node, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.primaryNode == nil {
		return nil, ErrPrimaryNotHealthy
	}

	if s.failoverEnabled && !s.primaryNode.isHealthy() {
		return s.primaryNode.node, ErrPrimaryNotHealthy
	}

	return s.primaryNode.node, nil
}

// GetSecondaries gets the secondaries of a shard
func (s *localElector) GetSecondaries() ([]Node, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	var secondaries []Node
	for _, n := range s.nodes {
		if !n.primary {
			secondaries = append(secondaries, n.node)
		}
	}

	return secondaries, nil
}

func (s *localElector) updateMetrics() {
	s.m.RLock()
	defer s.m.RUnlock()

	for _, node := range s.nodes {
		var val float64

		if node.primary {
			val = 1
		}

		metrics.PrimaryGauge.WithLabelValues(s.shardName, node.node.GetStorage()).Set(val)
	}
}
