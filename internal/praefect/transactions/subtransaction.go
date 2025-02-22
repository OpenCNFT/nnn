package transactions

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/transaction/voting"
)

// VoteResult represents the outcome of a transaction for a single voter.
type VoteResult int

const (
	// VoteUndecided means that the voter either didn't yet show up or that
	// the vote couldn't yet be decided due to there being no majority yet.
	VoteUndecided VoteResult = iota
	// VoteCommitted means that the voter committed his vote.
	VoteCommitted
	// VoteFailed means that the voter has failed the vote because a
	// majority of nodes has elected a different result.
	VoteFailed
	// VoteCanceled means that the transaction was cancelled.
	VoteCanceled
	// VoteStopped means that the transaction was gracefully stopped.
	VoteStopped
)

// subtransaction is a single session where voters are voting for a certain outcome.
type subtransaction struct {
	doneCh chan interface{}

	threshold uint

	lock         sync.RWMutex
	votersByNode map[string]*Voter
	voteCounts   map[voting.Vote]uint
}

func newSubtransaction(voters []Voter, threshold uint) (*subtransaction, error) {
	votersByNode := make(map[string]*Voter, len(voters))
	for _, voter := range voters {
		votersByNode[voter.Name] = &voter
	}

	return &subtransaction{
		doneCh:       make(chan interface{}),
		threshold:    threshold,
		votersByNode: votersByNode,
		voteCounts:   make(map[voting.Vote]uint, len(voters)),
	}, nil
}

func (t *subtransaction) cancel() {
	t.lock.Lock()
	defer t.lock.Unlock()

	for _, voter := range t.votersByNode {
		// If a voter didn't yet show up or is still undecided, we need
		// to mark it as failed so it won't get the idea of committing
		// the transaction at a later point anymore.
		if voter.result == VoteUndecided {
			voter.result = VoteCanceled
		}
	}

	if !t.isDone() {
		close(t.doneCh)
	}
}

func (t *subtransaction) stop() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	for _, voter := range t.votersByNode {
		switch voter.result {
		case VoteCanceled:
			// If the vote was canceled already, we cannot stop it.
			// A single node voter being canceled is not indicative
			// of all voter's state. Other voters must be checked.
			continue
		case VoteStopped:
			// Similar if the vote was stopped already.
			return ErrTransactionStopped
		case VoteUndecided:
			// Undecided voters will get stopped, ...
			voter.result = VoteStopped
		case VoteCommitted, VoteFailed:
			// ... while decided voters cannot be changed anymore.
			continue
		}
	}

	if !t.isDone() {
		close(t.doneCh)
	}

	return nil
}

func (t *subtransaction) state() map[string]VoteResult {
	t.lock.Lock()
	defer t.lock.Unlock()

	results := make(map[string]VoteResult, len(t.votersByNode))
	for node, voter := range t.votersByNode {
		results[node] = voter.result
	}

	return results
}

func (t *subtransaction) vote(node string, vote voting.Vote) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Cast our vote. In case the node doesn't exist or has already cast a
	// vote, we need to abort.
	voter, ok := t.votersByNode[node]
	if !ok {
		return fmt.Errorf("invalid node for transaction: %q", node)
	}
	if voter.vote != nil {
		return fmt.Errorf("node already cast a vote: %q", node)
	}

	// Update voter state to reflect the new vote counts. Before quorum is reached, this
	// function will check whether the threshold was reached and, if so, update all voters which
	// have already cast a vote. After quorum was reached, it will only update the currently
	// voting node.
	if err := t.updateVoterState(voter, &vote); err != nil {
		return fmt.Errorf("updating state of node %q: %w", node, err)
	}

	return nil
}

// updateVoterState updates undecided voters or cancels existing votes if given a `nil`
// vote. Voters are updated either as soon as quorum was reached or alternatively when
// quorum becomes impossible.
func (t *subtransaction) updateVoterState(voter *Voter, vote *voting.Vote) error {
	switch voter.result {
	case VoteUndecided:
		// Happy case, we can still cast a vote.
		break
	case VoteCanceled:
		return ErrTransactionCanceled
	case VoteStopped:
		return ErrTransactionStopped
	case VoteCommitted:
		return fmt.Errorf("cannot change committed vote")
	default:
		// Because we didn't vote yet, we know that the node cannot be
		// either in VoteCommitted or VoteFailed state.
		return fmt.Errorf("voter is in invalid state %d", voter.result)
	}

	switch {
	case vote != nil:
		if voter.vote != nil {
			return errors.New("changing current vote is not allowed")
		}

		t.voteCounts[*vote] += voter.Votes
		voter.vote = vote
	case vote == nil:
		if t.isDone() {
			// If the transaction is already done, it's too late to cancel our vote.
			// Other nodes may have committed their changes already.
			return errors.New("subtransaction was already finished")
		}

		// A voter's result can only be canceled if the subtransaction is still pending.
		// If a change has already been committed to disk the voter result cannot be
		// changed since the subtransction is considered complete.
		voter.result = VoteCanceled

		// Remove the voter's support for the vote so it's not counted towards the
		// majority. The node is not going to commit the subtransaction anyway.
		if voter.vote != nil {
			t.voteCounts[*voter.vote] -= voter.Votes
		}

		// A canceled voter can no longer voter so its vote is
		// reset after being subtracted from the vote counts.
		voter.vote = nil
	}

	defer func() {
		if t.mustSignalVoters() {
			close(t.doneCh)
		}
	}()

	// Check if quorum has been reached or if quorum is still attainable for the subtransaction.
	// If quorum has not been achieved the vote returned by the quorum check will be `nil`.
	// As long as quorum has not been achieved and is still possible, the subtransaction
	// will wait for additional voter's results to come in.
	majorityVote, quorumPossible := t.quorumCheck()
	if majorityVote == nil && quorumPossible {
		return nil
	}

	// Update all voters which have cast a vote and which are not undecided. We mustn't change
	// any voters which did decide on an outcome already as they may have already committed or
	// aborted their action.
	for _, voter := range t.votersByNode {
		// We cannot change the mind of nodes which have already settled on any outcome
		// after the fact.
		if voter.result != VoteUndecided {
			continue
		}

		// We do not change the mind of any voter which didn't yet cast its vote. While it
		// may be true that it can only fail anyway, it is easier to handle if we just wait
		// for its incoming vote and set it to failed at that point in time.
		if voter.vote == nil {
			continue
		}

		// If quorum is not possible we know there are not enough outstanding votes to cross
		// the threshold required by the subtransaction. We can thus mark this node as failed.
		if !quorumPossible {
			voter.result = VoteFailed
			continue
		}

		// At this point we know quorum has been achieved and a majority vote is present.
		// A check is done to see if the voter agrees with the quorum majority vote.
		if *voter.vote == *majorityVote {
			voter.result = VoteCommitted
		} else {
			voter.result = VoteFailed
		}
	}

	return nil
}

// mustSignalVoters determines whether we need to signal voters. Signalling may
// only happen once, so we need to make sure that either we just crossed the
// threshold or that nobody else did and no more votes are missing.
func (t *subtransaction) mustSignalVoters() bool {
	// If somebody else already notified voters, then we mustn't do so
	// again.
	if t.isDone() {
		return false
	}

	majorityVote, quorumPossible := t.quorumCheck()

	// If there is majority vote threshold has been met and voters can be signaled.
	if majorityVote != nil {
		return true
	}

	// If quorum is still possible the voters should not be signaled, since
	// remaining voters could cause us to reach quorum. If quorum is not
	// possible voters should be unblocked to allow the transaction to fail.
	return !quorumPossible
}

// quorumCheck returns the majority vote if quorum has been achieved
// and if not `nil` is returned. It also returns whether quorum can
// still be achieved with the outstanding voters.
func (t *subtransaction) quorumCheck() (*voting.Vote, bool) {
	var leader *voting.Vote
	var majority uint
	for v, voteCount := range t.voteCounts {
		if majority < voteCount {
			v := v
			majority = voteCount
			leader = &v
		}
	}

	if majority >= t.threshold {
		return leader, true
	}

	var outstanding uint
	for _, voter := range t.votersByNode {
		if voter.vote == nil && voter.result == VoteUndecided {
			outstanding += voter.Votes
		}
	}

	return nil, majority+outstanding >= t.threshold
}

func (t *subtransaction) collectVotes(ctx context.Context, node string) error {
	select {
	case <-ctx.Done():
	case <-t.doneCh:
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	voter, ok := t.votersByNode[node]
	if !ok {
		return fmt.Errorf("invalid node for transaction: %q", node)
	}

	// If the waiting stopped due to the context being canceled, we need to cancel
	// this voter's votes.
	if err := ctx.Err(); err != nil {
		if err := t.updateVoterState(voter, nil); err != nil {
			return fmt.Errorf("cancel vote: %w", err)
		}

		return ctx.Err()
	}

	switch voter.result {
	case VoteCommitted:
		// Happy case, we are part of the quorum.
		return nil
	case VoteFailed:
		if voter.vote == nil {
			return fmt.Errorf("%w: did not cast a vote", ErrTransactionFailed)
		}
		return fmt.Errorf("%w: got %d/%d votes for %v", ErrTransactionFailed,
			t.voteCounts[*voter.vote], t.threshold, *voter.vote)
	case VoteCanceled:
		// It may happen that the vote was cancelled or stopped just after majority was
		// reached. In that case, the node's state is now VoteCanceled/VoteStopped, so we
		// have to return an error here.
		return ErrTransactionCanceled
	case VoteStopped:
		return ErrTransactionStopped
	case VoteUndecided:
		// We shouldn't ever be undecided if the caller correctly calls
		// `vote()` before calling `collectVotes()` as this node
		// would've cast a vote in that case.
		return fmt.Errorf("voter is in undecided state: %q", node)
	default:
		return fmt.Errorf("voter is in invalid state %d: %q", voter.result, node)
	}
}

func (t *subtransaction) isDone() bool {
	select {
	case <-t.doneCh:
		return true
	default:
		return false
	}
}

func (t *subtransaction) getResult(node string) (VoteResult, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	voter, ok := t.votersByNode[node]
	if !ok {
		return VoteCanceled, fmt.Errorf("invalid node for transaction: %q", node)
	}

	return voter.result, nil
}

func (t *subtransaction) getVote(node string) (*voting.Vote, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	voter, ok := t.votersByNode[node]
	if !ok {
		return nil, fmt.Errorf("invalid node for transaction: %q", node)
	}

	if voter.vote == nil {
		return nil, nil
	}

	// Return a copy of the vote.
	vote := *voter.vote
	return &vote, nil
}

// cancelNodeVoter updates a node's associated voter state to `VoteCanceled`.
// All must voters wait until either quorum has been achieved or quorum
// becomes impossible. A canceled voter's votes are not counted as a part of
// the total outstanding votes which can cause a subtransaction to not have
// enough votes to reach the required threshold. If this happens the vote
// will be considered failed and the voters unblocked.
func (t *subtransaction) cancelNodeVoter(node string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	voter, ok := t.votersByNode[node]
	if !ok {
		return fmt.Errorf("invalid node for subtransaction: %q", node)
	}

	// Updating voter state with a nil vote will result in the voter
	// getting canceled.
	if err := t.updateVoterState(voter, nil); err != nil {
		return fmt.Errorf("cancel vote: %w", err)
	}

	return nil
}

// getPropagatedVoters returns provided voters with voter result state
// matching canceled subtransaction voters. This is used because newly
// created subtransactions need to propagate previously canceled voters.
func (t *subtransaction) getPropagatedVoters(voters []Voter) ([]Voter, error) {
	// Lock subtransaction to prevent concurrent writes to voter
	// result state from `updateVoterState()`.
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Check subtransaction voters state and propagate canceled voters.
	var propagatedVoters []Voter
	for _, voter := range voters {
		subVoter := t.votersByNode[voter.Name]
		if subVoter == nil {
			// This error should in theory never be reached. When a
			// subtransaction is created it receives all voters from
			// the parent transaction. The parent transaction voters
			// are not mutated throughout the lifespan of the
			// transaction meaning that all voters in a transaction
			// should be present in a subtransaction.
			return nil, errors.New("subtransaction missing voter")
		}

		// Only canceled voters need to be propagated since a node voter
		// can be canceled and the transaction continue. Other terminal
		// results are applied to voters and end the transaction.
		if subVoter.result == VoteCanceled {
			voter.result = VoteCanceled
		}

		propagatedVoters = append(propagatedVoters, voter)
	}

	return propagatedVoters, nil
}
