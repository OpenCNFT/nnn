package partition

import (
	"context"
	"runtime/trace"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// prepareOffloading initiates the offloading process during the transaction commit phase.
func (mgr *TransactionManager) prepareOffloading(ctx context.Context, transaction *Transaction) error {
	if transaction.runOffloading == nil {
		return nil
	}

	defer trace.StartRegion(ctx, "prepareOffloading").End()

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.prepareOffloading", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("total", "prepare")
	defer finishTimer()

	return nil
}

// verifyOffloading checks for conflicts before applying the Write-Ahead Log (WAL) entry to ensure consistency.
func (mgr *TransactionManager) verifyOffloading(ctx context.Context, transaction *Transaction) (*gitalypb.LogEntry_RepositoryOffloading, error) {
	if transaction.runOffloading == nil {
		return nil, nil
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.verifyOffloading", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("offload", "verify")
	defer finishTimer()

	return &gitalypb.LogEntry_RepositoryOffloading{}, nil
}

// applyOffloading finalizes the offloading by applying the Write-Ahead Log (WAL) entry to the target repository.
func (mgr *TransactionManager) applyOffloading(ctx context.Context, lsn storage.LSN, logEntry *gitalypb.LogEntry) error {
	if logEntry.GetRepositoryOffloading() == nil {
		return nil
	}

	span, _ := tracing.StartSpanIfHasParent(ctx, "transaction.applyRepacking", nil)
	defer span.Finish()

	finishTimer := mgr.metrics.housekeeping.ReportTaskLatency("repack", "apply")
	defer finishTimer()

	return nil
}
