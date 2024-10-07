package server

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v16/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// ClockSynced returns whether the system clock has an acceptable time drift when compared to NTP service.
func (s *Server) ClockSynced(_ context.Context, req *gitalypb.ClockSyncedRequest) (*gitalypb.ClockSyncedResponse, error) {
	if err := req.GetDriftThreshold().CheckValid(); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	synced, err := helper.CheckClockSync(req.GetNtpHost(), req.GetDriftThreshold().AsDuration())
	if err != nil {
		return nil, err
	}
	return &gitalypb.ClockSyncedResponse{Synced: synced}, nil
}
