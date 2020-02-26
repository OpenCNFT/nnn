package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/metadata"

	"gitlab.com/gitlab-org/gitaly/internal/command"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func nonTransactionalWriteRef(ctx context.Context, req *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
	if err := writeRef(ctx, req); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.WriteRefResponse{}, nil
}

func (s *server) transactionalWriteRef(ctx context.Context, req *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
	var resp gitalypb.WriteRefResponse

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, helper.ErrInternal(errors.New("couldn't get metadata"))
	}
	if len(md.Get("transaction_id")) == 0 {
		return nil, helper.ErrInternal(errors.New("expected request_id"))
	}

	transactionID := md.Get("transaction_id")[0]

	switch req.Transaction.Step {
	case gitalypb.Transaction_PRECOMMIT:
		if err := writeRef(ctx, req); err != nil {
			return nil, helper.ErrInternal(err)
		}

		rollback, err := rollbackRef(req)
		if err != nil {
			return nil, err
		}
		s.transactions.Start(transactionID, rollback)

	case gitalypb.Transaction_COMMIT:
		if err := s.transactions.Commit(transactionID); err != nil {
			return nil, err
		}
	case gitalypb.Transaction_ROLLBACK:
		if err := s.transactions.Rollback(transactionID); err != nil {
			return nil, err
		}
	}

	return &resp, nil
}

func (s *server) WriteRef(ctx context.Context, req *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
	if err := validateWriteRefRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if req.Transaction == nil {
		return nonTransactionalWriteRef(ctx, req)
	}

	return s.transactionalWriteRef(ctx, req)
}

func rollbackSymbolicRef(req *gitalypb.WriteRefRequest) (*command.Command, error) {
	return git.SafeCmd(context.Background(), req.GetRepository(), nil, git.SubCmd{Name: "symbolic-ref", Args: []string{string(req.GetRef()), string(req.GetOldRevision())}})
}

func writeRef(ctx context.Context, req *gitalypb.WriteRefRequest) error {
	if string(req.Ref) == "HEAD" {
		return updateSymbolicRef(ctx, req)
	}
	return updateRef(ctx, req)
}

func updateSymbolicRef(ctx context.Context, req *gitalypb.WriteRefRequest) error {
	cmd, err := git.SafeCmd(ctx, req.GetRepository(), nil, git.SubCmd{Name: "symbolic-ref", Args: []string{string(req.GetRef()), string(req.GetRevision())}})
	if err != nil {
		return fmt.Errorf("error when creating symbolic-ref command: %v", err)
	}
	if err = cmd.Wait(); err != nil {
		return fmt.Errorf("error when running symbolic-ref command: %v", err)
	}
	return nil
}

func rollbackRef(req *gitalypb.WriteRefRequest) (command.Cmd, error) {
	if string(req.Ref) == "HEAD" {
		return rollbackSymbolicRef(req)
	}

	u, err := updateref.New(context.Background(), req.GetRepository())
	if err != nil {
		return nil, fmt.Errorf("error when running creating new updater: %v", err)
	}
	if err = u.Update(string(req.GetRef()), string(req.GetOldRevision()), string(req.GetRevision())); err != nil {
		return nil, fmt.Errorf("error when creating rollback-ref command: %v", err)
	}

	return u, nil
}

func updateRef(ctx context.Context, req *gitalypb.WriteRefRequest) error {
	u, err := updateref.New(ctx, req.GetRepository())
	if err != nil {
		return fmt.Errorf("error when running creating new updater: %v", err)
	}
	if err = u.Update(string(req.GetRef()), string(req.GetRevision()), string(req.GetOldRevision())); err != nil {
		return fmt.Errorf("error when creating update-ref command: %v", err)
	}
	if err = u.Wait(); err != nil {
		return fmt.Errorf("error when running update-ref command: %v", err)
	}
	return nil
}

func validateWriteRefRequest(req *gitalypb.WriteRefRequest) error {
	if err := git.ValidateRevision(req.Ref); err != nil {
		return fmt.Errorf("invalid ref: %v", err)
	}
	if err := git.ValidateRevision(req.Revision); err != nil {
		return fmt.Errorf("invalid revision: %v", err)
	}
	if len(req.OldRevision) > 0 {
		if err := git.ValidateRevision(req.OldRevision); err != nil {
			return fmt.Errorf("invalid OldRevision: %v", err)
		}
	}

	if !bytes.Equal(req.Ref, []byte("HEAD")) && !bytes.HasPrefix(req.Ref, []byte("refs/")) {
		return fmt.Errorf("ref has to be a full reference")
	}
	return nil
}
