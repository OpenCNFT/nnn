package commit

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/signature"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
)

func (s *server) GetCommitSignatures(request *gitalypb.GetCommitSignaturesRequest, stream gitalypb.CommitService_GetCommitSignaturesServer) error {
	ctx := stream.Context()

	if err := s.locator.ValidateRepository(request.GetRepository()); err != nil {
		return err
	}

	repo := s.localrepo(request.GetRepository())

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("detecting object hash: %w", err)
	}

	if err := validateGetCommitSignaturesRequest(objectHash, request); err != nil {
		return structerr.NewInvalidArgument("%w", err)
	}

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return structerr.NewInternal("%w", err)
	}
	defer cancel()

	var signingKeys *signature.SigningKeys
	if s.cfg.Git.SigningKey != "" {
		signingKeys, err = signature.ParseSigningKeys(s.cfg.Git.SigningKey, s.cfg.Git.RotatedSigningKeys...)
		if err != nil {
			return fmt.Errorf("failed to parse signing key: %w", err)
		}
	}

	for _, commitID := range request.CommitIds {
		commitObj, err := objectReader.Object(ctx, git.Revision(commitID)+"^{commit}")
		if err != nil {
			if errors.As(err, &catfile.NotFoundError{}) {
				continue
			}
			return structerr.NewInternal("%w", err)
		}

		signatureKey, commitText, err := extractSignature(commitObj)
		if err != nil {
			return structerr.NewInternal("%w", err)
		}

		signer := gitalypb.GetCommitSignaturesResponse_SIGNER_USER
		if signingKeys != nil {
			if err := signingKeys.Verify(signatureKey, commitText); err == nil {
				signer = gitalypb.GetCommitSignaturesResponse_SIGNER_SYSTEM
			}
		}

		if err = sendResponse(commitID, signatureKey, commitText, signer, stream); err != nil {
			return structerr.NewInternal("%w", err)
		}
	}

	return nil
}

func extractSignature(reader io.Reader) ([]byte, []byte, error) {
	commitText := []byte{}
	signatureKey := []byte{}
	sawSignature := false
	inSignature := false
	lineBreak := []byte("\n")
	whiteSpace := []byte(" ")
	bufferedReader := bufio.NewReader(reader)

	for {
		line, err := bufferedReader.ReadBytes('\n')

		if err == io.EOF {
			commitText = append(commitText, line...)
			break
		}
		if err != nil {
			return nil, nil, err
		}

		if !sawSignature && !inSignature {
			for _, signatureField := range [][]byte{[]byte("gpgsig "), []byte("gpgsig-sha256 ")} {
				if !bytes.HasPrefix(line, signatureField) {
					continue
				}

				sawSignature, inSignature = true, true
				line = bytes.TrimPrefix(line, signatureField)
				break
			}
		}

		if inSignature && !bytes.Equal(line, lineBreak) {
			line = bytes.TrimPrefix(line, whiteSpace)
			signatureKey = append(signatureKey, line...)
		} else if inSignature {
			inSignature = false
			commitText = append(commitText, line...)
		} else {
			commitText = append(commitText, line...)
		}
	}

	// Remove last line break from signature
	signatureKey = bytes.TrimSuffix(signatureKey, lineBreak)

	return signatureKey, commitText, nil
}

func sendResponse(
	commitID string,
	signatureKey []byte,
	commitText []byte,
	signer gitalypb.GetCommitSignaturesResponse_Signer,
	stream gitalypb.CommitService_GetCommitSignaturesServer,
) error {
	if len(signatureKey) <= 0 {
		return nil
	}

	err := stream.Send(&gitalypb.GetCommitSignaturesResponse{
		CommitId:  commitID,
		Signature: signatureKey,
		Signer:    signer,
	})
	if err != nil {
		return err
	}

	streamWriter := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetCommitSignaturesResponse{SignedText: p})
	})

	msgReader := bytes.NewReader(commitText)

	_, err = io.Copy(streamWriter, msgReader)
	if err != nil {
		return fmt.Errorf("failed to send response: %w", err)
	}

	return nil
}

func validateGetCommitSignaturesRequest(objectHash git.ObjectHash, request *gitalypb.GetCommitSignaturesRequest) error {
	if len(request.GetCommitIds()) == 0 {
		return errors.New("empty CommitIds")
	}

	// Do not support shorthand or invalid commit SHAs
	for _, commitID := range request.CommitIds {
		if err := objectHash.ValidateHex(commitID); err != nil {
			return err
		}
	}

	return nil
}
