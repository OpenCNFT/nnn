package backup

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/repoutil"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/counter"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v16/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v16/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// remoteRepository implements git repository access over GRPC
type remoteRepository struct {
	repo *gitalypb.Repository
	conn *grpc.ClientConn
}

// NewRemoteRepository returns a repository accessor that operates on a remote
// repository.
func NewRemoteRepository(repo *gitalypb.Repository, conn *grpc.ClientConn) *remoteRepository {
	return &remoteRepository{
		repo: repo,
		conn: conn,
	}
}

// ListRefs fetches the full set of refs and targets for the repository
func (rr *remoteRepository) ListRefs(ctx context.Context) ([]git.Reference, error) {
	refClient := rr.newRefClient()
	stream, err := refClient.ListRefs(ctx, &gitalypb.ListRefsRequest{
		Repository: rr.repo,
		Head:       true,
		Patterns:   [][]byte{[]byte("refs/")},
	})
	if err != nil {
		return nil, fmt.Errorf("remote repository: list refs: %w", err)
	}

	var refs []git.Reference

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("remote repository: list refs: %w", err)
		}
		for _, ref := range resp.GetReferences() {
			refs = append(refs, git.NewReference(git.ReferenceName(ref.GetName()), git.ObjectID(ref.GetTarget())))
		}
	}

	return refs, nil
}

// GetCustomHooks fetches the custom hooks archive.
func (rr *remoteRepository) GetCustomHooks(ctx context.Context, out io.Writer) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.GetCustomHooks(ctx, &gitalypb.GetCustomHooksRequest{Repository: rr.repo})
	if err != nil {
		return fmt.Errorf("remote repository: get custom hooks: %w", err)
	}

	hooks := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	if _, err := io.Copy(out, hooks); err != nil {
		return fmt.Errorf("remote repository: get custom hooks: %w", err)
	}

	return nil
}

// CreateBundle fetches a bundle that contains refs matching patterns. When
// patterns is nil all refs are bundled.
func (rr *remoteRepository) CreateBundle(ctx context.Context, out io.Writer, patterns io.Reader) error {
	if patterns != nil {
		return rr.createBundlePatterns(ctx, out, patterns)
	}

	return rr.createBundle(ctx, out)
}

func (rr *remoteRepository) createBundle(ctx context.Context, out io.Writer) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.CreateBundle(ctx, &gitalypb.CreateBundleRequest{
		Repository: rr.repo,
	})
	if err != nil {
		return fmt.Errorf("remote repository: create bundle: %w", err)
	}

	bundle := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		if structerr.GRPCCode(err) == codes.FailedPrecondition {
			err = localrepo.ErrEmptyBundle
		}
		return resp.GetData(), err
	})

	if _, err := io.Copy(out, bundle); err != nil {
		return fmt.Errorf("remote repository: create bundle: %w", err)
	}

	return nil
}

func (rr *remoteRepository) createBundlePatterns(ctx context.Context, out io.Writer, patterns io.Reader) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.CreateBundleFromRefList(ctx)
	if err != nil {
		return fmt.Errorf("remote repository: create bundle patterns: %w", err)
	}
	c := chunk.New(&createBundleFromRefListSender{
		stream: stream,
	})

	buf := bufio.NewReader(patterns)
	for {
		line, err := buf.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("remote repository: create bundle patterns: %w", err)
		}

		line = bytes.TrimSuffix(line, []byte("\n"))

		if err := c.Send(&gitalypb.CreateBundleFromRefListRequest{
			Repository: rr.repo,
			Patterns:   [][]byte{line},
		}); err != nil {
			return fmt.Errorf("remote repository: create bundle patterns: send: %w", err)
		}
	}
	if err := c.Flush(); err != nil {
		return fmt.Errorf("remote repository: create bundle patterns: flush: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("remote repository: create bundle patterns: close: %w", err)
	}

	bundle := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		if structerr.GRPCCode(err) == codes.FailedPrecondition {
			err = localrepo.ErrEmptyBundle
		}
		return resp.GetData(), err
	})

	if _, err := io.Copy(out, bundle); err != nil {
		return fmt.Errorf("remote repository: create bundle patterns: recv: %w", err)
	}

	return nil
}

type createBundleFromRefListSender struct {
	stream gitalypb.RepositoryService_CreateBundleFromRefListClient
	chunk  gitalypb.CreateBundleFromRefListRequest
}

// Reset should create a fresh response message.
func (s *createBundleFromRefListSender) Reset() {
	s.chunk = gitalypb.CreateBundleFromRefListRequest{}
}

// Append should append the given item to the slice in the current response message
func (s *createBundleFromRefListSender) Append(msg proto.Message) {
	req := msg.(*gitalypb.CreateBundleFromRefListRequest)
	s.chunk.Repository = req.GetRepository()
	s.chunk.Patterns = append(s.chunk.Patterns, req.GetPatterns()...)
}

// Send should send the current response message
func (s *createBundleFromRefListSender) Send() error {
	err := s.stream.Send(&s.chunk)
	if err != nil {
		// On error, SendMsg aborts the stream. If the error was generated by
		// the client, the status is returned directly; otherwise, io.EOF is
		// returned and the status of the stream may be discovered using Recv.
		if errors.Is(err, io.EOF) {
			if _, recvErr := s.stream.Recv(); recvErr != nil {
				return recvErr
			}
		}
	}

	return err
}

// updateRefsSender chunks requests to the UpdateReferences RPC.
type updateRefsSender struct {
	refs   []*gitalypb.UpdateReferencesRequest_Update
	stream gitalypb.RefService_UpdateReferencesClient
}

// Reset should create a fresh response message.
func (s *updateRefsSender) Reset() {
	s.refs = s.refs[:0]
}

// Append should append the given item to the slice in the current response message
func (s *updateRefsSender) Append(msg proto.Message) {
	s.refs = append(s.refs, msg.(*gitalypb.UpdateReferencesRequest_Update))
}

// Send should send the current response message
func (s *updateRefsSender) Send() error {
	err := s.stream.Send(&gitalypb.UpdateReferencesRequest{
		Updates: s.refs,
	})
	if err != nil {
		// On error, SendMsg aborts the stream. If the error was generated by
		// the client, the status is returned directly; otherwise, io.EOF is
		// returned and the status of the stream may be discovered using Recv.
		if errors.Is(err, io.EOF) {
			if _, recvErr := s.stream.CloseAndRecv(); recvErr != nil {
				return recvErr
			}
		}
	}

	return err
}

// Remove removes the repository. Does not return an error if the repository
// cannot be found.
func (rr *remoteRepository) Remove(ctx context.Context) error {
	repoClient := rr.newRepoClient()
	_, err := repoClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: rr.repo,
	})
	switch {
	case status.Code(err) == codes.NotFound:
		return nil
	case err != nil:
		return fmt.Errorf("remote repository: remove: %w", err)
	}
	return nil
}

// ResetRefs attempts to reset the list of refs in the repository to match the
// specified refs slice. Do not include the symbolic HEAD reference in the list.
func (rr *remoteRepository) ResetRefs(ctx context.Context, refs []git.Reference) error {
	if len(refs) == 0 {
		return errors.New("empty refs list")
	}

	existingRefs, err := rr.ListRefs(ctx)
	if err != nil {
		return fmt.Errorf("list existing refs: %w", err)
	}

	objectHash, err := rr.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	refsToKeep := make(map[git.ReferenceName]struct{}, len(refs))
	for _, ref := range refs {
		refsToKeep[ref.Name] = struct{}{}
	}

	capacity := int(math.Max(float64(len(refs)), float64(len(existingRefs))))
	updates := make([]*gitalypb.UpdateReferencesRequest_Update, 0, capacity)
	// Add updates to delete existing refs not in the new set
	for _, existingRef := range existingRefs {
		if shouldRemoveRef(refsToKeep, existingRef.Name) {
			updates = append(updates, &gitalypb.UpdateReferencesRequest_Update{
				Reference:   []byte(existingRef.Name),
				NewObjectId: []byte(objectHash.ZeroOID),
			})
		}
	}
	// Add updates to create or modify refs in the new set
	for _, newRef := range refs {
		updates = append(updates, &gitalypb.UpdateReferencesRequest_Update{
			Reference:   []byte(newRef.Name),
			NewObjectId: []byte(newRef.Target),
		})
	}

	refClient := rr.newRefClient()
	stream, err := refClient.UpdateReferences(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	// We need to send the first update without the chunker, because the RPC expects the `Repository` field to be
	// empty in subsequent messages. We also need to send the first reference because the RPC expects at least one
	// update.
	if err := stream.Send(&gitalypb.UpdateReferencesRequest{
		Repository: rr.repo,
		Updates: []*gitalypb.UpdateReferencesRequest_Update{
			updates[0],
		},
	}); err != nil {
		return fmt.Errorf("send initial request: %w", err)
	}

	chunker := chunk.New(&updateRefsSender{stream: stream})

	for _, update := range updates[1:] {
		if err := chunker.Send(update); err != nil {
			return fmt.Errorf("send ref update: %w", err)
		}
	}

	if err := chunker.Flush(); err != nil {
		return fmt.Errorf("flush ref update chunker: %w", err)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("close stream: %w", err)
	}

	return nil
}

// SetHeadReference sets the symbolic HEAD reference of the repository.
func (rr *remoteRepository) SetHeadReference(ctx context.Context, target git.ReferenceName) error {
	repoClient := rr.newRepoClient()

	_, err := repoClient.WriteRef(ctx, &gitalypb.WriteRefRequest{
		Repository: rr.repo,
		Ref:        []byte("HEAD"),
		Revision:   []byte(target),
	})
	if err != nil {
		return fmt.Errorf("write HEAD ref: %w", err)
	}

	return nil
}

// Create creates the repository.
func (rr *remoteRepository) Create(ctx context.Context, hash git.ObjectHash, defaultBranch string) error {
	repoClient := rr.newRepoClient()
	if _, err := repoClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{
		Repository:    rr.repo,
		ObjectFormat:  hash.ProtoFormat,
		DefaultBranch: []byte(defaultBranch),
	}); err != nil {
		return fmt.Errorf("remote repository: create: %w", err)
	}
	return nil
}

// FetchBundle fetches references from a bundle. Refs will be mirrored to the
// repository.
func (rr *remoteRepository) FetchBundle(ctx context.Context, reader io.Reader, updateHead bool) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.FetchBundle(ctx)
	if err != nil {
		return fmt.Errorf("remote repository: fetch bundle: %w", err)
	}
	request := &gitalypb.FetchBundleRequest{Repository: rr.repo, UpdateHead: updateHead}
	bundle := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		// Only set `Repository` on the first `Send` of the stream
		request = &gitalypb.FetchBundleRequest{}

		return nil
	})
	if _, err := io.Copy(bundle, reader); err != nil {
		return fmt.Errorf("remote repository: fetch bundle: %w", err)
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("remote repository: fetch bundle: %w", err)
	}
	return nil
}

// SetCustomHooks updates the custom hooks for the repository.
func (rr *remoteRepository) SetCustomHooks(ctx context.Context, reader io.Reader) error {
	repoClient := rr.newRepoClient()
	stream, err := repoClient.SetCustomHooks(ctx)
	if err != nil {
		return fmt.Errorf("remote repository: set custom hooks: %w", err)
	}

	request := &gitalypb.SetCustomHooksRequest{Repository: rr.repo}
	bundle := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		// Only set `Repository` on the first `Send` of the stream
		request = &gitalypb.SetCustomHooksRequest{}

		return nil
	})
	if _, err := io.Copy(bundle, reader); err != nil {
		return fmt.Errorf("remote repository: set custom hooks: %w", err)
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("remote repository: set custom hooks: %w", err)
	}
	return nil
}

// ObjectHash detects the object hash used by the repository.
func (rr *remoteRepository) ObjectHash(ctx context.Context) (git.ObjectHash, error) {
	repoClient := rr.newRepoClient()

	response, err := repoClient.ObjectFormat(ctx, &gitalypb.ObjectFormatRequest{
		Repository: rr.repo,
	})
	if err != nil {
		return git.ObjectHash{}, fmt.Errorf("remote repository: object hash: %w", err)
	}

	return git.ObjectHashByProto(response.GetFormat())
}

// HeadReference returns the current value of HEAD.
func (rr *remoteRepository) HeadReference(ctx context.Context) (git.ReferenceName, error) {
	refClient := rr.newRefClient()

	response, err := refClient.FindDefaultBranchName(ctx, &gitalypb.FindDefaultBranchNameRequest{
		Repository: rr.repo,
		HeadOnly:   true,
	})
	if err != nil {
		return "", fmt.Errorf("remote repository: head reference: %w", err)
	}

	return git.ReferenceName(response.GetName()), nil
}

func (rr *remoteRepository) newRepoClient() gitalypb.RepositoryServiceClient {
	return gitalypb.NewRepositoryServiceClient(rr.conn)
}

func (rr *remoteRepository) newRefClient() gitalypb.RefServiceClient {
	return gitalypb.NewRefServiceClient(rr.conn)
}

type localRepository struct {
	logger        log.Logger
	locator       storage.Locator
	gitCmdFactory gitcmd.CommandFactory
	txManager     transaction.Manager
	repoCounter   *counter.RepositoryCounter
	catfileCache  catfile.Cache
	repo          *localrepo.Repo
}

// NewLocalRepository returns a repository accessor that operates on a local
// repository.
func NewLocalRepository(
	logger log.Logger,
	locator storage.Locator,
	gitCmdFactory gitcmd.CommandFactory,
	txManager transaction.Manager,
	repoCounter *counter.RepositoryCounter,
	catfileCache catfile.Cache,
	repo *localrepo.Repo,
) *localRepository {
	return &localRepository{
		logger:        logger,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		txManager:     txManager,
		repoCounter:   repoCounter,
		catfileCache:  catfileCache,
		repo:          repo,
	}
}

// ListRefs fetches the full set of refs and targets for the repository.
func (r *localRepository) ListRefs(ctx context.Context) ([]git.Reference, error) {
	refs, err := r.repo.GetReferences(ctx, "refs/")
	if err != nil {
		return nil, fmt.Errorf("local repository: list refs: %w", err)
	}

	headOID, err := r.repo.ResolveRevision(ctx, git.Revision("HEAD"))
	switch {
	case errors.Is(err, git.ErrReferenceNotFound):
		// Nothing to add
	case err != nil:
		return nil, structerr.NewInternal("local repository: list refs: %w", err)
	default:
		head := git.NewReference("HEAD", headOID)
		refs = append([]git.Reference{head}, refs...)
	}

	return refs, nil
}

// GetCustomHooks fetches the custom hooks archive.
func (r *localRepository) GetCustomHooks(ctx context.Context, out io.Writer) error {
	repoPath, err := r.locator.GetRepoPath(ctx, r.repo)
	if err != nil {
		return fmt.Errorf("get repo path: %w", err)
	}

	if err := repoutil.GetCustomHooks(ctx, r.logger, repoPath, out); err != nil {
		return fmt.Errorf("local repository: get custom hooks: %w", err)
	}
	return nil
}

// CreateBundle fetches a bundle that contains refs matching patterns. When
// patterns is nil all refs are bundled.
func (r *localRepository) CreateBundle(ctx context.Context, out io.Writer, patterns io.Reader) error {
	err := r.repo.CreateBundle(ctx, out, &localrepo.CreateBundleOpts{
		Patterns: patterns,
	})
	if err != nil {
		return fmt.Errorf("local repository: create bundle: %w", err)
	}

	return nil
}

// Remove removes the repository. Does not return an error if the repository
// cannot be found.
func (r *localRepository) Remove(ctx context.Context) error {
	err := repoutil.Remove(ctx, r.logger, r.locator, r.txManager, r.repoCounter, r.repo)
	switch {
	case status.Code(err) == codes.NotFound:
		return nil
	case err != nil:
		return fmt.Errorf("local repository: remove: %w", err)
	}
	return nil
}

// Create creates the repository.
func (r *localRepository) Create(ctx context.Context, hash git.ObjectHash, defaultBranch string) error {
	if err := repoutil.Create(
		ctx,
		r.logger,
		r.locator,
		r.gitCmdFactory,
		r.catfileCache,
		r.txManager,
		r.repoCounter,
		r.repo,
		func(repository *gitalypb.Repository) error { return nil },
		repoutil.WithObjectHash(hash),
		repoutil.WithBranchName(defaultBranch),
	); err != nil {
		return fmt.Errorf("local repository: create: %w", err)
	}

	// Recreate the local repository, since the cache of object hash and ref-format needs
	// to be invalidated.
	r.repo = localrepo.New(r.logger, r.locator, r.gitCmdFactory, r.catfileCache, r.repo)

	return nil
}

// FetchBundle fetches references from a bundle. Refs will be mirrored to the
// repository.
func (r *localRepository) FetchBundle(ctx context.Context, reader io.Reader, updateHead bool) error {
	err := r.repo.FetchBundle(ctx, r.txManager, reader, &localrepo.FetchBundleOpts{
		UpdateHead: updateHead,
	})
	if err != nil {
		return fmt.Errorf("local repository: fetch bundle: %w", err)
	}
	return nil
}

// SetCustomHooks updates the custom hooks for the repository.
func (r *localRepository) SetCustomHooks(ctx context.Context, reader io.Reader) error {
	if err := repoutil.SetCustomHooks(
		ctx,
		r.logger,
		r.locator,
		r.txManager,
		reader,
		r.repo,
	); err != nil {
		return fmt.Errorf("local repository: set custom hooks: %w", err)
	}
	return nil
}

// ObjectHash detects the object hash used by the repository.
func (r *localRepository) ObjectHash(ctx context.Context) (git.ObjectHash, error) {
	hash, err := r.repo.ObjectHash(ctx)
	if err != nil {
		return git.ObjectHash{}, fmt.Errorf("local repository: object hash: %w", err)
	}

	return hash, nil
}

// HeadReference returns the current value of HEAD.
func (r *localRepository) HeadReference(ctx context.Context) (git.ReferenceName, error) {
	head, err := r.repo.HeadReference(ctx)
	if err != nil {
		return "", fmt.Errorf("local repository: head reference: %w", err)
	}

	return head, nil
}

// ResetRefs attempts to reset the list of refs in the repository to match the
// specified refs slice. Do not include the symbolic HEAD reference in the list.
func (r *localRepository) ResetRefs(ctx context.Context, refs []git.Reference) (returnedErr error) {
	u, err := updateref.New(ctx, r.repo)
	if err != nil {
		return fmt.Errorf("error when running creating new updater: %w", err)
	}
	defer func() {
		if err := u.Close(); err != nil && returnedErr == nil {
			returnedErr = fmt.Errorf("close updater: %w", err)
		}
	}()

	existingRefs, err := r.repo.GetReferences(ctx)
	if err != nil {
		return fmt.Errorf("get existing refs: %w", err)
	}

	objectHash, err := r.ObjectHash(ctx)
	if err != nil {
		return fmt.Errorf("object hash: %w", err)
	}

	refsToKeep := make(map[git.ReferenceName]struct{}, len(refs))
	for _, ref := range refs {
		refsToKeep[ref.Name] = struct{}{}
	}

	capacity := int(math.Max(float64(len(refs)), float64(len(existingRefs))))
	updates := make([]git.Reference, 0, capacity)
	// Prepare updates to delete existing refs not in the new set
	for _, existingRef := range existingRefs {
		if shouldRemoveRef(refsToKeep, existingRef.Name) {
			updates = append(updates, git.NewReference(existingRef.Name, objectHash.ZeroOID))
		}
	}
	// Prepare updates to create or modify refs in the new set
	updates = append(updates, refs...)

	if err := u.Start(); err != nil {
		return fmt.Errorf("start reset refs transaction: %w", err)
	}

	for _, update := range updates {
		if err := u.Update(update.Name, git.ObjectID(update.Target), ""); err != nil {
			return fmt.Errorf("reset ref: %w", err)
		}
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit reset refs: %w", err)
	}

	return nil
}

// SetHeadReference sets the symbolic HEAD reference of the repository.
func (r *localRepository) SetHeadReference(ctx context.Context, target git.ReferenceName) error {
	return r.repo.SetDefaultBranch(ctx, r.txManager, target)
}

// shouldRemoveRef checks if a reference should be removed from the repository.
func shouldRemoveRef(refsToKeep map[git.ReferenceName]struct{}, name git.ReferenceName) bool {
	// Updating HEAD reference is not allowed in UpdateReferences call.
	if name == "HEAD" {
		return false
	}

	if _, shouldKeep := refsToKeep[name]; shouldKeep {
		return false
	}

	return true
}
