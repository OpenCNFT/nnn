package catfile

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
)

// ObjectInfo represents a header returned by `git cat-file --batch`
type ObjectInfo struct {
	Oid  git.ObjectID
	Type string
	Size int64
	// Format is the object format used by this object, e.g. "sha1" or "sha256".
	Format string
}

// IsBlob returns true if object type is "blob"
func (o *ObjectInfo) IsBlob() bool {
	return o.Type == "blob"
}

// ObjectID is the ID of the object.
func (o *ObjectInfo) ObjectID() git.ObjectID {
	return o.Oid
}

// ObjectType is the type of the object.
func (o *ObjectInfo) ObjectType() string {
	return o.Type
}

// ObjectSize is the size of the object.
func (o *ObjectInfo) ObjectSize() int64 {
	return o.Size
}

// NotFoundError is returned when requesting an object that does not exist.
type NotFoundError struct{ error }

// IsNotFound tests whether err has type NotFoundError.
func IsNotFound(err error) bool {
	_, ok := err.(NotFoundError)
	return ok
}

// ParseObjectInfo reads from a reader and parses the data into an ObjectInfo struct with the given
// object hash.
func ParseObjectInfo(objectHash git.ObjectHash, stdout *bufio.Reader, nulTerminated bool) (*ObjectInfo, error) {
restart:
	var terminator byte = '\n'
	if nulTerminated {
		terminator = '\000'
	}

	infoLine, err := stdout.ReadString(terminator)
	if err != nil {
		return nil, fmt.Errorf("read info line: %w", err)
	}

	infoLine = strings.TrimSuffix(infoLine, string(terminator))
	if strings.HasSuffix(infoLine, " missing") {
		// We use a hack to flush stdout of git-cat-file(1), which is that we request an
		// object that cannot exist. This causes Git to write an error and immediately flush
		// stdout. The only downside is that we need to filter this error here, but that's
		// acceptable while git-cat-file(1) doesn't yet have any way to natively flush.
		if strings.HasPrefix(infoLine, flushCommand) {
			goto restart
		}

		return nil, NotFoundError{fmt.Errorf("object not found")}
	}

	info := strings.Split(infoLine, " ")
	if len(info) != 3 {
		return nil, fmt.Errorf("invalid info line: %q", infoLine)
	}

	oid, err := objectHash.FromHex(info[0])
	if err != nil {
		return nil, fmt.Errorf("parse object ID: %w", err)
	}

	objectSize, err := strconv.ParseInt(info[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse object size: %w", err)
	}

	return &ObjectInfo{
		Oid:    oid,
		Type:   info[1],
		Size:   objectSize,
		Format: objectHash.Format,
	}, nil
}

// ObjectInfoReader returns information about an object referenced by a given revision.
type ObjectInfoReader interface {
	cacheable

	// Info requests information about the revision pointed to by the given revision.
	Info(context.Context, git.Revision) (*ObjectInfo, error)

	// ObjectQueue returns an ObjectQueue that can be used to batch multiple object info
	// requests. Using the queue is more efficient than using `Info()` when requesting a bunch
	// of objects. The returned function must be executed after use of the ObjectQueue has
	// finished.
	ObjectQueue(context.Context) (ObjectQueue, func(), error)
}

// objectInfoReader is a reader for Git object information. This reader is implemented via a
// long-lived  `git cat-file --batch-check` process such that we do not have to spawn a separate
// process per object info we're about to read.
type objectInfoReader struct {
	// These items must be listed first to ensure 64-bit alignment on a 32-bit system.
	// This explicit ordering can go away once we use Go 1.19's atomic types: https://gitlab.com/gitlab-org/gitaly/-/issues/4702
	queue      requestQueue
	queueInUse int32

	cmd        *command.Command
	objectHash git.ObjectHash

	counter *prometheus.CounterVec
}

func newObjectInfoReader(
	ctx context.Context,
	repo git.RepositoryExecutor,
	counter *prometheus.CounterVec,
) (*objectInfoReader, error) {
	gitVersion, err := repo.GitVersion(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting Git version: %w", err)
	}

	flags := []git.Option{
		git.Flag{Name: "--batch-check"},
		git.Flag{Name: "--buffer"},
		git.Flag{Name: "-z"},
	}

	if gitVersion.CatfileSupportsNulTerminatedOutput() {
		flags = append(flags, git.Flag{Name: "-Z"})
	}

	batchCmd, err := repo.Exec(ctx,
		git.Command{
			Name:  "cat-file",
			Flags: flags,
		},
		git.WithSetupStdin(),
	)
	if err != nil {
		return nil, err
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting object hash: %w", err)
	}

	objectInfoReader := &objectInfoReader{
		cmd:        batchCmd,
		objectHash: objectHash,
		counter:    counter,
		queue: requestQueue{
			objectHash:      objectHash,
			isNulTerminated: gitVersion.CatfileSupportsNulTerminatedOutput(),
			stdout:          bufio.NewReader(batchCmd),
			stdin:           bufio.NewWriter(batchCmd),
		},
	}

	return objectInfoReader, nil
}

func (o *objectInfoReader) close() {
	o.queue.close()
	_ = o.cmd.Wait()
}

func (o *objectInfoReader) isClosed() bool {
	return o.queue.isClosed()
}

func (o *objectInfoReader) isDirty() bool {
	if atomic.LoadInt32(&o.queueInUse) != 0 {
		return true
	}

	return o.queue.isDirty()
}

func (o *objectInfoReader) infoQueue(ctx context.Context, tracedMethod string) (*requestQueue, func(), error) {
	if !atomic.CompareAndSwapInt32(&o.queueInUse, 0, 1) {
		return nil, nil, fmt.Errorf("object info queue already in use")
	}

	trace := startTrace(ctx, o.counter, tracedMethod)
	o.queue.trace = trace

	return &o.queue, func() {
		atomic.StoreInt32(&o.queueInUse, 0)
		trace.finish()
	}, nil
}

func (o *objectInfoReader) Info(ctx context.Context, revision git.Revision) (*ObjectInfo, error) {
	queue, cleanup, err := o.infoQueue(ctx, "catfile.Info")
	if err != nil {
		return nil, err
	}
	defer cleanup()

	if err := queue.RequestInfo(ctx, revision); err != nil {
		return nil, err
	}

	if err := queue.Flush(ctx); err != nil {
		return nil, err
	}

	objectInfo, err := queue.ReadInfo(ctx)
	if err != nil {
		return nil, err
	}

	return objectInfo, nil
}

func (o *objectInfoReader) ObjectQueue(ctx context.Context) (ObjectQueue, func(), error) {
	queue, cleanup, err := o.infoQueue(ctx, "catfile.InfoQueue")
	if err != nil {
		return nil, nil, err
	}

	return queue, cleanup, nil
}
