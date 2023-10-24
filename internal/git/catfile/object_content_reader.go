package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
)

// ObjectContentReader is a reader for Git objects.
type ObjectContentReader interface {
	cacheable

	// Reader returns a new Object for the given revision. The Object must be fully consumed
	// before another object is requested.
	Object(context.Context, git.Revision) (*Object, error)

	// ObjectQueue returns an ObjectQueue that can be used to batch multiple object requests.
	// Using the queue is more efficient than using `Object()` when requesting a bunch of
	// objects. The returned function must be executed after use of the ObjectQueue has
	// finished.
	ObjectQueue(context.Context) (ObjectQueue, func(), error)
}

// objectContentReader is a reader for Git objects. Reading is implemented via a long-lived `git cat-file
// --batch` process such that we do not have to spawn a new process for each object we are about to
// read.
type objectContentReader struct {
	cmd *command.Command

	counter *prometheus.CounterVec

	queue      requestQueue
	queueInUse int32
}

func newObjectContentReader(
	ctx context.Context,
	repo git.RepositoryExecutor,
	counter *prometheus.CounterVec,
) (*objectContentReader, error) {
	flags := []git.Option{
		git.Flag{Name: "--batch"},
		git.Flag{Name: "--buffer"},
		git.Flag{Name: "-z"},
		git.Flag{Name: "-Z"},
	}

	if featureflag.MailmapOptions.IsEnabled(ctx) {
		flags = append([]git.Option{git.Flag{Name: "--use-mailmap"}}, flags...)
	}

	batchCmd, err := repo.Exec(ctx,
		git.Command{
			Name:  "cat-file",
			Flags: flags,
		},
		git.WithSetupStdin(),
		git.WithSetupStdout(),
	)
	if err != nil {
		return nil, err
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting object hash: %w", err)
	}

	objectReader := &objectContentReader{
		cmd:     batchCmd,
		counter: counter,
		queue: requestQueue{
			objectHash:      objectHash,
			isObjectQueue:   true,
			isNulTerminated: true,
			stdout:          bufio.NewReader(batchCmd),
			stdin:           bufio.NewWriter(batchCmd),
		},
	}

	return objectReader, nil
}

func (o *objectContentReader) close() {
	o.queue.close()
	_ = o.cmd.Wait()
}

func (o *objectContentReader) isClosed() bool {
	return o.queue.isClosed()
}

func (o *objectContentReader) isDirty() bool {
	if atomic.LoadInt32(&o.queueInUse) != 0 {
		return true
	}

	return o.queue.isDirty()
}

func (o *objectContentReader) objectQueue(ctx context.Context, tracedMethod string) (*requestQueue, func(), error) {
	if !atomic.CompareAndSwapInt32(&o.queueInUse, 0, 1) {
		return nil, nil, fmt.Errorf("object queue already in use")
	}

	trace := startTrace(ctx, o.counter, tracedMethod)
	o.queue.trace = trace

	return &o.queue, func() {
		atomic.StoreInt32(&o.queueInUse, 0)
		trace.finish()
	}, nil
}

func (o *objectContentReader) Object(ctx context.Context, revision git.Revision) (*Object, error) {
	queue, finish, err := o.objectQueue(ctx, "")
	if err != nil {
		return nil, err
	}
	defer finish()

	if err := queue.RequestObject(ctx, revision); err != nil {
		return nil, err
	}

	if err := queue.Flush(ctx); err != nil {
		return nil, err
	}

	object, err := queue.ReadObject(ctx)
	if err != nil {
		return nil, err
	}

	return object, nil
}

func (o *objectContentReader) ObjectQueue(ctx context.Context) (ObjectQueue, func(), error) {
	queue, finish, err := o.objectQueue(ctx, "catfile.ObjectQueue")
	if err != nil {
		return nil, nil, err
	}
	return queue, finish, nil
}

// Object represents data returned by `git cat-file --batch`
type Object struct {
	// ObjectInfo represents main information about object
	ObjectInfo

	// dataReader is reader which has all the object data.
	dataReader io.Reader
}

func (o *Object) Read(p []byte) (int, error) {
	return o.dataReader.Read(p)
}

// WriteTo implements the io.WriterTo interface. It defers the write to the embedded object reader
// via `io.Copy()`, which in turn will use `WriteTo()` or `ReadFrom()` in case these interfaces are
// implemented by the respective reader or writer.
func (o *Object) WriteTo(w io.Writer) (int64, error) {
	// `io.Copy()` will make use of `ReadFrom()` in case the writer implements it.
	return io.Copy(w, o.dataReader)
}
