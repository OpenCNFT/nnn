package backup

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestLoggingPipeline(t *testing.T) {
	t.Parallel()

	testPipeline(t, func() Pipeline {
		return NewLoggingPipeline(testhelper.SharedLogger(t))
	})
}

func TestParallelPipeline(t *testing.T) {
	t.Parallel()

	testPipeline(t, func() Pipeline {
		return NewParallelPipeline(NewLoggingPipeline(testhelper.SharedLogger(t)), 2, 0)
	})

	t.Run("parallelism", func(t *testing.T) {
		for _, tc := range []struct {
			parallel            int
			parallelStorage     int
			expectedMaxParallel int64
		}{
			{
				parallel:            2,
				parallelStorage:     0,
				expectedMaxParallel: 2,
			},
			{
				parallel:            2,
				parallelStorage:     3,
				expectedMaxParallel: 2,
			},
			{
				parallel:            0,
				parallelStorage:     3,
				expectedMaxParallel: 6, // 2 storages * 3 workers per storage
			},
		} {
			t.Run(fmt.Sprintf("parallel:%d,parallelStorage:%d", tc.parallel, tc.parallelStorage), func(t *testing.T) {
				var calls int64
				strategy := MockStrategy{
					CreateFunc: func(ctx context.Context, req *CreateRequest) error {
						currentCalls := atomic.AddInt64(&calls, 1)
						defer atomic.AddInt64(&calls, -1)

						assert.LessOrEqual(t, currentCalls, tc.expectedMaxParallel)

						time.Sleep(time.Millisecond)
						return nil
					},
				}
				var p Pipeline
				p = NewLoggingPipeline(testhelper.SharedLogger(t))
				p = NewParallelPipeline(p, tc.parallel, tc.parallelStorage)
				ctx := testhelper.Context(t)

				for i := 0; i < 10; i++ {
					p.Handle(ctx, NewCreateCommand(strategy, CreateRequest{Repository: &gitalypb.Repository{StorageName: "storage1"}}))
					p.Handle(ctx, NewCreateCommand(strategy, CreateRequest{Repository: &gitalypb.Repository{StorageName: "storage2"}}))
				}
				require.NoError(t, p.Done())
			})
		}
	})

	t.Run("context done", func(t *testing.T) {
		var strategy MockStrategy
		var p Pipeline
		p = NewLoggingPipeline(testhelper.SharedLogger(t))
		p = NewParallelPipeline(p, 0, 0) // make sure worker channels always block

		ctx, cancel := context.WithCancel(testhelper.Context(t))

		cancel()
		<-ctx.Done()

		p.Handle(ctx, NewCreateCommand(strategy, CreateRequest{Repository: &gitalypb.Repository{StorageName: "default"}}))

		err := p.Done()
		require.EqualError(t, err, "pipeline: context canceled")
	})
}

type MockStrategy struct {
	CreateFunc                func(context.Context, *CreateRequest) error
	RestoreFunc               func(context.Context, *RestoreRequest) error
	RemoveAllRepositoriesFunc func(context.Context, *RemoveAllRepositoriesRequest) error
}

func (s MockStrategy) Create(ctx context.Context, req *CreateRequest) error {
	if s.CreateFunc != nil {
		return s.CreateFunc(ctx, req)
	}
	return nil
}

func (s MockStrategy) Restore(ctx context.Context, req *RestoreRequest) error {
	if s.RestoreFunc != nil {
		return s.RestoreFunc(ctx, req)
	}
	return nil
}

func (s MockStrategy) RemoveAllRepositories(ctx context.Context, req *RemoveAllRepositoriesRequest) error {
	if s.RemoveAllRepositoriesFunc != nil {
		return s.RemoveAllRepositoriesFunc(ctx, req)
	}
	return nil
}

func testPipeline(t *testing.T, init func() Pipeline) {
	strategy := MockStrategy{
		CreateFunc: func(_ context.Context, req *CreateRequest) error {
			switch req.Repository.StorageName {
			case "normal":
				return nil
			case "skip":
				return ErrSkipped
			case "error":
				return assert.AnError
			}
			require.Failf(t, "unexpected call to Create", "StorageName = %q", req.Repository.StorageName)
			return nil
		},
	}

	for _, tc := range []struct {
		desc           string
		command        Command
		level          logrus.Level
		expectedFields log.Fields
	}{
		{
			desc:    "Create command. Normal repository",
			command: NewCreateCommand(strategy, CreateRequest{Repository: &gitalypb.Repository{RelativePath: "a.git", StorageName: "normal"}}),
			level:   logrus.InfoLevel,
			expectedFields: log.Fields{
				"command":         "create",
				"gl_project_path": "",
				"relative_path":   "a.git",
				"storage_name":    "normal",
			},
		},
		{
			desc:    "Create command. Skipped repository",
			command: NewCreateCommand(strategy, CreateRequest{Repository: &gitalypb.Repository{RelativePath: "b.git", StorageName: "skip"}}),
			level:   logrus.WarnLevel,
			expectedFields: log.Fields{
				"command":         "create",
				"gl_project_path": "",
				"relative_path":   "b.git",
				"storage_name":    "skip",
			},
		},
		{
			desc:    "Create command. Error creating repository",
			command: NewCreateCommand(strategy, CreateRequest{Repository: &gitalypb.Repository{RelativePath: "c.git", StorageName: "error"}}),
			level:   logrus.ErrorLevel,
			expectedFields: log.Fields{
				"command":         "create",
				"gl_project_path": "",
				"relative_path":   "c.git",
				"storage_name":    "error",
			},
		},
	} {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			logger := testhelper.SharedLogger(t)
			loggerHook := testhelper.AddLoggerHook(logger)

			t.Parallel()

			p := init()
			ctx := testhelper.Context(t)

			p.Handle(ctx, tc.command)

			logEntries := loggerHook.AllEntries()

			for _, logEntry := range logEntries {
				require.Equal(t, tc.expectedFields, logEntry.Data)
				require.Equal(t, tc.level, logEntry.Level)
			}

			err := p.Done()

			if tc.level == logrus.ErrorLevel {
				require.EqualError(t, err, "pipeline: 1 failures encountered:\n - c.git: assert.AnError general error for testing\n")
			}
		})
	}

	t.Run("restore command", func(t *testing.T) {
		t.Parallel()

		strategy := MockStrategy{
			RestoreFunc: func(_ context.Context, req *RestoreRequest) error {
				switch req.Repository.StorageName {
				case "normal":
					return nil
				case "skip":
					return ErrSkipped
				case "error":
					return assert.AnError
				}
				require.Failf(t, "unexpected call to Restore", "StorageName = %q", req.Repository.StorageName)
				return nil
			},
		}
		p := init()
		ctx := testhelper.Context(t)

		commands := []Command{
			NewRestoreCommand(strategy, RestoreRequest{Repository: &gitalypb.Repository{RelativePath: "a.git", StorageName: "normal"}}),
			NewRestoreCommand(strategy, RestoreRequest{Repository: &gitalypb.Repository{RelativePath: "b.git", StorageName: "skip"}}),
			NewRestoreCommand(strategy, RestoreRequest{Repository: &gitalypb.Repository{RelativePath: "c.git", StorageName: "error"}}),
		}
		for _, cmd := range commands {
			p.Handle(ctx, cmd)
		}
		err := p.Done()
		require.EqualError(t, err, "pipeline: 1 failures encountered:\n - c.git: assert.AnError general error for testing\n")
	})
}

func TestPipelineError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		repos         []*gitalypb.Repository
		expectedError string
	}{
		{
			name: "with gl_project_path",
			repos: []*gitalypb.Repository{
				{RelativePath: "1.git", GlProjectPath: "Projects/Apple"},
				{RelativePath: "2.git", GlProjectPath: "Projects/Banana"},
				{RelativePath: "3.git", GlProjectPath: "Projects/Carrot"},
			},
			expectedError: `3 failures encountered:
 - 1.git (Projects/Apple): assert.AnError general error for testing
 - 2.git (Projects/Banana): assert.AnError general error for testing
 - 3.git (Projects/Carrot): assert.AnError general error for testing
`,
		},
		{
			name: "without gl_project_path",
			repos: []*gitalypb.Repository{
				{RelativePath: "1.git"},
				{RelativePath: "2.git"},
				{RelativePath: "3.git"},
			},
			expectedError: `3 failures encountered:
 - 1.git: assert.AnError general error for testing
 - 2.git: assert.AnError general error for testing
 - 3.git: assert.AnError general error for testing
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := PipelineErrors{}

			for _, repo := range tc.repos {
				err.AddError(repo, assert.AnError)
			}

			require.EqualError(t, err, tc.expectedError)
		})
	}
}
