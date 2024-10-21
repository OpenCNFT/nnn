package snapshot

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"golang.org/x/sync/errgroup"
)

func TestManager(t *testing.T) {
	ctx := testhelper.Context(t)

	umask := testhelper.Umask()

	writeFile := func(t *testing.T, storageDir string, snapshot FileSystem, relativePath string) {
		t.Helper()

		require.NoError(t, os.WriteFile(filepath.Join(storageDir, snapshot.RelativePath(relativePath)), nil, fs.ModePerm))
	}

	type metricValues struct {
		createdExclusiveSnapshotCounter   uint64
		destroyedExclusiveSnapshotCounter uint64
		createdSharedSnapshotCounter      uint64
		reusedSharedSnapshotCounter       uint64
		destroyedSharedSnapshotCounter    uint64
	}

	for _, tc := range []struct {
		desc            string
		run             func(t *testing.T, mgr *Manager)
		expectedMetrics metricValues
	}{
		{
			desc: "exclusive snapshots are not shared",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, true)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, true)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs2)

				require.NotEqual(t, fs1.Root(), fs2.Root())

				writeFile(t, mgr.storageDir, fs1, "repositories/a/fs1")
				writeFile(t, mgr.storageDir, fs2, "repositories/a/fs2")

				testhelper.RequireDirectoryState(t, fs1.Root(), "", testhelper.DirectoryState{
					// The snapshotting process does not use the existing permissions for
					// directories in the hierarchy before the repository directories.
					"/":                       {Mode: mode.Directory},
					"/repositories":           {Mode: mode.Directory},
					"/repositories/a":         {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/refs":    {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/objects": {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/HEAD":    {Mode: umask.Mask(fs.ModePerm), Content: []byte("a content")},
					"/repositories/a/fs1":     {Mode: umask.Mask(fs.ModePerm), Content: []byte{}},
				})

				testhelper.RequireDirectoryState(t, fs2.Root(), "", testhelper.DirectoryState{
					"/":                       {Mode: mode.Directory},
					"/repositories":           {Mode: mode.Directory},
					"/repositories/a":         {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/refs":    {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/objects": {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
					"/repositories/a/HEAD":    {Mode: umask.Mask(fs.ModePerm), Content: []byte("a content")},
					"/repositories/a/fs2":     {Mode: umask.Mask(fs.ModePerm), Content: []byte{}},
				})
			},
			expectedMetrics: metricValues{
				createdExclusiveSnapshotCounter:   2,
				destroyedExclusiveSnapshotCounter: 2,
			},
		},
		{
			desc: "shared snapshots are shared",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs2)

				require.Equal(t, fs1.Root(), fs2.Root())

				// Writing into shared snapshots is not allowed.
				require.ErrorIs(t, os.WriteFile(filepath.Join(fs1.Root(), "some file"), nil, fs.ModePerm), os.ErrPermission)

				expectedDirectoryState := testhelper.DirectoryState{
					"/":                       {Mode: ModeReadOnlyDirectory},
					"/repositories":           {Mode: ModeReadOnlyDirectory},
					"/repositories/a":         {Mode: ModeReadOnlyDirectory},
					"/repositories/a/refs":    {Mode: ModeReadOnlyDirectory},
					"/repositories/a/objects": {Mode: ModeReadOnlyDirectory},
					"/repositories/a/HEAD":    {Mode: umask.Mask(fs.ModePerm), Content: []byte("a content")},
				}

				testhelper.RequireDirectoryState(t, fs1.Root(), "", expectedDirectoryState)
				testhelper.RequireDirectoryState(t, fs2.Root(), "", expectedDirectoryState)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   1,
				reusedSharedSnapshotCounter:    1,
				destroyedSharedSnapshotCounter: 1,
			},
		},
		{
			desc: "multiple relative paths are snapshotted",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a", "repositories/b"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				// The order of the relative paths should not prevent sharing a snapshot.
				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/b", "repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs2)

				require.Equal(t, fs1.Root(), fs2.Root())

				expectedDirectoryState := testhelper.DirectoryState{
					"/":                       {Mode: ModeReadOnlyDirectory},
					"/repositories":           {Mode: ModeReadOnlyDirectory},
					"/repositories/a":         {Mode: ModeReadOnlyDirectory},
					"/repositories/a/refs":    {Mode: ModeReadOnlyDirectory},
					"/repositories/a/objects": {Mode: ModeReadOnlyDirectory},
					"/repositories/a/HEAD":    {Mode: umask.Mask(fs.ModePerm), Content: []byte("a content")},
					"/repositories/b":         {Mode: ModeReadOnlyDirectory},
					"/repositories/b/refs":    {Mode: ModeReadOnlyDirectory},
					"/repositories/b/objects": {Mode: ModeReadOnlyDirectory},
					"/repositories/b/HEAD":    {Mode: umask.Mask(fs.ModePerm), Content: []byte("b content")},
				}

				testhelper.RequireDirectoryState(t, fs1.Root(), "", expectedDirectoryState)
				testhelper.RequireDirectoryState(t, fs2.Root(), "", expectedDirectoryState)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   1,
				reusedSharedSnapshotCounter:    1,
				destroyedSharedSnapshotCounter: 1,
			},
		},
		{
			desc: "alternate is included in snapshot",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/c"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				testhelper.RequireDirectoryState(t, fs1.Root(), "", testhelper.DirectoryState{
					"/":                                       {Mode: ModeReadOnlyDirectory},
					"/repositories":                           {Mode: ModeReadOnlyDirectory},
					"/repositories/b":                         {Mode: ModeReadOnlyDirectory},
					"/repositories/b/refs":                    {Mode: ModeReadOnlyDirectory},
					"/repositories/b/objects":                 {Mode: ModeReadOnlyDirectory},
					"/repositories/b/HEAD":                    {Mode: umask.Mask(fs.ModePerm), Content: []byte("b content")},
					"/repositories/c":                         {Mode: ModeReadOnlyDirectory},
					"/repositories/c/refs":                    {Mode: ModeReadOnlyDirectory},
					"/repositories/c/objects":                 {Mode: ModeReadOnlyDirectory},
					"/repositories/c/HEAD":                    {Mode: umask.Mask(fs.ModePerm), Content: []byte("c content")},
					"/repositories/c/objects/info":            {Mode: ModeReadOnlyDirectory},
					"/repositories/c/objects/info/alternates": {Mode: umask.Mask(fs.ModePerm), Content: []byte("../../b/objects")},
				})
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   1,
				destroyedSharedSnapshotCounter: 1,
			},
		},
		{
			desc: "shared snaphots against the relative paths with the same LSN are shared",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs2)

				require.Equal(t, fs1.Root(), fs2.Root())

				mgr.SetLSN(2)

				fs3, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs3)

				fs4, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs4)

				require.Equal(t, fs3.Root(), fs4.Root())
				require.NotEqual(t, fs1.Root(), fs3.Root())
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   2,
				reusedSharedSnapshotCounter:    2,
				destroyedSharedSnapshotCounter: 2,
			},
		},
		{
			desc: "shared snaphots against different relative paths are not shared",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs1)

				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/b"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs2)

				fs3, err := mgr.GetSnapshot(ctx, []string{"repositories/a", "repositories/b"}, false)
				require.NoError(t, err)
				defer testhelper.MustClose(t, fs3)

				require.NotEqual(t, fs1.Root(), fs2.Root())
				require.NotEqual(t, fs1.Root(), fs3.Root())
				require.NotEqual(t, fs2.Root(), fs3.Root())
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   3,
				destroyedSharedSnapshotCounter: 3,
			},
		},
		{
			desc: "unused shared snapshots are cached up to a limit",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				mgr.maxInactiveSharedSnapshots = 2

				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)

				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)

				// Shared snaphots should be equal.
				require.Equal(t, fs1.Root(), fs2.Root())

				// Clean up the other user.
				testhelper.MustClose(t, fs2)

				fs3, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)

				// The first user is still there using the snapshot so it should still be there
				// and be reused for the next snapshotter.
				require.Equal(t, fs1.Root(), fs3.Root())

				// Clean both of the last users of the shared snapshot.
				testhelper.MustClose(t, fs1)
				testhelper.MustClose(t, fs3)

				fs4, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)

				// Unused snapshot was recovered from the cache.
				require.Equal(t, fs1.Root(), fs4.Root())
				// Release the snapshot back to the cache.
				testhelper.MustClose(t, fs4)

				// Open two snapshots. Both are against different data, so both lead to
				// creating new snapshots.
				fsB, err := mgr.GetSnapshot(ctx, []string{"repositories/b"}, false)
				require.NoError(t, err)
				require.NotEqual(t, fsB.Root(), fs4.Root())

				fsC, err := mgr.GetSnapshot(ctx, []string{"repositories/c"}, false)
				require.NoError(t, err)
				require.NotEqual(t, fsC.Root(), fsB.Root())

				// We now have two more unused shared snapshots which along with the original
				// one would lead to exceeding cache size.
				testhelper.MustClose(t, fsB)
				testhelper.MustClose(t, fsC)

				// As the cache size was exceeded, the shared snapshot of A should have been
				// evicted, and a new one returned.
				fs5, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				require.NotEqual(t, fs5.Root(), fs1.Root())
				require.NotEqual(t, fs5.Root(), fsC.Root())
				testhelper.MustClose(t, fs5)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   4,
				reusedSharedSnapshotCounter:    3,
				destroyedSharedSnapshotCounter: 4,
			},
		},
		{
			desc: "exclusive snapshots don't affect caching",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				mgr.maxInactiveSharedSnapshots = 2

				// Open shared snapshot and close it to cache it.
				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				testhelper.MustClose(t, fs1)

				// Open two exclusive snapshots. They are not cached, and should not evict the
				// snapshot of A in cache.
				for i := 0; i < 2; i++ {
					fsExclusive, err := mgr.GetSnapshot(ctx, []string{"repositories/b"}, true)
					require.NoError(t, err)
					require.NotEqual(t, fsExclusive.Root(), fs1.Root())
					testhelper.MustClose(t, fsExclusive)
				}

				// The shared snapshot should still be retrieved from the cache.
				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				require.Equal(t, fs1.Root(), fs2.Root())
				testhelper.MustClose(t, fs2)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:      1,
				reusedSharedSnapshotCounter:       1,
				destroyedSharedSnapshotCounter:    1,
				createdExclusiveSnapshotCounter:   2,
				destroyedExclusiveSnapshotCounter: 2,
			},
		},
		{
			desc: "LSN changing invalidates cache",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				// Open shared snapshot and close it to cache it.
				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				testhelper.MustClose(t, fs1)

				mgr.SetLSN(1)

				// The shared snapshot should still be retrieved from the cache.
				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				require.NotEqual(t, fs1.Root(), fs2.Root())
				testhelper.MustClose(t, fs2)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   2,
				destroyedSharedSnapshotCounter: 2,
			},
		},
		{
			desc: "LSN changing while shared snapshot is open prevents caching",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				// Open shared snapshot and close it to cache it.
				fs1, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)

				mgr.SetLSN(1)

				testhelper.MustClose(t, fs1)

				// The shared snapshot should still be retrieved from the cache.
				fs2, err := mgr.GetSnapshot(ctx, []string{"repositories/a"}, false)
				require.NoError(t, err)
				require.NotEqual(t, fs1.Root(), fs2.Root())
				testhelper.MustClose(t, fs2)
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   2,
				destroyedSharedSnapshotCounter: 2,
			},
		},

		{
			desc: "concurrently taking multiple shared snapshots",
			run: func(t *testing.T, mgr *Manager) {
				defer testhelper.MustClose(t, mgr)

				// Defer the clean snapshot clean ups at the end of the test.
				var cleanGroup errgroup.Group
				defer func() { require.NoError(t, cleanGroup.Wait()) }()

				startCleaning := make(chan struct{})
				defer close(startCleaning)

				snapshotGroup, ctx := errgroup.WithContext(ctx)
				startSnapshot := make(chan struct{})
				takeSnapshots := func(relativePath string, snapshots []FileSystem) {
					for i := 0; i < len(snapshots); i++ {
						snapshotGroup.Go(func() error {
							<-startSnapshot
							var err error
							fs, err := mgr.GetSnapshot(ctx, []string{relativePath}, false)
							if err != nil {
								return err
							}

							snapshots[i] = fs

							cleanGroup.Go(func() error {
								<-startCleaning
								return fs.Close()
							})

							return nil
						})
					}
				}

				snapshotsA := make([]FileSystem, 20)
				takeSnapshots("repositories/a", snapshotsA)

				snapshotsB := make([]FileSystem, 20)
				takeSnapshots("repositories/b", snapshotsB)

				close(startSnapshot)
				require.NoError(t, snapshotGroup.Wait())

				// All of the snapshots taken with the same relative path should be the same.
				for _, fs := range snapshotsA {
					require.Equal(t, snapshotsA[0].Root(), fs.Root())
				}

				for _, fs := range snapshotsB {
					require.Equal(t, snapshotsB[0].Root(), fs.Root())
				}

				require.NotEqual(t, snapshotsA[0].Root(), snapshotsB[0].Root())
			},
			expectedMetrics: metricValues{
				createdSharedSnapshotCounter:   2,
				reusedSharedSnapshotCounter:    38,
				destroyedSharedSnapshotCounter: 2,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tmpDir := t.TempDir()
			storageDir := filepath.Join(tmpDir, "storage-dir")
			workingDir := filepath.Join(storageDir, "working-dir")

			testhelper.CreateFS(t, storageDir, fstest.MapFS{
				".":            {Mode: fs.ModeDir | fs.ModePerm},
				"working-dir":  {Mode: fs.ModeDir | fs.ModePerm},
				"repositories": {Mode: fs.ModeDir | fs.ModePerm},
				// Create enough content in the repositories to pass the repository validity check.
				"repositories/a":                         {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/a/HEAD":                    {Mode: fs.ModePerm, Data: []byte("a content")},
				"repositories/a/refs":                    {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/a/objects":                 {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/b":                         {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/b/HEAD":                    {Mode: fs.ModePerm, Data: []byte("b content")},
				"repositories/b/refs":                    {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/b/objects":                 {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/c/HEAD":                    {Mode: fs.ModePerm, Data: []byte("c content")},
				"repositories/c":                         {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/c/refs":                    {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/c/objects":                 {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/c/objects/info":            {Mode: fs.ModeDir | fs.ModePerm},
				"repositories/c/objects/info/alternates": {Mode: fs.ModePerm, Data: []byte("../../b/objects")},
			})

			metrics := NewMetrics()

			mgr, err := NewManager(testhelper.SharedLogger(t), storageDir, workingDir, metrics.Scope("storage-name"))
			require.NoError(t, err)

			tc.run(t, mgr)

			testhelper.RequirePromMetrics(t, metrics, fmt.Sprintf(`
# HELP gitaly_exclusive_snapshots_created_total Number of created exclusive snapshots.
# TYPE gitaly_exclusive_snapshots_created_total counter
gitaly_exclusive_snapshots_created_total{storage="storage-name"} %d
# HELP gitaly_exclusive_snapshots_destroyed_total Number of destroyed exclusive snapshots.
# TYPE gitaly_exclusive_snapshots_destroyed_total counter
gitaly_exclusive_snapshots_destroyed_total{storage="storage-name"} %d
# HELP gitaly_shared_snapshots_created_total Number of created shared snapshots.
# TYPE gitaly_shared_snapshots_created_total counter
gitaly_shared_snapshots_created_total{storage="storage-name"} %d
# HELP gitaly_shared_snapshots_reused_total Number of reused shared snapshots.
# TYPE gitaly_shared_snapshots_reused_total counter
gitaly_shared_snapshots_reused_total{storage="storage-name"} %d
# HELP gitaly_shared_snapshots_destroyed_total Number of destroyed shared snapshots.
# TYPE gitaly_shared_snapshots_destroyed_total counter
gitaly_shared_snapshots_destroyed_total{storage="storage-name"} %d
			`,
				tc.expectedMetrics.createdExclusiveSnapshotCounter,
				tc.expectedMetrics.destroyedExclusiveSnapshotCounter,
				tc.expectedMetrics.createdSharedSnapshotCounter,
				tc.expectedMetrics.reusedSharedSnapshotCounter,
				tc.expectedMetrics.destroyedSharedSnapshotCounter,
			))

			// All snapshots should have been cleaned up.
			testhelper.RequireDirectoryState(t, workingDir, "", testhelper.DirectoryState{
				"/": {Mode: fs.ModeDir | umask.Mask(fs.ModePerm)},
			})
			require.Empty(t, mgr.activeSharedSnapshots)
		})
	}
}
