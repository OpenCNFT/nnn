package fshistory

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

func TestHistory(t *testing.T) {
	t.Run("new history is empty", func(t *testing.T) {
		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{},
				lsnByPath:  map[string]storage.LSN{},
				root: &node{
					nodeType: directoryNode,
					children: children{},
				},
			},
			New(),
		)
	})

	t.Run("empty write sets don't modify history", func(t *testing.T) {
		history := New()

		tx := history.Begin(0)
		require.NoError(t, tx.Read("refs/heads/main"))
		tx.Commit(1)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{},
				lsnByPath:  map[string]storage.LSN{},
				root: &node{
					nodeType: directoryNode,
					children: children{},
				},
			},
			history,
		)
	})

	t.Run("create nodes", func(t *testing.T) {
		history := New()

		tx := history.Begin(0)
		require.NoError(t, tx.CreateDirectory("root-directory"))
		require.NoError(t, tx.CreateDirectory("root-directory/directory"))
		require.NoError(t, tx.CreateFile("root-directory/file"))
		require.NoError(t, tx.CreateFile("root-directory/negative"))
		require.NoError(t, tx.Remove("root-directory/negative"))
		require.NoError(t, tx.CreateFile("root-file"))
		require.NoError(t, tx.Remove("root-negative"))
		require.NoError(t, tx.CreateDirectory("implicit-subdir-1/directory"))
		require.NoError(t, tx.CreateFile("implicit-subdir-2/file"))
		require.NoError(t, tx.Remove("implicit-subdir-3/negative"))
		tx.Commit(1)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					1: {
						"root-directory":              {},
						"root-directory/directory":    {},
						"root-directory/file":         {},
						"root-directory/negative":     {},
						"root-file":                   {},
						"root-negative":               {},
						"implicit-subdir-1/directory": {},
						"implicit-subdir-2/file":      {},
						"implicit-subdir-3/negative":  {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"root-directory":              1,
					"root-directory/directory":    1,
					"root-directory/file":         1,
					"root-directory/negative":     1,
					"root-file":                   1,
					"root-negative":               1,
					"implicit-subdir-1/directory": 1,
					"implicit-subdir-2/file":      1,
					"implicit-subdir-3/negative":  1,
				},
				root: &node{
					nodeType:              directoryNode,
					knownDirectoryEntries: 5,
					children: children{
						"root-directory": {
							nodeType:              directoryNode,
							lsn:                   1,
							knownDirectoryEntries: 2,
							children: children{
								"directory": {
									nodeType: directoryNode,
									lsn:      1,
									children: children{},
								},
								"file": {
									nodeType: fileNode,
									lsn:      1,
									children: children{},
								},
								"negative": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
							},
						},
						"root-file": {
							nodeType: fileNode,
							lsn:      1,
							children: children{},
						},
						"root-negative": {
							nodeType: negativeNode,
							lsn:      1,
							children: children{},
						},
						"implicit-subdir-1": {
							nodeType:              directoryNode,
							knownDirectoryEntries: 1,
							children: children{
								"directory": {
									nodeType: directoryNode,
									lsn:      1,
									children: children{},
								},
							},
						},
						"implicit-subdir-2": {
							nodeType:              directoryNode,
							knownDirectoryEntries: 1,
							children: children{
								"file": {
									nodeType: fileNode,
									lsn:      1,
									children: children{},
								},
							},
						},
						"implicit-subdir-3": {
							nodeType: directoryNode,
							children: children{
								"negative": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)
	})

	t.Run("remove nodes", func(t *testing.T) {
		history := New()

		tx := history.Begin(0)
		require.NoError(t, tx.CreateDirectory("directory"))
		require.NoError(t, tx.CreateFile("file"))
		require.NoError(t, tx.CreateDirectory("subdir/directory"))
		require.NoError(t, tx.CreateFile("subdir/file"))
		require.NoError(t, tx.Remove("subdir/negative"))
		tx.Commit(1)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					1: {
						"directory":        {},
						"file":             {},
						"subdir/directory": {},
						"subdir/file":      {},
						"subdir/negative":  {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"directory":        1,
					"file":             1,
					"subdir/directory": 1,
					"subdir/file":      1,
					"subdir/negative":  1,
				},
				root: &node{
					nodeType:              directoryNode,
					knownDirectoryEntries: 3,
					children: children{
						"directory": {
							nodeType: directoryNode,
							lsn:      1,
							children: children{},
						},
						"file": {
							nodeType: fileNode,
							lsn:      1,
							children: children{},
						},
						"subdir": {
							nodeType:              directoryNode,
							knownDirectoryEntries: 2,
							children: children{
								"directory": {
									nodeType: directoryNode,
									lsn:      1,
									children: children{},
								},
								"file": {
									nodeType: fileNode,
									lsn:      1,
									children: children{},
								},
								"negative": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)

		tx = history.Begin(1)
		require.NoError(t, tx.Remove("directory"))
		require.NoError(t, tx.Remove("file"))
		require.NoError(t, tx.Remove("subdir/directory"))
		require.NoError(t, tx.Remove("subdir/file"))
		require.NoError(t, tx.Remove("subdir"))
		tx.Commit(2)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					1: {
						"subdir/negative": {},
					},
					2: {
						"directory":        {},
						"file":             {},
						"subdir/directory": {},
						"subdir/file":      {},
						"subdir":           {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"subdir/negative":  1,
					"directory":        2,
					"file":             2,
					"subdir/directory": 2,
					"subdir/file":      2,
					"subdir":           2,
				},
				root: &node{
					nodeType: directoryNode,
					children: children{
						"directory": {
							nodeType: negativeNode,
							lsn:      2,
							children: children{},
						},
						"file": {
							nodeType: negativeNode,
							lsn:      2,
							children: children{},
						},
						"subdir": {
							nodeType: negativeNode,
							lsn:      2,
							children: children{
								"directory": {
									nodeType: negativeNode,
									lsn:      2,
									children: children{},
								},
								"file": {
									nodeType: negativeNode,
									lsn:      2,
									children: children{},
								},
								"negative": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)
	})

	t.Run("create nodes on negatives", func(t *testing.T) {
		history := New()

		tx := history.Begin(0)
		require.NoError(t, tx.Remove("file"))
		require.NoError(t, tx.Remove("directory/file"))
		require.NoError(t, tx.Remove("directory/directory"))
		require.NoError(t, tx.Remove("directory/negative"))
		require.NoError(t, tx.Remove("directory"))
		tx.Commit(1)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					1: {
						"file":                {},
						"directory":           {},
						"directory/file":      {},
						"directory/directory": {},
						"directory/negative":  {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"file":                1,
					"directory":           1,
					"directory/file":      1,
					"directory/directory": 1,
					"directory/negative":  1,
				},
				root: &node{
					nodeType: directoryNode,
					children: children{
						"file": {
							nodeType: negativeNode,
							lsn:      1,
							children: children{},
						},
						"directory": {
							nodeType: negativeNode,
							lsn:      1,
							children: children{
								"directory": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
								"file": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
								"negative": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)

		tx = history.Begin(1)
		require.NoError(t, tx.CreateFile("file"))
		require.NoError(t, tx.CreateDirectory("directory"))
		require.NoError(t, tx.CreateFile("directory/file"))
		require.NoError(t, tx.CreateDirectory("directory/directory"))
		tx.Commit(2)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					1: {
						"directory/negative": {},
					},

					2: {
						"file":                {},
						"directory":           {},
						"directory/file":      {},
						"directory/directory": {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"directory/negative":  1,
					"file":                2,
					"directory":           2,
					"directory/file":      2,
					"directory/directory": 2,
				},
				root: &node{
					nodeType:              directoryNode,
					knownDirectoryEntries: 2,
					children: children{
						"file": {
							nodeType: fileNode,
							lsn:      2,
							children: children{},
						},
						"directory": {
							nodeType:              directoryNode,
							lsn:                   2,
							knownDirectoryEntries: 2,
							children: children{
								"directory": {
									nodeType: directoryNode,
									lsn:      2,
									children: children{},
								},
								"file": {
									nodeType: fileNode,
									lsn:      2,
									children: children{},
								},
								"negative": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)
	})

	t.Run("concurrency conflicts", func(t *testing.T) {
		history := New()

		tx := history.Begin(0)
		require.NoError(t, tx.CreateDirectory("directory"))
		require.NoError(t, tx.CreateFile("file"))
		require.NoError(t, tx.Remove("negative/negative"))
		require.NoError(t, tx.Remove("negative"))
		tx.Commit(1)

		t.Run("concurrent directory creation", func(t *testing.T) {
			require.Equal(t, newConflictingOperation("directory", 0, 1), history.Begin(0).CreateDirectory("directory"))
		})

		t.Run("concurrent file creation", func(t *testing.T) {
			require.Equal(t, newConflictingOperation("file", 0, 1), history.Begin(0).CreateFile("file"))
		})

		t.Run("concurrent removal", func(t *testing.T) {
			require.Equal(t, newConflictingOperation("negative", 0, 1), history.Begin(0).CreateFile("negative"))
		})

		t.Run("concurrent removal of parent directory of directory", func(t *testing.T) {
			require.Equal(t, newConflictingOperation("negative", 0, 1), history.Begin(0).CreateDirectory("negative/directory"))
		})

		t.Run("concurrent removal of parent directory of file", func(t *testing.T) {
			require.Equal(t, newConflictingOperation("negative", 0, 1), history.Begin(0).CreateFile("negative/file"))
		})

		t.Run("concurrent removal of parent directory", func(t *testing.T) {
			require.Equal(t, newConflictingOperation("negative", 0, 1), history.Begin(0).CreateFile("negative/negative"))
		})

		t.Run("directory creation conflicts with a read", func(t *testing.T) {
			require.Equal(t, newConflictingOperation("directory", 0, 1), history.Begin(0).Read("directory"))
		})

		t.Run("file creation conflicts with a read", func(t *testing.T) {
			require.Equal(t, newConflictingOperation("file", 0, 1), history.Begin(0).Read("file"))
		})

		t.Run("removal conflicts with a read", func(t *testing.T) {
			require.Equal(t, newConflictingOperation("negative", 0, 1), history.Begin(0).Read("negative"))
		})

		t.Run("removal of parent directory with a read", func(t *testing.T) {
			require.Equal(t, newConflictingOperation("negative", 0, 1), history.Begin(0).Read("negative/negative"))
		})

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					1: {
						"directory":         {},
						"file":              {},
						"negative":          {},
						"negative/negative": {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"directory":         1,
					"file":              1,
					"negative":          1,
					"negative/negative": 1,
				},
				root: &node{
					nodeType:              directoryNode,
					knownDirectoryEntries: 2,
					children: children{
						"directory": {
							nodeType: directoryNode,
							lsn:      1,
							children: children{},
						},
						"file": {
							nodeType: fileNode,
							lsn:      1,
							children: children{},
						},
						"negative": {
							nodeType: negativeNode,
							lsn:      1,
							children: children{
								"negative": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)
	})

	t.Run("invalid operations", func(t *testing.T) {
		history := New()

		tx := history.Begin(0)
		require.NoError(t, tx.CreateDirectory("refs/heads"))
		require.NoError(t, tx.CreateFile("refs/heads/main"))
		require.NoError(t, tx.Remove("removed"))
		tx.Commit(1)

		t.Run("create directory on implicitly created directory", func(t *testing.T) {
			require.Equal(t, newAlreadyExistsError("refs"), history.Begin(1).CreateDirectory("refs"))
		})

		t.Run("create directory on explicitly created directory", func(t *testing.T) {
			require.Equal(t, newAlreadyExistsError("refs/heads"), history.Begin(1).CreateDirectory("refs/heads"))
		})

		t.Run("create directory on file", func(t *testing.T) {
			require.Equal(t, newAlreadyExistsError("refs/heads/main"), history.Begin(1).CreateDirectory("refs/heads/main"))
		})

		t.Run("create directory below file", func(t *testing.T) {
			require.Equal(t, newNotDirectoryError("refs/heads/main"), history.Begin(1).CreateDirectory("refs/heads/main/child"))
		})

		t.Run("create directory below negative node", func(t *testing.T) {
			require.Equal(t, newNotDirectoryError("removed"), history.Begin(1).CreateDirectory("removed/directory"))
		})

		t.Run("create file on implicitly created directory", func(t *testing.T) {
			require.Equal(t, newAlreadyExistsError("refs"), history.Begin(1).CreateFile("refs"))
		})

		t.Run("create file on explicitly created directory", func(t *testing.T) {
			require.Equal(t, newAlreadyExistsError("refs/heads"), history.Begin(1).CreateFile("refs/heads"))
		})

		t.Run("create file on file", func(t *testing.T) {
			require.Equal(t, newAlreadyExistsError("refs/heads/main"), history.Begin(1).CreateFile("refs/heads/main"))
		})

		t.Run("create file below file", func(t *testing.T) {
			require.Equal(t, newNotDirectoryError("refs/heads/main"), history.Begin(1).CreateFile("refs/heads/main/child"))
		})

		t.Run("create file below negative node", func(t *testing.T) {
			require.Equal(t, newNotDirectoryError("removed"), history.Begin(1).CreateDirectory("removed/file"))
		})

		t.Run("remove directory with a file in it", func(t *testing.T) {
			require.Equal(t, newDirectoryNotEmptyError("refs/heads"), history.Begin(1).Remove("refs/heads"))
		})

		t.Run("remove directory with a directory in it", func(t *testing.T) {
			require.Equal(t, newDirectoryNotEmptyError("refs"), history.Begin(1).Remove("refs"))
		})

		t.Run("remove a negative node", func(t *testing.T) {
			require.Equal(t, newNotFoundError("removed"), history.Begin(1).Remove("removed"))
		})

		t.Run("remove directory entry below a file", func(t *testing.T) {
			require.Equal(t, newNotDirectoryError("refs/heads/main"), history.Begin(1).Remove("refs/heads/main/child"))
		})

		t.Run("remove directory entry below negative node", func(t *testing.T) {
			require.Equal(t, newNotDirectoryError("removed"), history.Begin(1).CreateDirectory("removed/directory-entry"))
		})

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					1: {
						"refs/heads":      {},
						"refs/heads/main": {},
						"removed":         {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"refs/heads":      1,
					"refs/heads/main": 1,
					"removed":         1,
				},
				root: &node{
					nodeType:              directoryNode,
					knownDirectoryEntries: 1,
					children: children{
						"refs": {
							nodeType:              directoryNode,
							knownDirectoryEntries: 1,
							children: children{
								"heads": {
									nodeType:              directoryNode,
									lsn:                   1,
									knownDirectoryEntries: 1,
									children: children{
										"main": {
											nodeType: fileNode,
											lsn:      1,
											children: children{},
										},
									},
								},
							},
						},
						"removed": {
							nodeType: negativeNode,
							lsn:      1,
							children: children{},
						},
					},
				},
			},
			history,
		)
	})

	t.Run("read verification", func(t *testing.T) {
		history := New()

		tx1 := history.Begin(0)
		require.NoError(t, tx1.Remove("parent/negative"))
		require.NoError(t, tx1.CreateFile("parent/file"))
		require.NoError(t, tx1.CreateDirectory("parent/directory"))
		tx1.Commit(1)

		tx2 := history.Begin(0)
		// The parent is not considered modified by the changes in its children.
		require.NoError(t, tx2.Read("parent"))
		// Reading unmodified path does not conflict.
		require.NoError(t, tx2.Read("parent/unmodified"))
		// Reading any of the paths modified at a later LSN conflicts.
		require.Equal(t, newConflictingOperation("parent/negative", 0, 1), tx2.Read("parent/negative"))
		require.Equal(t, newConflictingOperation("parent/file", 0, 1), tx2.Read("parent/file"))
		require.Equal(t, newConflictingOperation("parent/directory", 0, 1), tx2.Read("parent/directory"))

		// This transaction was reading already at LSN 1 and does not conflict with its changes.
		tx3 := history.Begin(1)
		require.NoError(t, tx3.Read("parent"))
		require.NoError(t, tx3.Read("parent/unmodified"))
		require.NoError(t, tx3.Read("parent/negative"))
		require.NoError(t, tx3.Read("parent/file"))
		require.NoError(t, tx3.Read("parent/directory"))
		tx3.Commit(2)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					1: {
						"parent/negative":  {},
						"parent/file":      {},
						"parent/directory": {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"parent/negative":  1,
					"parent/file":      1,
					"parent/directory": 1,
				},
				root: &node{
					nodeType:              directoryNode,
					knownDirectoryEntries: 1,
					children: children{
						"parent": {
							nodeType:              directoryNode,
							knownDirectoryEntries: 2,
							children: children{
								"negative": &node{
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
								"file": &node{
									nodeType: fileNode,
									lsn:      1,
									children: children{},
								},
								"directory": &node{
									nodeType: directoryNode,
									lsn:      1,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)
	})

	t.Run("later transactions can overwrite previous ones", func(t *testing.T) {
		history := New()

		tx := history.Begin(0)
		require.NoError(t, tx.CreateFile("file"))
		require.NoError(t, tx.CreateDirectory("directory"))
		require.NoError(t, tx.Remove("negative/negative"))
		require.NoError(t, tx.Remove("negative"))
		tx.Commit(1)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					1: {
						"file":              {},
						"directory":         {},
						"negative/negative": {},
						"negative":          {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"file":              1,
					"directory":         1,
					"negative/negative": 1,
					"negative":          1,
				},
				root: &node{
					nodeType:              directoryNode,
					knownDirectoryEntries: 2,
					children: children{
						"file": {
							nodeType: fileNode,
							lsn:      1,
							children: children{},
						},
						"directory": {
							nodeType: directoryNode,
							lsn:      1,
							children: children{},
						},
						"negative": {
							nodeType: negativeNode,
							lsn:      1,
							children: children{
								"negative": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)

		tx = history.Begin(1)
		require.NoError(t, tx.Remove("file"))
		require.NoError(t, tx.Remove("directory"))
		require.NoError(t, tx.CreateDirectory("negative"))
		require.NoError(t, tx.CreateFile("negative/negative"))
		tx.Commit(2)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					2: {
						"file":              {},
						"directory":         {},
						"negative/negative": {},
						"negative":          {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"file":              2,
					"directory":         2,
					"negative/negative": 2,
					"negative":          2,
				},
				root: &node{
					nodeType:              directoryNode,
					knownDirectoryEntries: 1,
					children: children{
						"file": {
							nodeType: negativeNode,
							lsn:      2,
							children: children{},
						},
						"directory": {
							nodeType: negativeNode,
							lsn:      2,
							children: children{},
						},
						"negative": {
							nodeType:              directoryNode,
							lsn:                   2,
							knownDirectoryEntries: 1,
							children: children{
								"negative": {
									nodeType: fileNode,
									lsn:      2,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)
	})

	t.Run("evict writes of an LSN", func(t *testing.T) {
		history := New()

		tx1 := history.Begin(0)

		require.NoError(t, tx1.Remove("recreated-directory-2/removed-file-1"))
		require.NoError(t, tx1.Remove("recreated-directory-2"))

		require.NoError(t, tx1.CreateDirectory("directory-1"))

		require.NoError(t, tx1.Remove("non-empty-negatives/negative-1"))
		require.NoError(t, tx1.CreateDirectory("non-empty-directories/directory-1"))
		require.NoError(t, tx1.CreateFile("non-empty-files/file-1"))
		tx1.Commit(1)

		tx2 := history.Begin(1)
		require.NoError(t, tx2.CreateDirectory("recreated-directory-2"))

		require.NoError(t, tx2.CreateDirectory("directory-1/directory-2"))

		require.NoError(t, tx2.Remove("non-empty-negatives/negative-2"))
		require.NoError(t, tx2.CreateDirectory("non-empty-directories/directory-2"))
		require.NoError(t, tx2.CreateFile("non-empty-files/file-2"))
		tx2.Commit(2)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					1: {
						"recreated-directory-2/removed-file-1": {},

						"directory-1": {},

						"non-empty-negatives/negative-1":    {},
						"non-empty-directories/directory-1": {},
						"non-empty-files/file-1":            {},
					},
					2: {
						"recreated-directory-2":             {},
						"directory-1/directory-2":           {},
						"non-empty-negatives/negative-2":    {},
						"non-empty-directories/directory-2": {},
						"non-empty-files/file-2":            {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"recreated-directory-2/removed-file-1": 1,
					"directory-1":                          1,
					"non-empty-negatives/negative-1":       1,
					"non-empty-directories/directory-1":    1,
					"non-empty-files/file-1":               1,

					"recreated-directory-2":             2,
					"directory-1/directory-2":           2,
					"non-empty-negatives/negative-2":    2,
					"non-empty-directories/directory-2": 2,
					"non-empty-files/file-2":            2,
				},
				root: &node{
					nodeType:              directoryNode,
					knownDirectoryEntries: 5,
					children: children{
						"recreated-directory-2": {
							nodeType: directoryNode,
							lsn:      2,
							children: children{
								"removed-file-1": {
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
							},
						},
						"directory-1": {
							nodeType:              directoryNode,
							lsn:                   1,
							knownDirectoryEntries: 1,
							children: children{
								"directory-2": &node{
									nodeType: directoryNode,
									lsn:      2,
									children: children{},
								},
							},
						},
						"non-empty-negatives": {
							nodeType: directoryNode,
							children: children{
								"negative-1": &node{
									nodeType: negativeNode,
									lsn:      1,
									children: children{},
								},
								"negative-2": &node{
									nodeType: negativeNode,
									lsn:      2,
									children: children{},
								},
							},
						},
						"non-empty-directories": {
							nodeType:              directoryNode,
							knownDirectoryEntries: 2,
							children: children{
								"directory-1": &node{
									nodeType: directoryNode,
									lsn:      1,
									children: children{},
								},
								"directory-2": &node{
									nodeType: directoryNode,
									lsn:      2,
									children: children{},
								},
							},
						},
						"non-empty-files": {
							nodeType:              directoryNode,
							knownDirectoryEntries: 2,
							children: children{
								"file-1": &node{
									nodeType: fileNode,
									lsn:      1,
									children: children{},
								},
								"file-2": &node{
									nodeType: fileNode,
									lsn:      2,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)

		history.EvictLSN(1)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{
					2: {
						"recreated-directory-2":             {},
						"directory-1/directory-2":           {},
						"non-empty-negatives/negative-2":    {},
						"non-empty-directories/directory-2": {},
						"non-empty-files/file-2":            {},
					},
				},
				lsnByPath: map[string]storage.LSN{
					"recreated-directory-2":             2,
					"directory-1/directory-2":           2,
					"non-empty-negatives/negative-2":    2,
					"non-empty-directories/directory-2": 2,
					"non-empty-files/file-2":            2,
				},
				root: &node{
					nodeType:              directoryNode,
					knownDirectoryEntries: 5,
					children: children{
						"recreated-directory-2": {
							nodeType: directoryNode,
							lsn:      2,
							children: children{},
						},
						"directory-1": {
							nodeType:              directoryNode,
							lsn:                   1,
							knownDirectoryEntries: 1,
							children: children{
								"directory-2": &node{
									nodeType: directoryNode,
									lsn:      2,
									children: children{},
								},
							},
						},
						"non-empty-negatives": {
							nodeType: directoryNode,
							children: children{
								"negative-2": &node{
									nodeType: negativeNode,
									lsn:      2,
									children: children{},
								},
							},
						},
						"non-empty-directories": {
							nodeType:              directoryNode,
							knownDirectoryEntries: 1,
							children: children{
								"directory-2": &node{
									nodeType: directoryNode,
									lsn:      2,
									children: children{},
								},
							},
						},
						"non-empty-files": {
							nodeType:              directoryNode,
							knownDirectoryEntries: 1,
							children: children{
								"file-2": &node{
									nodeType: fileNode,
									lsn:      2,
									children: children{},
								},
							},
						},
					},
				},
			},
			history,
		)

		history.EvictLSN(2)

		require.Equal(t,
			&History{
				pathsByLSN: map[storage.LSN]map[string]struct{}{},
				lsnByPath:  map[string]storage.LSN{},
				root: &node{
					nodeType: directoryNode,
					children: children{},
				},
			},
			history,
		)
	})
}
