package partition

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
)

func generateConsumerTests(t *testing.T, setup testTransactionSetup) []transactionTestCase {
	customSetup := func(t *testing.T, ctx context.Context, testPartitionID storage.PartitionID, relativePath string) testTransactionSetup {
		setup := setupTest(t, ctx, testPartitionID, relativePath)
		setup.Consumer = &MockLogConsumer{}

		return setup
	}

	return []transactionTestCase{
		{
			desc:        "unacknowledged entry not pruned",
			customSetup: customSetup,
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Directory: gittest.FilesOrReftables(testhelper.DirectoryState{
					"/":                           {Mode: mode.Directory},
					"/wal":                        {Mode: mode.Directory},
					"/wal/0000000000001":          {Mode: mode.Directory},
					"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(refChangeLogEntry(setup, "refs/heads/main", setup.Commits.First.OID)),
					"/wal/0000000000001/1":        {Mode: mode.File, Content: []byte(setup.Commits.First.OID + "\n")},
				}, buildReftableDirectory(map[int][]git.ReferenceUpdates{
					1: {{"refs/heads/main": git.ReferenceUpdate{NewOID: setup.Commits.First.OID}}},
				})),
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
					},
				},
			},
		},
		{
			desc:        "acknowledged entry pruned",
			customSetup: customSetup,
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				ConsumerAcknowledge{
					LSN: 1,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":    {Mode: mode.Directory},
					"/wal": {Mode: mode.Directory},
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
					},
				},
			},
		},
		{
			desc:        "acknowledging a later entry prunes prior entries",
			customSetup: customSetup,
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.Commits.First.OID, NewOID: setup.Commits.Second.OID},
					},
				},
				ConsumerAcknowledge{
					LSN: 2,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(2).ToProto(),
				},
				Directory: testhelper.DirectoryState{
					"/":    {Mode: mode.Directory},
					"/wal": {Mode: mode.Directory},
				},
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.Second.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
										{
											MinIndex: 3,
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.Second.OID.String(),
												},
											},
										},
									},
								},
							},
						),
					},
				},
			},
		},
		{
			desc:        "dependent transaction blocks pruning acknowledged entry",
			customSetup: customSetup,
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				ConsumerAcknowledge{
					LSN: 1,
				},
				CloseManager{},
				Commit{
					TransactionID: 2,
					ExpectedError: storage.ErrTransactionProcessingStopped,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Directory: gittest.FilesOrReftables(testhelper.DirectoryState{
					"/":                           {Mode: mode.Directory},
					"/wal":                        {Mode: mode.Directory},
					"/wal/0000000000001":          {Mode: mode.Directory},
					"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(refChangeLogEntry(setup, "refs/heads/main", setup.Commits.First.OID)),
					"/wal/0000000000001/1":        {Mode: mode.File, Content: []byte(setup.Commits.First.OID + "\n")},
				}, buildReftableDirectory(map[int][]git.ReferenceUpdates{
					1: {{"refs/heads/main": git.ReferenceUpdate{NewOID: setup.Commits.First.OID}}},
				})),
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
					},
				},
			},
		},
		{
			desc:        "consumer position zeroed lsn on restart",
			customSetup: customSetup,
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				ConsumerAcknowledge{
					LSN: 1,
				},
				Begin{
					TransactionID:       2,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 1,
				},
				Commit{
					TransactionID: 2,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/other": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Second.OID},
					},
				},
				Begin{
					TransactionID:       3,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 2,
				},
				Begin{
					TransactionID:       4,
					RelativePaths:       []string{setup.RelativePath},
					ExpectedSnapshotLSN: 2,
				},
				ConsumerAcknowledge{
					LSN: 2,
				},
				Commit{
					TransactionID: 3,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/third": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.Third.OID},
					},
				},
				ConsumerAcknowledge{
					LSN: 3,
				},
				CloseManager{},
				StartManager{},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(3).ToProto(),
				},
				Directory: gittest.FilesOrReftables(testhelper.DirectoryState{
					"/":                           {Mode: mode.Directory},
					"/wal":                        {Mode: mode.Directory},
					"/wal/0000000000002":          {Mode: mode.Directory},
					"/wal/0000000000002/MANIFEST": manifestDirectoryEntry(refChangeLogEntry(setup, "refs/heads/other", setup.Commits.Second.OID)),
					"/wal/0000000000002/1":        {Mode: mode.File, Content: []byte(setup.Commits.Second.OID + "\n")},
					"/wal/0000000000003":          {Mode: mode.Directory},
					"/wal/0000000000003/MANIFEST": manifestDirectoryEntry(refChangeLogEntry(setup, "refs/heads/third", setup.Commits.Third.OID)),
					"/wal/0000000000003/1":        {Mode: mode.File, Content: []byte(setup.Commits.Third.OID + "\n")},
				}, buildReftableDirectory(map[int][]git.ReferenceUpdates{
					2: {{"refs/heads/other": git.ReferenceUpdate{NewOID: setup.Commits.Second.OID}}},
					3: {{"refs/heads/third": git.ReferenceUpdate{NewOID: setup.Commits.Third.OID}}},
				})),
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main":  setup.Commits.First.OID,
										"refs/heads/other": setup.Commits.Second.OID,
										"refs/heads/third": setup.Commits.Third.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
										{
											MinIndex: 3,
											MaxIndex: 3,
											References: []git.Reference{
												{
													Name:   "refs/heads/other",
													Target: setup.Commits.Second.OID.String(),
												},
											},
										},
										{
											MinIndex: 4,
											MaxIndex: 4,
											References: []git.Reference{
												{
													Name:   "refs/heads/third",
													Target: setup.Commits.Third.OID.String(),
												},
											},
										},
									},
								},
							},
						),
					},
				},
			},
		},
		{
			desc:        "stopped manager does not prune acknowledged entry",
			customSetup: customSetup,
			steps: steps{
				StartManager{},
				Begin{
					TransactionID: 1,
					RelativePaths: []string{setup.RelativePath},
				},
				Commit{
					TransactionID: 1,
					ReferenceUpdates: git.ReferenceUpdates{
						"refs/heads/main": {OldOID: setup.ObjectHash.ZeroOID, NewOID: setup.Commits.First.OID},
					},
				},
				CloseManager{},
				ConsumerAcknowledge{
					LSN: 1,
				},
			},
			expectedState: StateAssertion{
				Database: DatabaseState{
					string(keyAppliedLSN): storage.LSN(1).ToProto(),
				},
				Directory: gittest.FilesOrReftables(testhelper.DirectoryState{
					"/":                           {Mode: mode.Directory},
					"/wal":                        {Mode: mode.Directory},
					"/wal/0000000000001":          {Mode: mode.Directory},
					"/wal/0000000000001/MANIFEST": manifestDirectoryEntry(refChangeLogEntry(setup, "refs/heads/main", setup.Commits.First.OID)),
					"/wal/0000000000001/1":        {Mode: mode.File, Content: []byte(setup.Commits.First.OID + "\n")},
				}, buildReftableDirectory(map[int][]git.ReferenceUpdates{
					1: {{"refs/heads/main": git.ReferenceUpdate{NewOID: setup.Commits.First.OID}}},
				})),
				Repositories: RepositoryStates{
					setup.RelativePath: {
						DefaultBranch: "refs/heads/main",
						References: gittest.FilesOrReftables(
							&ReferencesState{
								FilesBackend: &FilesBackendState{
									LooseReferences: map[git.ReferenceName]git.ObjectID{
										"refs/heads/main": setup.Commits.First.OID,
									},
								},
							}, &ReferencesState{
								ReftableBackend: &ReftableBackendState{
									Tables: []ReftableTable{
										{
											MinIndex: 1,
											MaxIndex: 1,
											References: []git.Reference{
												{
													Name:       "HEAD",
													Target:     "refs/heads/main",
													IsSymbolic: true,
												},
											},
										},
										{
											MinIndex: 2,
											MaxIndex: 2,
											References: []git.Reference{
												{
													Name:   "refs/heads/main",
													Target: setup.Commits.First.OID.String(),
												},
											},
										},
									},
								},
							},
						),
					},
				},
			},
		},
	}
}
