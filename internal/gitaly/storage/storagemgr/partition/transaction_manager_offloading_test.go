package partition

import (
	"context"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	housekeepingcfg "gitlab.com/gitlab-org/gitaly/v16/internal/git/housekeeping/config"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

type customSetupFunc func(*testing.T, context.Context, storage.PartitionID, string) testTransactionSetup

func generateOffloadingPackRefsTests(t *testing.T, ctx context.Context, testPartitionID storage.PartitionID, relativePath string) []transactionTestCase {

	var _ []git.ObjectID

	customSetup := func(t *testing.T, ctx context.Context, testPartitionID storage.PartitionID, relativePath string) testTransactionSetup {
		setup := setupTest(t, ctx, testPartitionID, relativePath)
		_ = gittest.WriteBlobs(t, setup.Config, setup.RepositoryPath, 3)
		gittest.WriteRef(t, setup.Config, setup.RepositoryPath, "refs/heads/main", setup.Commits.First.OID)
		gittest.WriteRef(t, setup.Config, setup.RepositoryPath, "refs/heads/branch-1", setup.Commits.Second.OID)
		gittest.WriteRef(t, setup.Config, setup.RepositoryPath, "refs/heads/branch-2", setup.Commits.Third.OID)

		gittest.WriteTag(t, setup.Config, setup.RepositoryPath, "v1.0.0", setup.Commits.Diverging.OID.Revision())
		annotatedTag := gittest.WriteTag(t, setup.Config, setup.RepositoryPath, "v2.0.0", setup.Commits.Diverging.OID.Revision(), gittest.WriteTagConfig{
			Message: "annotated tag",
		})
		setup.AnnotatedTags = append(setup.AnnotatedTags, testTransactionTag{
			Name: "v2.0.0",
			OID:  annotatedTag,
		})

		gittest.Exec(t, setup.Config, "-C", setup.RepositoryPath, "gc")

		return setup
	}

	setup := customSetup(t, ctx, testPartitionID, relativePath)
	lightweightTag := setup.Commits.Diverging.OID
	annotatedTag := setup.AnnotatedTags[0]

	expectedObjectIDs := []git.ObjectID{
		setup.Commits.First.OID, setup.Commits.Second.OID, setup.Commits.Third.OID,
		lightweightTag, annotatedTag.OID,
		setup.ObjectHash.EmptyTreeOID}

	//[core]
	//repositoryformatversion = 0
	//filemode = true
	//bare = true
	//ignorecase = true
	//precomposeunicode = true
	//[remote "offload"]
	//url = file:///Users/peijian/Gitlab/playground/promise-orgin/local-promisor/mb-file-gamer-promisor.git
	//promisor = true
	//fetch = +refs/heads/*:refs/remotes/gsremote/*

	// expectations:
	// 1. blobs are not in the pack files
	// 2. config files are have a new section about offloading

	// A way to add test case
	// 1. arry of test case description
	// 2. a function to get setup,  steps and expectedResult
	// 3. a funciton new Test case, para: descrition, setup, setpes, expect
	// when a new test is added, added description and add new map to setup, steps and expect

	return []transactionTestCase{
		offloadingTestCase1("just 1 transaction", customSetup, setup,
			expectedObjectIDs, lightweightTag, annotatedTag.OID),
		//offloadingTestCase2("2 transaction: repo deletion, then offloading", customSetup, setup,
		//	expectedObjectIDs, lightweightTag, annotatedTag.OID),
		//offloadingTestCase3("2 transaction: repo offloading, then deletion", customSetup, setup,
		//	expectedObjectIDs, lightweightTag, annotatedTag.OID),
		offloadingTestCase4("2 transaction: repo offloading, then deletion", customSetup, setup,
			expectedObjectIDs, lightweightTag, annotatedTag.OID),
		// newTestCase(setup, steps, expectation)
	}
}

func offloadingTestCase1(desc string, customSetup customSetupFunc, setup testTransactionSetup,
	expectedOIDs []git.ObjectID, lightweightTag, annotatedTagOID git.ObjectID) transactionTestCase {

	testingSteps := steps{
		StartManager{},
		Begin{
			TransactionID: 1,
			RelativePaths: []string{setup.RelativePath},
		},
		MarkOffloading{
			TransactionID: 1,
			Config: housekeepingcfg.OffloadingConfig{
				Bucket: "blob_offloads",
				Filter: "blob:none",
				Prefix: setup.RelativePath,
				// FilterToDir: this will bre reset to snapshot repo's dir
			},
		},
		Commit{
			TransactionID: 1,
		},
	}

	expectReferencesFileBackend := &FilesBackendState{
		PackedReferences: map[git.ReferenceName]git.ObjectID{
			"refs/heads/main":     setup.Commits.First.OID,
			"refs/heads/branch-1": setup.Commits.Second.OID,
			"refs/heads/branch-2": setup.Commits.Third.OID,
			"refs/tags/v1.0.0":    lightweightTag,
			"refs/tags/v2.0.0":    annotatedTagOID,
		},
		LooseReferences: map[git.ReferenceName]git.ObjectID{},
	}
	expectReferencesState := &ReferencesState{
		FilesBackend:    expectReferencesFileBackend,
		ReftableBackend: nil,
	}
	expectPackfiles := &PackfilesState{
		LooseObjects: nil,
		Packfiles: []*PackfileState{
			{
				Objects:         expectedOIDs,
				HasReverseIndex: true,
			},
		},
	}

	return transactionTestCase{
		desc:        desc,
		customSetup: customSetup,
		steps:       testingSteps,
		expectedState: StateAssertion{
			Database: DatabaseState{
				string(keyAppliedLSN): storage.LSN(1).ToProto(),
			},
			Repositories: RepositoryStates{
				setup.RelativePath: {
					References: gittest.FilesOrReftables(expectReferencesState, nil),
					Packfiles:  expectPackfiles,
				},
			},
		},
	}
}

func offloadingTestCase2(desc string, customSetup customSetupFunc, setup testTransactionSetup,
	expectedOIDs []git.ObjectID, lightweightTag, annotatedTagOID git.ObjectID) transactionTestCase {

	// Setup
	// Steps
	// Expectation

	testingSteps := steps{
		StartManager{},
		Begin{
			TransactionID: 1,
			RelativePaths: []string{setup.RelativePath},
		},
		Begin{
			TransactionID: 2,
			RelativePaths: []string{setup.RelativePath},
		},
		MarkOffloading{
			TransactionID: 2,
			Config: housekeepingcfg.OffloadingConfig{
				Bucket: "blob_offloads",
				Filter: "blob:none",
				Prefix: setup.RelativePath,
				// FilterToDir: this will bre reset to snapshot repo's dir
			},
		},
		Commit{
			TransactionID:    1,
			DeleteRepository: true,
		},
		Commit{
			TransactionID: 2,
			ExpectedError: storage.ErrRepositoryNotFound, // TODO this should fail
		},
	}

	return transactionTestCase{
		desc:        desc,
		customSetup: customSetup,
		steps:       testingSteps,
		expectedState: StateAssertion{
			Database: DatabaseState{
				string(keyAppliedLSN): storage.LSN(1).ToProto(),
			},
			Repositories: RepositoryStates{},
		},
	}
}

func offloadingTestCase3(desc string, customSetup customSetupFunc, setup testTransactionSetup,
	expectedOIDs []git.ObjectID, lightweightTag, annotatedTagOID git.ObjectID) transactionTestCase {

	// offload is committed and processed first, them repo deletion
	testingSteps := steps{
		StartManager{},
		Begin{
			TransactionID: 1,
			RelativePaths: []string{setup.RelativePath},
		},
		Begin{
			TransactionID: 2,
			RelativePaths: []string{setup.RelativePath},
		},
		MarkOffloading{
			TransactionID: 1,
			Config: housekeepingcfg.OffloadingConfig{
				Bucket: "blob_offloads",
				Filter: "blob:none",
				Prefix: setup.RelativePath,
				// FilterToDir: this will bre reset to snapshot repo's dir
			},
		},
		Commit{
			TransactionID: 1,
			ExpectedError: nil,
		},
		Commit{
			TransactionID:    2,
			DeleteRepository: true,
		},
	}

	return transactionTestCase{
		desc:        desc,
		customSetup: customSetup,
		steps:       testingSteps,
		expectedState: StateAssertion{
			Database: DatabaseState{
				string(keyAppliedLSN): storage.LSN(2).ToProto(),
			},
			Repositories: RepositoryStates{},
		},
	}
}

func offloadingTestCase4(desc string, customSetup customSetupFunc, setup testTransactionSetup,
	expectedOIDs []git.ObjectID, lightweightTag, annotatedTagOID git.ObjectID) transactionTestCase {
	// do some write, e.g. delete a blob that should be offloaded then offload should be aborted (what is uploaded should be
	// cleaned up)

	// Do a housekeeping repack with config RepackObjectsStrategyFullWithCruft
	// - since our blobs are not with any commit, they will be pruned
	// Do an offloading with another transaction
	// - our offloading includes the blobs, so this will trigger a conflict,
	// - I am expecting verifyOffloading can detect that and return with error
	// - meanwhile, we can check how clean what is uploaded

	testingSteps := steps{
		StartManager{},
		Begin{
			TransactionID: 1,
			RelativePaths: []string{setup.RelativePath},
		},
		Begin{
			TransactionID: 2,
			RelativePaths: []string{setup.RelativePath},
		},
		RunRepack{
			TransactionID: 1,
			Config: housekeepingcfg.RepackObjectsConfig{
				Strategy:            housekeepingcfg.RepackObjectsStrategyFullWithCruft,
				WriteBitmap:         true,
				WriteMultiPackIndex: true,
			},
		},
		MarkOffloading{
			TransactionID: 2,
			Config: housekeepingcfg.OffloadingConfig{
				Bucket: "blob_offloads",
				Filter: "blob:none",
				Prefix: setup.RelativePath,
				// FilterToDir: this will bre reset to snapshot repo's dir
			},
		},
		Commit{
			TransactionID: 1,
			ExpectedError: nil,
		},
		Commit{
			TransactionID: 2,
			ExpectedError: errOffloadingConflictHousekeeping,
		},
	}

	expectReferencesFileBackend := &FilesBackendState{
		PackedReferences: map[git.ReferenceName]git.ObjectID{
			"refs/heads/main":     setup.Commits.First.OID,
			"refs/heads/branch-1": setup.Commits.Second.OID,
			"refs/heads/branch-2": setup.Commits.Third.OID,
			"refs/tags/v1.0.0":    lightweightTag,
			"refs/tags/v2.0.0":    annotatedTagOID,
		},
		LooseReferences: map[git.ReferenceName]git.ObjectID{},
	}
	expectReferencesState := &ReferencesState{
		FilesBackend:    expectReferencesFileBackend,
		ReftableBackend: nil,
	}
	expectPackfiles := &PackfilesState{
		LooseObjects: nil,
		Packfiles: []*PackfileState{
			{
				Objects:         expectedOIDs,
				HasReverseIndex: true,
			},
		},
		HasMultiPackIndex: true,
	}

	return transactionTestCase{
		desc:        desc,
		customSetup: customSetup,
		steps:       testingSteps,
		expectedState: StateAssertion{
			Database: DatabaseState{
				string(keyAppliedLSN): storage.LSN(1).ToProto(),
			},
			Repositories: RepositoryStates{
				setup.RelativePath: {
					DefaultBranch:       "refs/heads/main",
					References:          expectReferencesState,
					Packfiles:           expectPackfiles,
					FullRepackTimestamp: &FullRepackTimestamp{Exists: true},
				},
			},
		},
	}
}
