package storagectx

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	grpc_metadata "google.golang.org/grpc/metadata"
)

type nilTransaction struct{ storage.Transaction }

func TestContextWithTransaction(t *testing.T) {
	t.Run("no transaction in context", func(t *testing.T) {
		RunWithTransaction(context.Background(), func(tx storage.Transaction) {
			t.Fatalf("callback should not be executed without transaction")
		})
	})

	t.Run("transaction in context", func(t *testing.T) {
		callbackRan := false
		expectedTX := &nilTransaction{}

		RunWithTransaction(
			ContextWithTransaction(context.Background(), expectedTX),
			func(tx storage.Transaction) {
				require.Same(t, expectedTX, tx)
				require.NotSame(t, tx, &nilTransaction{})
				callbackRan = true
			},
		)

		require.True(t, callbackRan)
	})
}

func TestPartitioningHint(t *testing.T) {
	t.Run("no hint provided", func(t *testing.T) {
		ctx := context.Background()

		relativePath, err := ExtractPartitioningHintFromIncomingContext(ctx)
		require.NoError(t, err)
		require.Empty(t, relativePath)
	})

	t.Run("hint provided", func(t *testing.T) {
		ctx := SetPartitioningHintToIncomingContext(context.Background(), "relative-path")

		relativePath, err := ExtractPartitioningHintFromIncomingContext(ctx)
		require.NoError(t, err)
		require.Equal(t, relativePath, "relative-path")
	})

	t.Run("doesn't modify original metadata", func(t *testing.T) {
		originalMetadata := grpc_metadata.New(nil)
		originalCtx := grpc_metadata.NewIncomingContext(context.Background(), originalMetadata)

		ctx := SetPartitioningHintToIncomingContext(originalCtx, "relative-path")

		relativePath, err := ExtractPartitioningHintFromIncomingContext(ctx)
		require.NoError(t, err)
		require.Equal(t, relativePath, "relative-path")

		relativePath, err = ExtractPartitioningHintFromIncomingContext(originalCtx)
		require.NoError(t, err)
		require.Empty(t, relativePath)
	})

	t.Run("fails if multiple hints set", func(t *testing.T) {
		md := grpc_metadata.New(nil)
		md.Set(keyPartitioningHint, "relative-path-1", "relative-path-2")

		relativePath, err := ExtractPartitioningHintFromIncomingContext(
			grpc_metadata.NewIncomingContext(context.Background(), md),
		)
		require.Equal(t, errors.New("multiple partitioning hints"), err)
		require.Empty(t, relativePath)
	})

	t.Run("removes the hint", func(t *testing.T) {
		ctx := SetPartitioningHintToIncomingContext(context.Background(), "relative-path")

		relativePath, err := ExtractPartitioningHintFromIncomingContext(ctx)
		require.NoError(t, err)
		require.Equal(t, relativePath, "relative-path")

		ctx = RemovePartitioningHintFromIncomingContext(ctx)
		relativePath, err = ExtractPartitioningHintFromIncomingContext(ctx)
		require.NoError(t, err)
		require.Equal(t, relativePath, "")
	})
}
