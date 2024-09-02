package storagectx

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	grpc_metadata "google.golang.org/grpc/metadata"
)

type keyTransaction struct{}

// ContextWithTransaction stores the transaction into the context.
func ContextWithTransaction(ctx context.Context, tx storage.Transaction) context.Context {
	return context.WithValue(ctx, keyTransaction{}, tx)
}

// ExtractTransaction extracts the transaction from the context. Nil is returned if there's
// no transaction in the context.
func ExtractTransaction(ctx context.Context) storage.Transaction {
	value := ctx.Value(keyTransaction{})
	if value == nil {
		return nil
	}

	return value.(storage.Transaction)
}

const keyPartitioningHint = "gitaly-partitioning-hint"

// ContextWithPartitioningHint stores the relativePath as a partitioning hint into the incoming
// gRPC metadata in the context.
func ContextWithPartitioningHint(ctx context.Context, relativePath string) context.Context {
	md, ok := grpc_metadata.FromIncomingContext(ctx)
	if !ok {
		md = grpc_metadata.New(nil)
	} else {
		md = md.Copy()
	}
	md.Set(keyPartitioningHint, relativePath)

	return grpc_metadata.NewIncomingContext(ctx, md)
}

// ExtractPartitioningHint extracts the partitioning hint from the incoming gRPC
// metadata in the context. Empty string is returned if no partitioning hint was provided.
// An error is returned if the metadata in the context contained multiple partitioning hints.
func ExtractPartitioningHint(ctx context.Context) (string, error) {
	relativePaths := grpc_metadata.ValueFromIncomingContext(ctx, keyPartitioningHint)
	if len(relativePaths) > 1 {
		return "", errors.New("multiple partitioning hints")
	}

	if len(relativePaths) == 0 {
		// No partitioning hint was set.
		return "", nil
	}

	return relativePaths[0], nil
}

// RemovePartitioningHintFromIncomingContext removes the partitioning hint from the provided context.
func RemovePartitioningHintFromIncomingContext(ctx context.Context) context.Context {
	md, ok := grpc_metadata.FromIncomingContext(ctx)
	if !ok {
		md = grpc_metadata.New(nil)
	} else {
		md = md.Copy()
	}
	md.Delete(keyPartitioningHint)

	return grpc_metadata.NewIncomingContext(ctx, md)
}
