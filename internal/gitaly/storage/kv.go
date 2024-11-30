package storage

// RepositoryKeyPrefix is the prefix used for storing keys recording repository
// existence in a partition.
const RepositoryKeyPrefix = "r/"

// RepositoryKey generates the database key for recording repository existence in a partition.
func RepositoryKey(relativePath string) []byte {
	return []byte(RepositoryKeyPrefix + relativePath)
}
