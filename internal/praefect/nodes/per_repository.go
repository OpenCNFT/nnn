package nodes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore/glsql"
)

// ErrNoPrimary is returned if the repository does not have a primary.
var ErrNoPrimary = errors.New("no primary")

// PerRepositoryElector implements an elector that selects a primary for each repository.
// It elects a healthy node with most recent generation as the primary. If all nodes are
// on the same generation, it picks one randomly to balance repositories in simple fashion.
type PerRepositoryElector struct {
	logger log.Logger
	db     glsql.Querier
}

// NewPerRepositoryElector returns a new per repository primary elector.
func NewPerRepositoryElector(logger log.Logger, db glsql.Querier) *PerRepositoryElector {
	return &PerRepositoryElector{
		logger: logger,
		db:     db,
	}
}

// GetPrimary returns the primary storage of a repository. If the current primary is invalid, a new primary
// is elected if there are valid candidates for promotion.
func (pr *PerRepositoryElector) GetPrimary(ctx context.Context, virtualStorage string, repositoryID int64) (string, error) {
	// The query below contains three parts to account for visibility with read-committed isolation mode and
	// concurrent updates.
	//
	// If the repository already has a valid primary, the `reread` and `election` CTEs don't return results and
	// the query simply returns the primary from the `repositories` table (aliased as `snapshot`). No locks are
	// acquired. Same happens if the repository has an invalid primary but there are no valid candidates for
	// promotion.
	//
	// If the primary is invalid, the `reread` CTE locks the record. Upon acquiring the lock, Postgres rereads
	// the record. `reread` then contains an up to date record which has potentially been updated by a concurrent
	// transaction. If the reread record still contains an invalid primary, the `election` CTE performs an election.
	// If the repository has a valid primary after rereading the record, the `election` CTE doesn't re-elect a primary.
	//
	// The query then returns the primary from the correct CTE. The priority is:
	//   1. `election`, as this indicates this transaction re-elected the primary and the CTE now contains the most
	//      recent change
	//   2. `reread`, as this indicates a concurrent transaction had potentially changed the primary.
	//   3. `snapshot`, if the current primary was valid in the transaction's database snapshot.
	var current, previous sql.NullString
	if err := pr.db.QueryRowContext(ctx, `
WITH reread AS (
	SELECT true AS valid, repository_id, "primary"
	FROM repositories
	WHERE repository_id = $1
	AND NOT EXISTS (
		SELECT FROM valid_primaries
		WHERE valid_primaries.repository_id = repositories.repository_id
		AND storage = "primary"
	) AND EXISTS (
		SELECT FROM valid_primaries
		WHERE valid_primaries.repository_id = repositories.repository_id
	)
	FOR NO KEY UPDATE
),

election AS (
	UPDATE repositories
	SET "primary" = (
		SELECT storage
		FROM valid_primaries
		WHERE valid_primaries.repository_id = repositories.repository_id
		ORDER BY random()
		LIMIT 1
	)
	FROM reread
	WHERE repositories.repository_id = reread.repository_id
	AND NOT EXISTS (
		SELECT FROM valid_primaries
		WHERE valid_primaries.repository_id = $1
		AND storage = reread.primary
	)
	RETURNING true AS valid, repositories.primary
)

SELECT
	CASE WHEN election.valid
		THEN election.primary
		ELSE
			CASE WHEN reread.valid
				THEN reread.primary
				ELSE snapshot.primary
			END
	END,
	CASE WHEN reread.valid
		THEN reread.primary
		ELSE snapshot.primary
	END
FROM repositories AS snapshot
LEFT JOIN reread ON reread.valid
LEFT JOIN election ON election.valid
WHERE snapshot.repository_id = $1
`,
		repositoryID,
	).Scan(&current, &previous); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", datastore.ErrRepositoryNotFound
		}

		return "", fmt.Errorf("scan: %w", err)
	}

	if current != previous {
		pr.logger.WithFields(log.Fields{
			"repository_id":    repositoryID,
			"current_primary":  current.String,
			"previous_primary": previous.String,
		}).InfoContext(ctx, "primary node changed")
	}

	if !current.Valid {
		return "", ErrNoPrimary
	}

	return current.String, nil
}
