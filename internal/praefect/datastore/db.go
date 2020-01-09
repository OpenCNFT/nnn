package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// Blank import to enable integration of github.com/lib/pq into database/sql
	_ "github.com/lib/pq"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
)

// CheckPostgresVersion checks the server version of the Postgres DB
// specified in conf. This is a diagnostic for the Praefect Postgres
// rollout. https://gitlab.com/gitlab-org/gitaly/issues/1755
func CheckPostgresVersion(conf config.Config) error {
	db, err := sql.Open("postgres", conf.DB.ToPQString())
	if err != nil {
		return fmt.Errorf("sql open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var serverVersion int
	if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&serverVersion); err != nil {
		return fmt.Errorf("get postgres server version: %v", err)
	}

	const minimumServerVersion = 90600 // Postgres 9.6
	if serverVersion < minimumServerVersion {
		return fmt.Errorf("postgres server version too old: %d", serverVersion)
	}

	return nil
}
