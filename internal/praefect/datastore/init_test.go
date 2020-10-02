// +build postgres

package datastore

import (
	"log"
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/datastore/glsql"
)

func TestMain(m *testing.M) {
	code := m.Run()
	// Clean closes connection to database once all tests are done
	if err := glsql.Clean(); err != nil {
		log.Fatalln(err, "database disconnection failure")
	}
	os.Exit(code)
}

func getDB(t testing.TB) glsql.DB        { return glsql.GetDB(t, "datastore") }
func getDBConfig(t testing.TB) config.DB { return glsql.GetDBConfig(t, "datastore") }
