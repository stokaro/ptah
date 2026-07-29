//go:build integration

package mysql_test

import (
	"database/sql"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/dbschema/mysql"
)

func TestWriterDropDatabaseRealm_LiveRejectsProtectedDatabase(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dsnEnv  string
		dialect string
	}{
		{
			name:    "mysql",
			dsnEnv:  "MYSQL_ADMIN_TEST_DSN",
			dialect: platform.MySQL,
		},
		{
			name:    "mariadb",
			dsnEnv:  "MARIADB_ADMIN_TEST_DSN",
			dialect: platform.MariaDB,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			db := openMySQLWriterLiveDatabase(c, test.dsnEnv)
			c.Cleanup(func() {
				c.Check(db.Close(), qt.IsNil)
			})
			writer := mysql.NewMySQLWriter(db, "mysql", test.dialect)

			err := writer.DropDatabaseRealm(t.Context())

			c.Assert(err, qt.ErrorMatches, `mysql: refusing to clean protected database "mysql"`)
		})
	}
}

func openMySQLWriterLiveDatabase(c *qt.C, dsnEnv string) *sql.DB {
	c.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		c.Skipf("%s is not set", dsnEnv)
	}
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	c.Assert(db.PingContext(c.Context()), qt.IsNil)
	return db
}
