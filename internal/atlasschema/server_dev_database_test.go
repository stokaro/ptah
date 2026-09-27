package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/atlasschema"
)

// TestRefuseServerDevDatabase_FailurePath refuses a dev database beside a
// whole MySQL-family server before the dev database is contacted: a dev
// server that replays a desired state as a server is not taken yet
// (stokaro/ptah#3789).
func TestRefuseServerDevDatabase_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := atlasschema.RefuseServerDevDatabase(
		catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}, "mysql://root@localhost:3307/dev",
	)

	c.Assert(err, qt.ErrorIs, atlasschema.ErrServerDevDatabase)
}

// TestRefuseServerDevDatabase_HappyPath is the control: no dev database, and a
// dev database beside one database or beside PostgreSQL, are not refused.
func TestRefuseServerDevDatabase_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		info   catalog.ServerInfo
		devURL string
	}{
		{name: "a whole server without a dev database", info: catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}},
		{
			name:   "one database with a dev database",
			info:   catalog.ServerInfo{Dialect: platform.MariaDB, Schema: "app"},
			devURL: "mariadb://root@localhost:3307/dev",
		},
		{
			name:   "a PostgreSQL connection with a dev database",
			info:   catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"},
			devURL: "postgres://localhost/dev",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasschema.RefuseServerDevDatabase(test.info, test.devURL), qt.IsNil)
		})
	}
}
