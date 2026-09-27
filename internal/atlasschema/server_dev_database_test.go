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

// TestRefuseDevServer_FailurePath refuses a dev URL naming no MySQL-family
// database on schema diff and schema apply, which do not take a dev server
// (stokaro/ptah#3789).
func TestRefuseDevServer_FailurePath(t *testing.T) {
	for _, devURL := range []string{"mysql://root@localhost:3306", "mariadb://root@localhost:3306/", "mysql+unix://root@/run/mysqld/mysqld.sock"} {
		t.Run(devURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasschema.RefuseDevServer(devURL), qt.ErrorIs, atlasschema.ErrDevServerUnsupported)
		})
	}
}

// TestRefuseDevServer_HappyPath is the control: a dev URL naming a database,
// another dialect, and no dev URL at all are not refused.
func TestRefuseDevServer_HappyPath(t *testing.T) {
	for _, devURL := range []string{"", "mysql://root@localhost:3306/dev", "postgres://localhost/dev", "docker://mysql/8/dev"} {
		t.Run(devURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasschema.RefuseDevServer(devURL), qt.IsNil)
		})
	}
}
