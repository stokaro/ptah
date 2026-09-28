package atlasschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlassource"
)

// wholeMySQLServer is a connection to a whole MySQL server.
var wholeMySQLServer = catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}

// classifiedTo classifies one --to source named name, written with contents
// in a temporary directory, or a migration directory when name ends in a
// slash.
func classifiedTo(c *qt.C, name, contents string) atlassource.Set {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	if name[len(name)-1] == '/' {
		c.Assert(os.MkdirAll(path, 0o750), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(path, "1_init.sql"), []byte(contents), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(path, "atlas.sum"), []byte("h1:x=\n"), 0o600), qt.IsNil)
	} else {
		c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
	}
	set, err := atlassource.ClassifySet("--to", []string{"file://" + filepath.ToSlash(path)}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)
	return set
}

// TestRefuseServerDevDatabase_FailurePath refuses a dev database beside a
// whole MySQL-family server before the dev database is contacted. The pinned
// community binary v1.3.0 refuses SQL and a migration directory there with
// `cannot diff a schema "dev" with a database connection`, measured on MySQL
// 8.4.11 and MariaDB 11.8.9 (stokaro/ptah#3885), and a document declaring
// several databases with `cannot use HCL with more than 1 schema when dev-url
// is limited to schema "dev"`. A document declaring one database, which that
// binary plans, and a --to database are refused in Ptah's words.
func TestRefuseServerDevDatabase_FailurePath(t *testing.T) {
	c := qt.New(t)

	sqlErr := atlasschema.RefuseServerDevDatabase(wholeMySQLServer, "mysql://root@localhost:3307/dev",
		classifiedTo(c, "realm.sql", "CREATE DATABASE app;\n"))
	dirErr := atlasschema.RefuseServerDevDatabase(wholeMySQLServer, "mysql://root@localhost:3307/dev",
		classifiedTo(c, "migrations/", "CREATE DATABASE app;\n"))
	realmErr := atlasschema.RefuseServerDevDatabase(wholeMySQLServer, "mysql://root@localhost:3307/dev",
		classifiedTo(c, "realm.hcl", `schema "app" {}`+"\n"+`schema "more" {}`+"\n"))
	oneErr := atlasschema.RefuseServerDevDatabase(wholeMySQLServer, "mysql://root@localhost:3307/dev",
		classifiedTo(c, "one.hcl", `schema "app" {}`+"\n"))
	databaseErr := atlasschema.RefuseServerDevDatabase(wholeMySQLServer, "mysql://root@localhost:3307/dev",
		atlassource.Set{Kind: atlassource.KindDatabase})

	c.Assert(sqlErr, qt.ErrorMatches, `cannot diff a schema "dev" with a database connection`)
	c.Assert(dirErr, qt.ErrorMatches, `cannot diff a schema "dev" with a database connection`)
	c.Assert(realmErr, qt.ErrorMatches, `cannot use HCL with more than 1 schema when dev-url is limited to schema "dev"`)
	c.Assert(oneErr, qt.ErrorIs, atlasschema.ErrServerDevDatabase)
	c.Assert(databaseErr, qt.ErrorIs, atlasschema.ErrServerDevDatabase)
}

// TestRefuseServerDevDatabase_HappyPath is the control: a whole dev server, no
// dev database, and a dev database beside one database or beside PostgreSQL
// are not refused.
func TestRefuseServerDevDatabase_HappyPath(t *testing.T) {
	c := qt.New(t)
	sql := classifiedTo(c, "realm.sql", "CREATE DATABASE app;\n")
	tests := []struct {
		name    string
		info    catalog.ServerInfo
		devURL  string
		desired atlassource.Set
	}{
		{name: "a whole dev server beside a whole server", info: wholeMySQLServer,
			devURL: "mysql://root@localhost:3307/", desired: sql},
		{name: "a whole server without a dev database", info: wholeMySQLServer, desired: sql},
		{
			name:    "one database with a dev database",
			info:    catalog.ServerInfo{Dialect: platform.MariaDB, Schema: "app"},
			devURL:  "mariadb://root@localhost:3307/dev",
			desired: sql,
		},
		{
			name:    "a PostgreSQL connection with a dev database",
			info:    catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"},
			devURL:  "postgres://localhost/dev",
			desired: sql,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasschema.RefuseServerDevDatabase(test.info, test.devURL, test.desired), qt.IsNil)
		})
	}
}
