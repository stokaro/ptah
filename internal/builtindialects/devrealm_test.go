package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

// TestDescriptors_DevDatabases pins what each built-in descriptor states about
// its disposable databases, as the packages that read it switched on the
// dialect name before the descriptors carried it: internal/devlock for the
// query that names a dev database, the server identity of the MySQL family
// and the lock file of the engines whose server takes no advisory lock;
// migration/internal/scratchdb for the servers that create a database per
// test case; migration/internal/shadowdb for the schema reset; and
// migration/shadow for the engines whose schema is the selected database.
// Oracle states none of it, and a dev database there is refused.
func TestDescriptors_DevDatabases(t *testing.T) {
	const currentDatabase = "SELECT current_database()"
	tests := []struct {
		dialect          string
		want             descriptor.DevRealm
		schemaIsDatabase bool
	}{
		{dialect: platform.Postgres, want: descriptor.DevRealm{
			Identity: "query", Query: currentDatabase, ScratchDatabase: true,
			DropSchema: "DROP SCHEMA IF EXISTS %s CASCADE",
		}},
		{dialect: platform.CockroachDB, want: descriptor.DevRealm{
			Identity: "query", Query: currentDatabase, FileLock: "loopback", ScratchDatabase: true,
		}},
		{dialect: platform.YugabyteDB, want: descriptor.DevRealm{
			Identity: "query", Query: currentDatabase, ScratchDatabase: true,
		}},
		{dialect: platform.Spanner, want: descriptor.DevRealm{
			Identity: "query", Query: currentDatabase, ScratchDatabase: true,
		}},
		{dialect: platform.SQLServer, want: descriptor.DevRealm{Identity: "query", Query: "SELECT DB_NAME()"}},
		{dialect: platform.MySQL, want: descriptor.DevRealm{
			Identity: "query", Query: "SELECT DATABASE()", ServerQuery: "SELECT @@server_uuid", ScratchDatabase: true,
		}, schemaIsDatabase: true},
		{dialect: platform.MariaDB, want: descriptor.DevRealm{
			Identity: "query", Query: "SELECT DATABASE()",
			ServerQuery:     "SELECT CONCAT_WS(':', @@hostname, @@port, @@datadir)",
			ScratchDatabase: true,
		}, schemaIsDatabase: true},
		{dialect: platform.ClickHouse, want: descriptor.DevRealm{
			Identity: "query", Query: "SELECT currentDatabase()", FileLock: "loopback",
		}, schemaIsDatabase: true},
		{dialect: platform.SQLite, want: descriptor.DevRealm{
			Identity: "file", Query: "SELECT file FROM pragma_database_list WHERE name = 'main'", FileLock: "local",
		}},
		{dialect: platform.YDB, want: descriptor.DevRealm{Identity: "writer"}},
		{dialect: platform.Oracle, want: descriptor.DevRealm{}},
	}
	c := qt.New(t)
	c.Assert(tests, qt.HasLen, len(platform.Dialects().Dialects()))
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			described, found := platform.Lookup(test.dialect)
			c.Assert(found, qt.IsTrue)
			c.Assert(described.DevRealm, qt.DeepEquals, test.want)
			c.Assert(described.SchemaIsDatabase, qt.Equals, test.schemaIsDatabase)
		})
	}
}
