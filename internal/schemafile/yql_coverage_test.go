package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemafile"
)

// The SQL header can add limits, but its absence must not erase what the YQL
// reader cannot describe. Both CLI loading paths pass through this boundary.
func TestYQLSourceLimitsSurviveFileLoading(t *testing.T) {
	for _, load := range []struct {
		name string
		read func(string, schemafile.Options) (*schemamodel.Database, error)
	}{
		{"single file", schemafile.LoadPath},
		{"source list", func(path string, opts schemafile.Options) (*schemamodel.Database, error) {
			return schemafile.LoadAll([]string{path}, opts)
		}},
	} {
		t.Run(load.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(path, []byte("CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id));"), 0o600), qt.IsNil)
			database, err := load.read(path, schemafile.Options{Dialect: "ydb"})
			c.Assert(err, qt.IsNil)
			for _, kind := range []coverage.Kind{coverage.Replication} {
				c.Assert(database.NotDescribed.Describes(kind), qt.IsFalse, qt.Commentf("%s", kind))
			}
			for _, kind := range []coverage.Kind{coverage.StreamingQuery, coverage.Changefeed, coverage.CoordinationNode, coverage.ResourcePool, coverage.ResourcePoolClassifier, coverage.ColumnTable, coverage.View, coverage.Topic} {
				c.Assert(database.NotDescribed.Describes(kind), qt.IsTrue)
			}
		})
	}
}

func TestYQLPrincipalChangesAcrossFiles(t *testing.T) {
	c := qt.New(t)
	directory := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(directory, "01.sql"), []byte("CREATE USER app; CREATE GROUP readers;"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "02.sql"), []byte("ALTER USER app NOLOGIN; ALTER GROUP readers ADD USER app;"), 0o600), qt.IsNil)
	database, err := schemafile.LoadPath(directory, schemafile.Options{Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(database.Roles, qt.HasLen, 2)
	c.Assert(database.Roles[0].Name, qt.Equals, "app")
	c.Assert(database.Roles[0].Login, qt.IsFalse)
	c.Assert(database.Roles[0].MemberOf, qt.DeepEquals, []string{"readers"})
	c.Assert(database.NotDescribed.Describes(coverage.Role), qt.IsTrue)
}

func TestYQLPrivilegeChangesAcrossFiles(t *testing.T) {
	c := qt.New(t)
	directory := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(directory, "01.sql"), []byte("CREATE TABLE `shop/orders` (id Uint64 NOT NULL, PRIMARY KEY(id)); GRANT SELECT ON `/local/shop/orders` TO readers WITH GRANT OPTION;"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "02.sql"), []byte("REVOKE GRANT OPTION FOR SELECT ON `shop/orders` FROM readers; GRANT LIST ON shop TO readers;"), 0o600), qt.IsNil)
	database, err := schemafile.LoadPath(directory, schemafile.Options{Dialect: "ydb", DatabaseURL: "ydb://example/local?dev_realm=isolated"})
	c.Assert(err, qt.IsNil)
	c.Assert(database.Grants, qt.DeepEquals, []schemamodel.Grant{{Role: "readers", OnSchema: "shop", Privileges: []string{"YDB.GENERIC.LIST"}}})
	c.Assert(database.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.GENERIC.READ"}},
		{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.ACCESS.GRANT"}},
	})
	c.Assert(database.NotDescribed.Describes(coverage.Grant), qt.IsTrue)
	c.Assert(database.DatabasePath, qt.Equals, "")
}

func TestYQLPrivilegeDatabaseContext(t *testing.T) {
	for _, test := range []struct{ name, url, target string }{
		{"query database", "ydb://example?database=/Root/db", "/Root/db/shop/orders"},
		{"TLS", "ydbs://example/Root/db", "/Root/db/shop/orders"},
		{"Docker default", "docker://ydb", "/local/shop/orders"},
		{"Docker explicit", "docker://ydb/26.2.1.14/local", "/local/shop/orders"},
		{"realm remains context only", "ydb://example/local?dev_realm=isolated", "/local/shop/orders"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, err := readYQLPermissionFile(c, test.url, test.target)
			c.Assert(err, qt.IsNil)
			c.Assert(database.Grants, qt.DeepEquals, []schemamodel.Grant{{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.GENERIC.READ"}}})
			c.Assert(database.DatabasePath, qt.Equals, "")
		})
	}
}

func TestYQLPrivilegeInvalidDatabaseContext(t *testing.T) {
	for _, test := range []struct{ name, url, target string }{
		{"different database", "ydb://example/Root/db", "/Root/other/shop/orders"},
		{"conflicting database", "ydb://example/local?database=/other", "/local/shop/orders"},
		{"invalid context", "ydb://user:CONTEXT_SENTINEL@example:bad/local", "/local/shop/orders"},
		{"other Docker dialect", "docker://postgres/17/dev", "/dev/shop/orders"},
		{"Docker database override", "docker://ydb?database=/other", "/other/shop/orders"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, err := readYQLPermissionFile(c, test.url, test.target)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), "CONTEXT_SENTINEL")
			c.Assert(database, qt.IsNil)
		})
	}
}

func readYQLPermissionFile(c *qt.C, databaseURL, target string) (*schemamodel.Database, error) {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	source := "CREATE TABLE `shop/orders` (id Uint64 NOT NULL, PRIMARY KEY(id)); GRANT SELECT ON `" + target + "` TO readers;"
	c.Assert(os.WriteFile(file, []byte(source), 0o600), qt.IsNil)
	return schemafile.LoadPath(file, schemafile.Options{Dialect: "ydb", DatabaseURL: databaseURL})
}
