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
			for _, kind := range []coverage.Kind{coverage.Changefeed} {
				c.Assert(database.NotDescribed.Describes(kind), qt.IsFalse, qt.Commentf("%s", kind))
			}
			for _, kind := range []coverage.Kind{coverage.CoordinationNode, coverage.ResourcePool, coverage.ResourcePoolClassifier, coverage.ColumnTable, coverage.View, coverage.Topic} {
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
