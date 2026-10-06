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
