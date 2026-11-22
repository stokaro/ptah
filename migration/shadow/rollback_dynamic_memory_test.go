package shadow_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

// memoryRollbackHistory creates a, b and c at versions 1, 2 and 3, in the
// Atlas format, which carries no down bodies.
var memoryRollbackHistory = fstest.MapFS{
	"1_a.sql": {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);")},
	"2_b.sql": {Data: []byte("CREATE TABLE b (id INTEGER PRIMARY KEY);")},
	"3_c.sql": {Data: []byte("CREATE TABLE c (id INTEGER PRIMARY KEY);")},
}

// TestPlanDynamicRollbackInMemory_HappyPath plans a rollback from version 3 to
// version 2, which drops c and nothing else, with a dev database that lives in
// its connection's one session. Planning replays the history on the dev
// database it opens and compares it with the target; read through a second
// connection, the dev database is a new, empty one and the plan drops a and b
// as well.
func TestPlanDynamicRollbackInMemory_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		devURL string
	}{
		{name: "a named database in memory", devURL: "sqlite://file?mode=memory"},
		{name: "the memory path", devURL: "sqlite://:memory:"},
		{name: "memory beside another parameter", devURL: "sqlite://dev?mode=memory&_fk=1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "target.db")))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(target) })
			_, err = target.ExecContext(c.Context(),
				"CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (id INTEGER PRIMARY KEY); "+
					"CREATE TABLE c (id INTEGER PRIMARY KEY);")
			c.Assert(err, qt.IsNil)

			statements, err := shadow.PlanDynamicRollback(c.Context(), shadow.DynamicRollbackOptions{
				Runtime:          builtintest.Runtime(),
				TargetConnection: target,
				DevDatabaseURL:   test.devURL,
				FS:               memoryRollbackHistory,
				TargetVersion:    2,
				ProviderOptions:  []migrator.FSProviderOption{migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas)},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{`DROP TABLE "c"`})
		})
	}
}
