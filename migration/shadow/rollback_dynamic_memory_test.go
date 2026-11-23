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

// TestPlanDynamicRollbackInMemoryTarget_HappyPath plans the same rollback for
// a target that lives in its connection's one session, as an embedder that
// holds an in-memory database has. The comparison reads the target on the
// connection it is given; opened again by its URL, the target is a new, empty
// database, and the plan creates a and b instead of dropping c.
//
// A dev database in memory is another database unless it is the target's
// shared-cache name: a private one, under the target's own URL too, is
// reached by its connection alone, and the planner reads each side on its
// own connection.
func TestPlanDynamicRollbackInMemoryTarget_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		targetURL string
		devURL    func(dir string) string
	}{
		{
			name: "a dev database file", targetURL: "sqlite://target?mode=memory",
			devURL: func(dir string) string { return atlasurl.SQLiteURLFromPath(filepath.Join(dir, "dev.db")) },
		},
		{
			name: "a private dev database in memory", targetURL: "sqlite://target?mode=memory",
			devURL: func(string) string { return "sqlite://dev?mode=memory" },
		},
		{
			name: "a private dev database under the target's URL", targetURL: "sqlite://target?mode=memory",
			devURL: func(string) string { return "sqlite://target?mode=memory" },
		},
		{
			name: "a shared-cache dev database of another name", targetURL: "sqlite://target?mode=memory&cache=shared",
			devURL: func(string) string { return "sqlite://dev?mode=memory&cache=shared" },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := memoryRollbackTarget(c, test.targetURL)

			statements, err := shadow.PlanDynamicRollback(c.Context(), shadow.DynamicRollbackOptions{
				Runtime:          builtintest.Runtime(),
				TargetConnection: target,
				DevDatabaseURL:   test.devURL(c.TempDir()),
				FS:               memoryRollbackHistory,
				TargetVersion:    2,
				ProviderOptions:  []migrator.FSProviderOption{migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas)},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{`DROP TABLE "c"`})
		})
	}
}

// TestPlanDynamicRollbackInMemoryTarget_FailurePath refuses a dev URL that
// opens the target's shared-cache database, before anything resets it: the
// target still holds its three tables.
func TestPlanDynamicRollbackInMemoryTarget_FailurePath(t *testing.T) {
	c := qt.New(t)
	const targetURL = "sqlite://target?mode=memory&cache=shared"
	target := memoryRollbackTarget(c, targetURL)

	statements, err := shadow.PlanDynamicRollback(c.Context(), shadow.DynamicRollbackOptions{
		Runtime:          builtintest.Runtime(),
		TargetConnection: target,
		DevDatabaseURL:   targetURL,
		FS:               memoryRollbackHistory,
		TargetVersion:    2,
		ProviderOptions:  []migrator.FSProviderOption{migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas)},
	})

	c.Assert(err, qt.ErrorMatches, `dynamic rollback planning failed: dev database must be distinct from target database`)
	c.Assert(statements, qt.IsNil)
	var tables int
	c.Assert(target.QueryRowContext(c.Context(), "SELECT count(*) FROM sqlite_master WHERE type = 'table'").Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 3)
}

// memoryRollbackTarget opens the in-memory database at url, closed when the
// test ends, holding a, b and c: memoryRollbackHistory applied up to version
// 3.
func memoryRollbackTarget(c *qt.C, url string) *dbschema.DatabaseConnection {
	c.Helper()
	target, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	_, err = target.ExecContext(c.Context(),
		"CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (id INTEGER PRIMARY KEY); "+
			"CREATE TABLE c (id INTEGER PRIMARY KEY);")
	c.Assert(err, qt.IsNil)
	return target
}
