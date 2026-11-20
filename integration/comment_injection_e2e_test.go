//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// TestMigrationFileKeepsDeclaredTextInsideItsComments generates a migration
// file from a schema whose table comment holds a line break and a DROP TABLE,
// applies the file, and reads the database back. SQLite keeps no table
// comment, so the text reaches a `--` comment of the file; the table the
// comment names survives. PostgreSQL has no row here: it writes the comment as
// a string literal, which is not a comment at all, and the next test covers
// the PostgreSQL route into a `--` comment.
func TestMigrationFileKeepsDeclaredTextInsideItsComments(t *testing.T) {
	const declared = "first\nDROP TABLE victim;\n-- end"
	tests := []struct {
		engine      string
		victimQuery string
		readBack    string
		want        string
	}{
		{engine: "sqlite", victimQuery: "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'victim'",
			readBack: "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'ro_c'", want: "1"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(qt.New(t), ctx, e2eRepoRoot(t), binary)
	for _, test := range tests {
		t.Run(test.engine, func(t *testing.T) {
			c := qt.New(t)
			dbURL := renderOmissionDatabases[test.engine](c)
			dir := c.TempDir()
			victim := "CREATE TABLE victim (id INT PRIMARY KEY);\n"
			victimSchema := filepath.Join(dir, "victim.sql")
			c.Assert(os.WriteFile(victimSchema, []byte(victim), 0o600), qt.IsNil)
			fullSchema := filepath.Join(dir, "schema.sql")
			c.Assert(os.WriteFile(fullSchema, []byte(victim+
				"CREATE TABLE ro_c (id INT PRIMARY KEY) COMMENT='"+declared+"';\n"), 0o600), qt.IsNil)
			migrations := filepath.Join(dir, "migrations")
			c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)

			stdout, stderr, err := runCLIProcess(ctx, dir, binary, "schema", "apply", "--db-url", dbURL, "--schema-file", victimSchema, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			stdout, stderr, err = runCLIProcess(ctx, dir, binary, "migrations", "generate", "--db-url", dbURL,
				"--schema-file", fullSchema, "--migrations-dir", migrations, "--name", "first")
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			stdout, stderr, err = runCLIProcess(ctx, dir, binary, "migrations", "up", "--db-url", dbURL, "--migrations-dir", migrations)
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))

			c.Assert(readOmissionBack(c, ctx, dbURL, test.victimQuery), qt.Equals, "1")
			c.Assert(readOmissionBack(c, ctx, dbURL, test.readBack), qt.Equals, test.want)
		})
	}
}

// TestMigrationFileKeepsALiveObjectNameInsideItsComment plants a table whose
// name holds a line break and a CREATE TABLE statement, which needs nothing
// but the right to create a table, and skips the drop of it by diff policy. The
// policy writes a `-- SKIP:` comment that names the table. The file is applied,
// and the statement the name carries has not run.
func TestMigrationFileKeepsALiveObjectNameInsideItsComment(t *testing.T) {
	tests := []struct {
		engine        string
		injectedQuery string
		keepQuery     string
	}{
		{engine: "sqlite", injectedQuery: "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'injected'",
			keepQuery: "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'keep'"},
		{engine: "postgres", injectedQuery: "SELECT count(*) FROM information_schema.tables WHERE table_name = 'injected'",
			keepQuery: "SELECT count(*) FROM information_schema.tables WHERE table_name = 'keep'"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(qt.New(t), ctx, e2eRepoRoot(t), binary)
	for _, test := range tests {
		t.Run(test.engine, func(t *testing.T) {
			c := qt.New(t)
			dbURL := renderOmissionDatabases[test.engine](c)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte("diff:\n  skip: [drop_table]\n"), 0o600), qt.IsNil)
			schema := filepath.Join(dir, "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE keep (id INT PRIMARY KEY);\n"), 0o600), qt.IsNil)
			migrations := filepath.Join(dir, "migrations")
			c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)
			conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
			c.Assert(err, qt.IsNil)
			_, err = conn.ExecContext(ctx, "CREATE TABLE \"gone\nCREATE TABLE injected(id int);\n--z\" (id int)")
			c.Assert(err, qt.IsNil)
			dbschema.CloseAndWarn(conn)

			stdout, stderr, err := runCLIProcess(ctx, dir, binary, "migrations", "generate", "--db-url", dbURL,
				"--schema-file", schema, "--migrations-dir", migrations, "--name", "first")
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			stdout, stderr, err = runCLIProcess(ctx, dir, binary, "migrations", "up", "--db-url", dbURL, "--migrations-dir", migrations)
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))

			c.Assert(readOmissionBack(c, ctx, dbURL, test.keepQuery), qt.Equals, "1")
			c.Assert(readOmissionBack(c, ctx, dbURL, test.injectedQuery), qt.Equals, "0")
		})
	}
}
