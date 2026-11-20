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

// TestDeclaredCommentIsNotATransactionDirectiveE2E declares a table comment
// that spells `-- +ptah no_transaction`, generates a migration file from it,
// appends a statement that fails, and applies the file. The file runs in its
// default transaction, so the failure rolls the table back. Were the comment
// read as the directive, the file would run without one and the table would
// stay.
func TestDeclaredCommentIsNotATransactionDirectiveE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(qt.New(t), ctx, e2eRepoRoot(t), binary)
	c := qt.New(t)
	dbURL := renderOmissionDatabases["sqlite"](c)
	dir := c.TempDir()
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE d (id INT PRIMARY KEY) COMMENT='+ptah no_transaction';\n"), 0o600), qt.IsNil)
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)

	stdout, stderr, err := runCLIProcess(ctx, dir, binary, "migrations", "generate", "--db-url", dbURL,
		"--schema-file", schema, "--migrations-dir", migrations, "--name", "first")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	files, globErr := filepath.Glob(filepath.Join(migrations, "*_first.up.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	up, readErr := os.ReadFile(files[0])
	c.Assert(readErr, qt.IsNil)
	// #nosec G703 -- files[0] comes from a Glob of a c.TempDir() directory; no external input reaches the path
	c.Assert(os.WriteFile(files[0], append(up, []byte("\nINSERT INTO no_such_table VALUES (1);\n")...), 0o600), qt.IsNil)

	stdout, stderr, err = runCLIProcess(ctx, dir, binary, "migrations", "up", "--db-url", dbURL, "--migrations-dir", migrations)

	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(readOmissionBack(c, ctx, dbURL, "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'd'"), qt.Equals, "0")
}

// TestLiveObjectNameIsNotAFileDirectiveE2E plants a table whose name spells
// `atlas:txmode bogus`, which needs only the right to create a table, and skips
// its drop by diff policy, so the skip comment names it in the file's header.
// Read as a directive, the word makes the file unreadable (`unknown txmode`);
// the file applies as any other.
func TestLiveObjectNameIsNotAFileDirectiveE2E(t *testing.T) {
	tests := []struct {
		engine    string
		keepQuery string
	}{
		{engine: "sqlite", keepQuery: "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'keep'"},
		{engine: "postgres", keepQuery: "SELECT count(*) FROM information_schema.tables WHERE table_name = 'keep'"},
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
			_, err = conn.ExecContext(ctx, `CREATE TABLE "gone atlas:txmode bogus" (id int)`)
			c.Assert(err, qt.IsNil)
			dbschema.CloseAndWarn(conn)

			stdout, stderr, err := runCLIProcess(ctx, dir, binary, "migrations", "generate", "--db-url", dbURL,
				"--schema-file", schema, "--migrations-dir", migrations, "--name", "first")
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			stdout, stderr, err = runCLIProcess(ctx, dir, binary, "migrations", "up", "--db-url", dbURL, "--migrations-dir", migrations)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(readOmissionBack(c, ctx, dbURL, test.keepQuery), qt.Equals, "1")
		})
	}
}

// TestAtlasChangeHeadingIsNotAFileDirectiveE2E diffs a table whose name spells
// `atlas:txmode none` with ptah-compat. The file opens with the heading
// `-- Create "atlas:txmode none" table` directly above its statement, the shape
// the community binary writes and reads no directive from. Read as one, the
// heading makes the file unreadable (`unknown txmode`); the file applies and
// the table reads back.
func TestAtlasChangeHeadingIsNotAFileDirectiveE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "atlas")
	buildPtahCompat(qt.New(t), ctx, e2eRepoRoot(t), binary)
	c := qt.New(t)
	dbURL := renderOmissionDatabases["sqlite"](c)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(`CREATE TABLE "atlas:txmode none" (id int PRIMARY KEY);`+"\n"), 0o600), qt.IsNil)

	stdout, stderr, err := runCLIProcess(ctx, dir, binary, "migrate", "diff", "first", "--dir", "file://migrations",
		"--to", "file://schema.sql", "--dev-url", "sqlite://dev?mode=memory")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	files, globErr := filepath.Glob(filepath.Join(dir, "migrations", "*_first.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	written, readErr := os.ReadFile(files[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(written), qt.Contains, "-- Create \"atlas:txmode none\" table\nCREATE TABLE")
	stdout, stderr, err = runCLIProcess(ctx, dir, binary, "migrate", "apply", "--dir", "file://migrations", "--url", dbURL)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(readOmissionBack(c, ctx, dbURL, "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'atlas:txmode none'"), qt.Equals, "1")
}
