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
	"ptah.run/internal/exeext"
)

// The project the rows below apply: a table t(id, a, b) with an index t_a on a,
// and a desired schema that keeps t(id, a) and adds c. A skip policy that
// covers column and index drops leaves b and t_a where they are and adds c;
// without one, b and t_a go. Measured on the pinned community binary v1.3.0
// on SQLite, `schema apply` with `diff { skip { drop_index = true
// drop_column = true } }` plans `ALTER TABLE t ADD COLUMN c` and nothing else.
const (
	diffSkipCurrent = "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT);\nCREATE INDEX t_a ON t (a);\n"
	diffSkipDesired = "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, c TEXT);\n"

	diffSkipNativePolicy = "diff:\n  skip: [drop_index, drop_column, drop_enum]\n"
	diffSkipCompatPolicy = "  diff {\n    skip {\n      drop_index = true\n      drop_column = true\n    }\n  }\n"

	skippedColumnB = "SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)"
	skippedIndexTA = "SKIP: DROP INDEX of t.t_a omitted by diff policy (skip: drop_index)"
)

// diffSkipBinaries are the two shipped programs, built once for the rows of a
// test.
type diffSkipBinaries struct{ native, compat string }

func buildDiffSkipBinaries(c *qt.C, ctx context.Context, repoRoot string) diffSkipBinaries {
	c.Helper()
	binaries := diffSkipBinaries{
		native: filepath.Join(c.TempDir(), "ptah"+exeext.Suffix),
		compat: filepath.Join(c.TempDir(), "ptah-compat"+exeext.Suffix),
	}
	buildPtah(c, ctx, repoRoot, binaries.native)
	buildPtahCompat(c, ctx, repoRoot, binaries.compat)
	return binaries
}

// diffSkipProjectDir is a working directory holding the desired schema and,
// under name, the project file carrying config.
func diffSkipProjectDir(c *qt.C, name, config string) string {
	c.Helper()
	return diffSkipProjectDirAt(c, c.TempDir(), name, config)
}

// diffSkipProjectDirAt writes the project of [diffSkipProjectDir] into dir.
func diffSkipProjectDirAt(c *qt.C, dir, name, config string) string {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(diffSkipDesired), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(config), 0o600), qt.IsNil)
	return dir
}

// diffSkipSQLite is a SQLite database in dir holding the current state.
func diffSkipSQLite(c *qt.C, dir string) string {
	c.Helper()
	url := "sqlite://" + filepath.ToSlash(filepath.Join(dir, "target.db"))
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), diffSkipCurrent)
	c.Assert(err, qt.IsNil)
	return url
}

// countIn answers a counting query against the database at url.
func countIn(c *qt.C, url, query string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&count), qt.IsNil)
	return count
}

// sqliteSkippedPlan is what native ptah prints for the project under the
// policy: the comment for each change left out, and the one statement left.
const sqliteSkippedPlan = "Planned schema changes:\n" +
	"-- " + skippedColumnB + "\n" +
	"-- " + skippedIndexTA + "\n" +
	"ALTER TABLE \"t\" ADD COLUMN \"c\" TEXT;\n" +
	"Auto-approval enabled; applying schema changes.\n" +
	"Schema apply completed successfully.\n"

// sqliteCompatSkippedPlan is what ptah-compat prints for the same plan: the
// community binary's comment naming the change, directly above its statement,
// and the column as a read of the catalog prints it.
const sqliteCompatSkippedPlan = "Planned schema changes:\n" +
	"-- " + skippedColumnB + "\n" +
	"-- " + skippedIndexTA + "\n" +
	"-- Add column \"c\" to table: \"t\"\n" +
	"ALTER TABLE \"t\" ADD COLUMN \"c\" text NULL;\n" +
	"Auto-approval enabled; applying schema changes.\n" +
	"Schema apply completed successfully.\n"

// The questions the rows read back from SQLite: whether t has column b, has
// column c, and whether index t_a exists.
const (
	sqliteColumnB = "SELECT count(*) FROM pragma_table_info('t') WHERE name = 'b'"
	sqliteColumnC = "SELECT count(*) FROM pragma_table_info('t') WHERE name = 'c'"
	sqliteIndexTA = "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = 't_a'"
)

// TestSchemaApplyHonorsDiffSkipOnSQLiteE2E applies the project through each
// shipped program on SQLite, with the skip policy and without it, and reads
// the database back. Dropping b is a table rebuild there, so the unskipped rows
// lose t_a as well.
func TestSchemaApplyHonorsDiffSkipOnSQLiteE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	t.Cleanup(cancel)
	binaries := buildDiffSkipBinaries(c, ctx, e2eRepoRoot(t))

	t.Run("native with the policy", func(t *testing.T) {
		c := qt.New(t)
		dir := diffSkipProjectDir(c, "ptah.yaml", diffSkipNativePolicy)
		url := diffSkipSQLite(c, dir)

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, "schema", "apply",
			"--db-url", url, "--schema-file", "schema.sql", "--config", "ptah.yaml", "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout, qt.Equals, sqliteSkippedPlan)
		c.Assert(countIn(c, url, sqliteColumnB), qt.Equals, 1)
		c.Assert(countIn(c, url, sqliteColumnC), qt.Equals, 1)
		c.Assert(countIn(c, url, sqliteIndexTA), qt.Equals, 1)
	})

	t.Run("compat with the policy", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		url := diffSkipSQLite(c, dir)
		diffSkipProjectDirAt(c, dir, "atlas.hcl", `env "local" {
  url = "`+url+`"
  dev = "sqlite://file?mode=memory"
  src = "file://schema.sql"
`+diffSkipCompatPolicy+`}
`)

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.compat, "schema", "apply", "--env", "local", "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout, qt.Equals, sqliteCompatSkippedPlan)
		c.Assert(countIn(c, url, sqliteColumnB), qt.Equals, 1)
		c.Assert(countIn(c, url, sqliteColumnC), qt.Equals, 1)
		c.Assert(countIn(c, url, sqliteIndexTA), qt.Equals, 1)
	})

	// Retyping a rebuilds t, and the rebuild creates t again from the
	// declaration. The community binary v1.3.0 rebuilds it without b under
	// `drop_column = true`, measured on SQLite; here b and its rows stay.
	t.Run("native keeps a skipped column through a rebuild", func(t *testing.T) {
		c := qt.New(t)
		dir := diffSkipProjectDir(c, "ptah.yaml", "diff:\n  skip: [drop_column]\n")
		c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER);\n"), 0o600), qt.IsNil)
		url := diffSkipSQLite(c, dir)
		conn, err := dbschema.ConnectToDatabase(c.Context(), url)
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(c.Context(), "INSERT INTO t (id, a, b) VALUES (1, '7', 'kept')")
		dbschema.CloseAndWarn(conn)
		c.Assert(err, qt.IsNil)

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, "schema", "apply",
			"--db-url", url, "--schema-file", "schema.sql", "--config", "ptah.yaml", "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout, qt.Contains, "-- "+skippedColumnB+"\n")
		c.Assert(stdout, qt.Contains, `INSERT INTO "new_t" ("id", "a", "b") SELECT "id", "a", "b" FROM "t";`)
		c.Assert(countIn(c, url, "SELECT count(*) FROM t WHERE a = 7 AND b = 'kept'"), qt.Equals, 1)
	})

	// Dropping b rebuilds t, and DROP TABLE takes its indexes along. The
	// community binary v1.3.0 loses t_a and says nothing, measured on SQLite;
	// here the plan keeps what it says it keeps.
	for _, row := range []struct {
		name, desired string
	}{
		{"a dropped column", "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT);\n"},
		{"an added constraint", "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, CONSTRAINT a_set CHECK (a <> ''));\n"},
	} {
		t.Run("native keeps a skipped index through a rebuild for "+row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := diffSkipProjectDir(c, "ptah.yaml", "diff:\n  skip: [drop_index]\n")
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(row.desired), 0o600), qt.IsNil)
			url := diffSkipSQLite(c, dir)
			args := []string{"schema", "apply", "--db-url", url, "--schema-file", "schema.sql", "--config", "ptah.yaml", "--auto-approve"}

			stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, args...)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "-- "+skippedIndexTA+"\n")
			c.Assert(stdout, qt.Contains, `ALTER TABLE "new_t" RENAME TO "t";`)
			c.Assert(stdout, qt.Contains, "CREATE INDEX t_a ON t (a);\n")
			c.Assert(countIn(c, url, sqliteIndexTA), qt.Equals, 1)

			stdout, stderr, err = runCLIProcess(ctx, dir, binaries.native, args...)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Not(qt.Contains), "CREATE")
			c.Assert(countIn(c, url, sqliteIndexTA), qt.Equals, 1)
		})
	}

	t.Run("compat keeps a skipped index through a rebuild", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		url := diffSkipSQLite(c, dir)
		diffSkipProjectDirAt(c, dir, "atlas.hcl", `env "local" {
  url = "`+url+`"
  dev = "sqlite://file?mode=memory"
  src = "file://schema.sql"
  diff {
    skip {
      drop_index = true
    }
  }
}
`)
		c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT);\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.compat, "schema", "apply", "--env", "local", "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout, qt.Contains, "-- "+skippedIndexTA+"\n")
		c.Assert(countIn(c, url, sqliteColumnB), qt.Equals, 0)
		c.Assert(countIn(c, url, sqliteIndexTA), qt.Equals, 1)
	})

	// The rebuilt table has no a, so t_a cannot be kept. Nothing runs.
	t.Run("native refuses to keep an index over a column the rebuild drops", func(t *testing.T) {
		c := qt.New(t)
		dir := diffSkipProjectDir(c, "ptah.yaml", "diff:\n  skip: [drop_index]\n")
		c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte("CREATE TABLE t (id INTEGER PRIMARY KEY, b TEXT);\n"), 0o600), qt.IsNil)
		url := diffSkipSQLite(c, dir)

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, "schema", "apply",
			"--db-url", url, "--schema-file", "schema.sql", "--config", "ptah.yaml", "--auto-approve")

		c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s", stdout))
		c.Assert(stderr, qt.Contains, "rebuilding table t cannot keep index t_a: the rebuilt table has no column a")
		c.Assert(countIn(c, url, "SELECT count(*) FROM pragma_table_info('t') WHERE name = 'a'"), qt.Equals, 1)
		c.Assert(countIn(c, url, sqliteIndexTA), qt.Equals, 1)
	})

	t.Run("native without the policy", func(t *testing.T) {
		c := qt.New(t)
		dir := diffSkipProjectDir(c, "ptah.yaml", "diff:\n  skip: []\n")
		url := diffSkipSQLite(c, dir)

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, "schema", "apply",
			"--db-url", url, "--schema-file", "schema.sql", "--config", "ptah.yaml", "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout+stderr, qt.Not(qt.Contains), "SKIP:")
		c.Assert(countIn(c, url, sqliteColumnB), qt.Equals, 0)
		c.Assert(countIn(c, url, sqliteColumnC), qt.Equals, 1)
		c.Assert(countIn(c, url, sqliteIndexTA), qt.Equals, 0)
	})
}

// The PostgreSQL state adds an enum nothing declares, which the native policy
// keeps as well. The community binary has no enum skip, so the compatibility
// row leaves it out.
const (
	postgresDiffSkipCurrent = "CREATE TABLE t (id integer PRIMARY KEY, a text, b text);\nCREATE INDEX t_a ON t (a);\n" +
		"CREATE TYPE legacy_status AS ENUM ('x');\n"

	postgresColumnB      = "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 't' AND column_name = 'b'"
	postgresColumnC      = "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 't' AND column_name = 'c'"
	postgresIndexTA      = "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = 't_a'"
	postgresLegacyStatus = "SELECT count(*) FROM pg_type WHERE typname = 'legacy_status'"
)

// TestSchemaApplyHonorsDiffSkipOnPostgreSQLE2E is the same project on
// PostgreSQL, where each drop is a statement of its own: DROP COLUMN, DROP
// INDEX and DROP TYPE.
func TestSchemaApplyHonorsDiffSkipOnPostgreSQLE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	t.Cleanup(cancel)
	binaries := buildDiffSkipBinaries(c, ctx, e2eRepoRoot(t))

	t.Run("native with the policy", func(t *testing.T) {
		c := qt.New(t)
		dir := diffSkipProjectDir(c, "ptah.yaml", diffSkipNativePolicy)
		url := databaseBuiltFrom(c, postgresDiffSkipCurrent)

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, "schema", "apply",
			"--db-url", url, "--schema-file", "schema.sql", "--config", "ptah.yaml", "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout, qt.Equals, "Planned schema changes:\n"+
			"-- "+skippedColumnB+"\n"+
			"-- "+skippedIndexTA+"\n"+
			"-- SKIP: DROP TYPE of legacy_status omitted by diff policy (skip: drop_enum)\n"+
			"ALTER TABLE \"t\" ADD COLUMN \"c\" TEXT;\n"+
			"Auto-approval enabled; applying schema changes.\n"+
			"Schema apply completed successfully.\n")
		c.Assert(countIn(c, url, postgresColumnB), qt.Equals, 1)
		c.Assert(countIn(c, url, postgresColumnC), qt.Equals, 1)
		c.Assert(countIn(c, url, postgresIndexTA), qt.Equals, 1)
		c.Assert(countIn(c, url, postgresLegacyStatus), qt.Equals, 1)
	})

	t.Run("compat with the policy", func(t *testing.T) {
		c := qt.New(t)
		url := databaseBuiltFrom(c, postgresDiffSkipCurrent)
		dev, _ := scratchReplayDatabase(c)
		dir := diffSkipProjectDir(c, "atlas.hcl", `env "local" {
  url = "`+url+`"
  dev = "`+dev+`"
  src = "file://schema.sql"
`+diffSkipCompatPolicy+`}
`)

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.compat, "schema", "apply", "--env", "local", "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout, qt.Contains, "-- "+skippedColumnB+"\n")
		c.Assert(stdout, qt.Contains, "-- "+skippedIndexTA+"\n")
		c.Assert(countIn(c, url, postgresColumnB), qt.Equals, 1)
		c.Assert(countIn(c, url, postgresColumnC), qt.Equals, 1)
		c.Assert(countIn(c, url, postgresIndexTA), qt.Equals, 1)
	})

	t.Run("native without the policy", func(t *testing.T) {
		c := qt.New(t)
		dir := diffSkipProjectDir(c, "ptah.yaml", "diff:\n  skip: []\n")
		url := databaseBuiltFrom(c, postgresDiffSkipCurrent)

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, "schema", "apply",
			"--db-url", url, "--schema-file", "schema.sql", "--config", "ptah.yaml", "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout+stderr, qt.Not(qt.Contains), "SKIP:")
		c.Assert(countIn(c, url, postgresColumnB), qt.Equals, 0)
		c.Assert(countIn(c, url, postgresColumnC), qt.Equals, 1)
		c.Assert(countIn(c, url, postgresIndexTA), qt.Equals, 0)
		c.Assert(countIn(c, url, postgresLegacyStatus), qt.Equals, 0)
	})
}
