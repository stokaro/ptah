//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
)

// Validated with identifier rules that keep no two SQL Server names apart,
// `schema diff` refuses every table with two columns as a possible collision
// (stokaro/ptah#4122). Against a live server the server resolves the names, so
// a schema it holds compares as synced, and two spellings its collation folds
// together are still refused. Between two documents nothing resolves them, and
// two ASCII names that differ after case folding are kept apart, because no
// SQL Server collation makes them one name.

// sqlServerDiffSchema is two tables of several columns each, the shape the
// offline rules refuse.
var sqlServerDiffSchema = []string{
	"CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL, name VARCHAR(100))",
	"CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL)",
}

// writeSQLServerDiffSchema writes statements to a schema file and returns its
// path.
func writeSQLServerDiffSchema(c *qt.C, statements []string) string {
	c.Helper()
	var body string
	for _, statement := range statements {
		body += statement + ";\n"
	}
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// TestSchemaDiffSQLServerLive_HappyPath compares a live database with the
// schema it holds, on both surfaces.
func TestSchemaDiffSQLServerLive_HappyPath(t *testing.T) {
	t.Run("native", func(t *testing.T) {
		c := qt.New(t)
		target := sqlServerDevDatabase(c)
		target.exec(c, sqlServerDiffSchema)
		desired := writeSQLServerDiffSchema(c, sqlServerDiffSchema)

		out, err := runPtahNativeWithError("schema", "diff", "--from", target.url, "--to", desired)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Schemas are synced")
	})
	t.Run("compat", func(t *testing.T) {
		c := qt.New(t)
		target := sqlServerDevDatabase(c)
		dev := sqlServerDevDatabase(c)
		target.exec(c, sqlServerDiffSchema)
		desired := writeSQLServerDiffSchema(c, sqlServerDiffSchema)

		out, err := runCompatVerb("schema", "diff", "--from", target.url, "--to", "file://"+filepath.ToSlash(desired),
			"--dev-url", dev.url)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Schemas are synced")
	})
	t.Run("native, two documents", func(t *testing.T) {
		c := qt.New(t)
		dev := sqlServerDevDatabase(c)
		from := writeSQLServerDiffSchema(c, sqlServerDiffSchema)
		to := writeSQLServerDiffSchema(c, sqlServerDiffSchema)

		out, err := runPtahNativeWithError("schema", "diff", "--from", from, "--to", to, "--dev-url", dev.url)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Schemas are synced")
	})
	t.Run("compat, two documents", func(t *testing.T) {
		c := qt.New(t)
		dev := sqlServerDevDatabase(c)
		from := writeSQLServerDiffSchema(c, sqlServerDiffSchema)
		to := writeSQLServerDiffSchema(c, sqlServerDiffSchema)

		out, err := runCompatVerb("schema", "diff", "--from", "file://"+filepath.ToSlash(from),
			"--to", "file://"+filepath.ToSlash(to), "--dev-url", dev.url)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Schemas are synced")
	})
}

// TestSchemaDiffSQLServerLive_FailurePath declares two columns the server's
// default collation, which is case-insensitive, treats as one. The live
// validation still refuses them, so the change above moves the check to the
// server's answer rather than dropping it.
func TestSchemaDiffSQLServerLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := sqlServerDevDatabase(c)
	target.exec(c, sqlServerDiffSchema)
	desired := writeSQLServerDiffSchema(c, []string{
		"CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL, Email VARCHAR(255))",
		"CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL)",
	})

	out, err := runPtahNativeWithError("schema", "diff", "--from", target.url, "--to", desired)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `(?s).*target columns users\.email and users\.Email may have the same catalog identity.*`)
	c.Assert(out, qt.Not(qt.Contains), "Schemas are synced")
}
