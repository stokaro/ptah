//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// sqlServerTableComment reads the comment the catalog holds for a table, and "" for
// none.
func sqlServerTableComment(c *qt.C, db *sql.DB, table string) string {
	c.Helper()
	var comment string
	c.Assert(db.QueryRowContext(c.Context(), `SELECT COALESCE(MAX(CAST(ep.value AS nvarchar(4000))), N'')
		FROM sys.extended_properties AS ep
		WHERE ep.class = 1 AND ep.major_id = OBJECT_ID(@p1) AND ep.minor_id = 0 AND ep.name = N'MS_Description'`, table,
	).Scan(&comment), qt.IsNil)
	return comment
}

// sqlServerColumnComment reads the comment the catalog holds for a column, and ""
// for none.
func sqlServerColumnComment(c *qt.C, db *sql.DB, table, column string) string {
	c.Helper()
	var comment string
	c.Assert(db.QueryRowContext(c.Context(), `SELECT COALESCE(MAX(CAST(ep.value AS nvarchar(4000))), N'')
		FROM sys.extended_properties AS ep
		WHERE ep.class = 1 AND ep.major_id = OBJECT_ID(@p1)
		  AND ep.minor_id = COLUMNPROPERTY(OBJECT_ID(@p1), @p2, 'ColumnId') AND ep.name = N'MS_Description'`, table, column,
	).Scan(&comment), qt.IsNil)
	return comment
}

// sqlServerCommentedTableStatements create a table in schema with a comment on it
// and on its column, the way an operator would.
func sqlServerCommentedTableStatements(schema string) []string {
	return []string{
		"CREATE TABLE [" + schema + "].[t] (id int NOT NULL)",
		"EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'it''s t', @level0type = N'SCHEMA', @level0name = N'" + schema +
			"', @level1type = N'TABLE', @level1name = N't'",
		"EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'the id', @level0type = N'SCHEMA', @level0name = N'" + schema +
			"', @level1type = N'TABLE', @level1name = N't', @level2type = N'COLUMN', @level2name = N'id'",
	}
}

// TestSQLServerTableCommentIsWrittenWithTheTableE2E creates a table with a comment on
// it and on a column, and reads both back from sys.extended_properties after that one
// apply. Written as a SQL comment, each would arrive only with the next plan, so the
// next plan is the other half of this: it is empty.
func TestSQLServerTableCommentIsWrittenWithTheTableE2E(t *testing.T) {
	const table = "  comment = \"it's t\"\n  column \"id\" {\n    type = int\n    null = false\n    comment = \"the id\"\n  }\n}\n"
	tests := []struct {
		name, document, table string
	}{
		{
			name: "the connected schema", table: "dbo.t",
			document: "schema \"dbo\" {\n}\n\ntable \"t\" {\n  schema = schema.dbo\n" + table,
		},
		{
			name: "another schema", table: "es_app.t",
			document: "schema \"dbo\" {\n}\n\nschema \"es_app\" {\n}\n\ntable \"t\" {\n  schema = schema.es_app\n" + table,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := declaredSQLServerDatabase(c)
			file := writeDeclaredSchemaFile(c, "schema.hcl", test.document)

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(sqlServerTableComment(c, db, test.table), qt.Equals, "it's t")
			c.Assert(sqlServerColumnComment(c, db, test.table, "id"), qt.Equals, "the id")

			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestSQLServerDBReadKeepsATableCommentE2E reads a commented table with `db read` and
// plans from the description against the same database. A description that writes the
// comments as SQL comments loses them, and the plan drops them from the table it
// describes. The description also builds an empty database, by each way one reaches a
// server, and `db read` of that database answers the same text.
func TestSQLServerDBReadKeepsATableCommentE2E(t *testing.T) {
	tests := []struct {
		name string
		// schema is where the table lives, and selection the read that reaches it.
		schema    string
		selection []string
		setup     []string
	}{
		{name: "the connected schema", schema: "dbo"},
		{
			name: "another schema", schema: "es_app", selection: []string{"--schemas", "es_app"},
			setup: []string{"CREATE SCHEMA es_app"},
		},
	}
	builds := []struct {
		name string
		args func(url, description, dir string) []string
	}{
		{name: "schema apply", args: func(url, description, _ string) []string {
			return []string{"schema", "apply", "--db-url", url, "--schema-file", description, "--auto-approve"}
		}},
		{name: "migrations up", args: func(url, _, dir string) []string {
			return []string{"migrations", "up", "--db-url", url, "--migrations-dir", dir}
		}},
	}
	for _, test := range tests {
		for _, build := range builds {
			t.Run(test.name+"/"+build.name, func(t *testing.T) {
				c := qt.New(t)
				source, sourceDB := declaredSQLServerDatabase(c)
				empty, emptyDB := declaredSQLServerDatabase(c)
				declaredSQLServerExec(c, sourceDB, test.setup...)
				declaredSQLServerExec(c, sourceDB, sqlServerCommentedTableStatements(test.schema)...)
				table := test.schema + ".t"

				read := runPtahIn(c, append([]string{"db", "read", "--db-url", source}, test.selection...)...)

				c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
				description := writeDeclaredSchemaFile(c, "described.sql", read.Stdout)
				planned := runPtahIn(c, append([]string{"schema", "apply", "--db-url", source, "--schema-file", description, "--dry-run"}, test.selection...)...)
				c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("description:\n%s\nstdout:\n%s\nstderr:\n%s", read.Stdout, planned.Stdout, planned.Stderr))
				c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")

				dir := c.TempDir()
				c.Assert(os.WriteFile(filepath.Join(dir, "1_replay.up.sql"), []byte(read.Stdout), 0o600), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "1_replay.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
				built := runPtahIn(c, build.args(empty, description, dir)...)
				c.Assert(built.ExitCode, qt.Equals, 0, qt.Commentf("description:\n%s\nstdout:\n%s\nstderr:\n%s", read.Stdout, built.Stdout, built.Stderr))
				c.Assert(sqlServerTableComment(c, emptyDB, table), qt.Equals, "it's t")
				c.Assert(sqlServerColumnComment(c, emptyDB, table, "id"), qt.Equals, "the id")

				again := runPtahIn(c, append([]string{"db", "read", "--db-url", empty}, test.selection...)...)

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", again.Stderr))
				c.Assert(again.Stdout, qt.Equals, read.Stdout)
			})
		}
	}
}

// TestSQLServerTableCommentIsRehearsedE2E rehearses a plan that creates a commented
// table, then one that changes the comment, on one dev database on both surfaces. The
// statements have to pass the replay guard, and the dev database holds no table and
// no comment afterward.
func TestSQLServerTableCommentIsRehearsedE2E(t *testing.T) {
	const table = "schema \"dbo\" {\n}\n\ntable \"t\" {\n  schema = schema.dbo\n  comment = %q\n" +
		"  column \"id\" {\n    type = int\n    null = false\n  }\n}\n"
	for _, surface := range devSchemaStateSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target, targetDB := declaredSQLServerDatabase(c)
			dev, devDB := declaredSQLServerDatabase(c)
			first := writeDeclaredSchemaFile(c, "schema.hcl", sqlServerTableDocument(table, "first"))

			created := clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir()}, surface.apply(target, first, dev)...)

			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
			c.Assert(sqlServerTableComment(c, targetDB, "dbo.t"), qt.Equals, "first")
			c.Assert(sqlServerUserTableCount(c, devDB), qt.Equals, 0)
			second := writeDeclaredSchemaFile(c, "schema.hcl", sqlServerTableDocument(table, "second"))

			changed := clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir()}, surface.apply(target, second, dev)...)

			c.Assert(changed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", changed.Stdout, changed.Stderr))
			c.Assert(changed.Stdout, qt.Contains, "sp_updateextendedproperty")
			c.Assert(sqlServerTableComment(c, targetDB, "dbo.t"), qt.Equals, "second")
			c.Assert(sqlServerUserTableCount(c, devDB), qt.Equals, 0)
		})
	}
}

// sqlServerTableDocument fills a table document's comment.
func sqlServerTableDocument(format, comment string) string {
	return fmt.Sprintf(format, comment)
}

// sqlServerUserTableCount counts the tables a database holds.
func sqlServerUserTableCount(c *qt.C, db *sql.DB) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(c.Context(), `SELECT count(*) FROM sys.tables WHERE is_ms_shipped = 0`).Scan(&count), qt.IsNil)
	return count
}
