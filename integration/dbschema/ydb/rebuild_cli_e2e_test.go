//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// rebuildE2EEntities is a table whose `n` column is declared with the type
// passed in, so two versions of it change the column's type.
func rebuildE2EEntities(nType string) string {
	return `package entities

//ptah:schema:table name="items" schema="ptah_ydb_rebuild_e2e"
type Item struct {
	//ptah:schema:field name="id" type="BIGINT" primary
	ID int64
	//ptah:schema:field name="n" type="` + nType + `"
	N int64
}
`
}

// TestYDBBinary_SchemaApplyRebuildsWhenAsked drives `schema apply` through a
// column type change YDB cannot make in place. Without --allow-table-rebuild
// the apply is refused and names the flag; with it the plan the binary prints
// warns that rows written during the rebuild are lost, the table is rebuilt
// with its rows, and `schema compare` finds nothing left to change.
func TestYDBBinary_SchemaApplyRebuildsWhenAsked(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropDirectory(c, conn, "ptah_ydb_rebuild_e2e", "items")
			c.Cleanup(func() { dropDirectory(c, conn, "ptah_ydb_rebuild_e2e", "items") })
			before, after := c.TempDir(), c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(before, "items.go"), []byte(rebuildE2EEntities("INTEGER")), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(after, "items.go"), []byte(rebuildE2EEntities("BIGINT")), 0o600), qt.IsNil)
			created, createErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", before,
				"--auto-approve")
			c.Assert(createErr, qt.IsNil, qt.Commentf("schema apply:\n%s", created))
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"UPSERT INTO `ptah_ydb_rebuild_e2e/items` (`id`, `n`) VALUES (1l, 10), (2l, NULL)"), qt.IsNil)

			refused, refusedErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", after,
				"--auto-approve")
			rebuilt, rebuildErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", after,
				"--auto-approve", "--allow-table-rebuild")
			compared, compareErr := runBinary(ctx, binary, "schema", "compare", "--db-url", url, "--root-dir", after,
				"--schemas", "ptah_ydb_rebuild_e2e", "--exit-code")

			c.Assert(refusedErr, qt.IsNotNil)
			c.Assert(refused, qt.Contains, "plans when asked with --allow-table-rebuild")
			c.Assert(rebuildErr, qt.IsNil, qt.Commentf("schema apply --allow-table-rebuild:\n%s", rebuilt))
			c.Assert(rebuilt, qt.Contains, "Rows written to ptah_ydb_rebuild_e2e/items between the copy and the swap are lost")
			c.Assert(compareErr, qt.IsNil, qt.Commentf("schema compare:\n%s", compared))
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `ptah_ydb_rebuild_e2e/items` WHERE `id` = 1l AND `n` = 10l"),
				qt.Equals, int64(1))
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `ptah_ydb_rebuild_e2e/items` WHERE `id` = 2l AND `n` IS NULL"),
				qt.Equals, int64(1))
		})
	}
}

// rebuildCommandsTable is the root-level table the planning-command test
// changes; a YAML schema file names no directory.
const rebuildCommandsTable = "ptah_ydb_rebuild_cmds"

// dropRebuildCommandsTables drops the root-level table the planning-command
// tests change, and the scratch tables a rebuild of it could leave.
func dropRebuildCommandsTables(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	for _, table := range []string{rebuildCommandsTable, "__ptah_rebuild_" + rebuildCommandsTable,
		"__ptah_replaced_" + rebuildCommandsTable} {
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TABLE IF EXISTS `"+table+"`"), qt.IsNil)
	}
}

// rebuildCommandsSources writes the table twice -- with `n` as INTEGER, and
// as BIGINT in Go annotations and in a YAML schema file -- and returns the
// directories and the file.
func rebuildCommandsSources(c *qt.C) (before, after, afterYAML string) {
	c.Helper()
	entities := func(nType string) string {
		return "package entities\n\n//ptah:schema:table name=\"" + rebuildCommandsTable + "\"\ntype Item struct {\n" +
			"\t//ptah:schema:field name=\"id\" type=\"BIGINT\" primary\n\tID int64\n" +
			"\t//ptah:schema:field name=\"n\" type=\"" + nType + "\"\n\tN int64\n}\n"
	}
	before, after = c.TempDir(), c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(before, "items.go"), []byte(entities("INTEGER")), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(after, "items.go"), []byte(entities("BIGINT")), 0o600), qt.IsNil)
	afterYAML = filepath.Join(c.TempDir(), "after.yaml")
	c.Assert(os.WriteFile(afterYAML, []byte("tables:\n  "+rebuildCommandsTable+":\n    columns:\n      id:\n"+
		"        type: BIGINT\n        primary: true\n        not_null: true\n      n:\n        type: BIGINT\n"), 0o600), qt.IsNil)
	return before, after, afterYAML
}

// TestYDBBinary_EveryPlanningCommandTakesTheFlag runs each native command
// that plans schema changes over a column type change YDB cannot make in
// place: with --allow-table-rebuild it plans the rebuild, notes included, and
// without it it is refused with a message naming the flag.
func TestYDBBinary_EveryPlanningCommandTakesTheFlag(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		url := dbtarget.URL(t, line.engine)
		before, after, afterYAML := rebuildCommandsSources(c)
		commands := []struct {
			name string
			args []string
		}{
			{name: "schema compare", args: []string{"schema", "compare", "--db-url", url, "--root-dir", after}},
			{name: "schema diff", args: []string{"schema", "diff", "--from", url, "--to", afterYAML,
				"--include", rebuildCommandsTable}},
			{name: "schema plan", args: []string{"schema", "plan", "--db-url", url, "--root-dir", after, "--name", "rebuild",
				"--output", filepath.Join(c.TempDir(), "rebuild.plan.hcl")}},
			{name: "migrations plan", args: []string{"migrations", "plan", "--db-url", url, "--root-dir", after}},
		}
		for _, command := range commands {
			t.Run(line.name+"/"+command.name, func(t *testing.T) {
				c := qt.New(t)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				conn := openYDB(c, line)
				dropRebuildCommandsTables(c, conn)
				c.Cleanup(func() { dropRebuildCommandsTables(c, conn) })
				created, createErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", before,
					"--auto-approve")
				c.Assert(createErr, qt.IsNil, qt.Commentf("schema apply:\n%s", created))

				planned, planErr := runBinary(ctx, binary, append(command.args, "--allow-table-rebuild")...)
				refused, refusedErr := runBinary(ctx, binary, command.args...)

				c.Assert(planErr, qt.IsNil, qt.Commentf("%s --allow-table-rebuild:\n%s", command.name, planned))
				c.Assert(planned, qt.Contains, "-- Rebuild of table "+rebuildCommandsTable+":")
				c.Assert(planned, qt.Contains, "ALTER TABLE `__ptah_rebuild_"+rebuildCommandsTable+"` RENAME TO `"+
					rebuildCommandsTable+"`")
				c.Assert(refusedErr, qt.IsNotNil)
				c.Assert(refused, qt.Contains, "plans when asked with --allow-table-rebuild")
			})
		}
	}
}

// `migrations generate` writes the rebuild into the migration file when
// asked, and is refused with the flag's name when not. `migrations lint`
// reads the file it wrote as a rebuild: the copy keeps the rows under the old
// name, so the final DROP TABLE is not reported as a lost table and the
// renames are not reported as a retired name.
func TestYDBBinary_MigrationsGenerateTakesTheFlag(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropRebuildCommandsTables(c, conn)
			c.Cleanup(func() { dropRebuildCommandsTables(c, conn) })
			before, after, _ := rebuildCommandsSources(c)
			created, createErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", before,
				"--auto-approve")
			c.Assert(createErr, qt.IsNil, qt.Commentf("schema apply:\n%s", created))
			refusedDir, generatedDir := c.TempDir(), c.TempDir()

			refused, refusedErr := runBinary(ctx, binary, "migrations", "generate", "--db-url", url, "--root-dir", after,
				"--migrations-dir", refusedDir, "--name", "rebuild")
			generated, generateErr := runBinary(ctx, binary, "migrations", "generate", "--db-url", url, "--root-dir", after,
				"--migrations-dir", generatedDir, "--name", "rebuild", "--allow-table-rebuild")

			c.Assert(refusedErr, qt.IsNotNil)
			c.Assert(refused, qt.Contains, "plans when asked with --allow-table-rebuild")
			c.Assert(generateErr, qt.IsNil, qt.Commentf("migrations generate --allow-table-rebuild:\n%s", generated))
			up, err := filepath.Glob(filepath.Join(generatedDir, "*.up.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(up, qt.HasLen, 1)
			body, err := os.ReadFile(up[0])
			c.Assert(err, qt.IsNil)
			c.Assert(string(body), qt.Contains, "Rows written to "+rebuildCommandsTable+" between the copy and the swap are lost")
			linted, lintErr := runBinary(ctx, binary, "migrations", "lint", "--dir", generatedDir, "--dialect", "ydb")
			c.Assert(lintErr, qt.IsNil, qt.Commentf("migrations lint:\n%s", linted))
			c.Assert(linted, qt.Not(qt.Contains), "DS101")
			c.Assert(linted, qt.Not(qt.Contains), "BC101")
			c.Assert(linted, qt.Not(qt.Contains), "BC103")
		})
	}
}
