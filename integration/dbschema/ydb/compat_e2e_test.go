//go:build integration

package ydb_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/exeext"
)

// compatDir is the directory the compatibility binary's tables live in, beside
// compatRootTable at the database root.
const (
	compatDir       = "ptah_ydb_compat"
	compatRootTable = "ptah_ydb_compat_root"
)

// compatDesired is a desired schema in HCL with a table at the database root
// and one in compatDir, the shape whose read scope a document's schema block
// must not narrow.
const compatDesired = `table "` + compatRootTable + `" {
  column "id" {
    type = Int64
  }
  column "note" {
    type = Utf8
    null = true
  }
  primary_key {
    columns = [column.id]
  }
}

schema "` + compatDir + `" {
}

table "orders" {
  schema = schema.` + compatDir + `
  column "id" {
    type = Uint64
  }
  column "total" {
    type = Decimal(22,9)
  }
  primary_key {
    columns = [column.id]
  }
  index "orders_total_ix" {
    type = "GLOBAL SYNC"
    columns = [column.total]
  }
}
`

// compatMigrations are two migrations of a table in compatDir; the second
// carries the down section migrate down runs.
var compatMigrations = map[string]string{
	"20260101000000_items.sql": "CREATE TABLE `" + compatDir + "/items` (`id` Int64 NOT NULL, PRIMARY KEY (`id`));\n",
	"20260102000000_name.sql": "-- atlas:txtar\n\n-- migration.sql --\n" +
		"ALTER TABLE `" + compatDir + "/items` ADD COLUMN `name` Utf8;\n\n" +
		"-- down.sql --\n" +
		"ALTER TABLE `" + compatDir + "/items` DROP COLUMN `name`;\n",
}

// compatScripts updates the root table's row, once without and once with a
// row-count assertion.
const compatScripts = `script "exec" "note" {
  exec "set" {
    sql = "UPDATE ` + "`" + compatRootTable + "`" + ` SET note = 'set'u WHERE id = 1"
  }
}

script "exec" "checked" {
  exec "set" {
    sql         = "UPDATE ` + "`" + compatRootTable + "`" + ` SET note = 'checked'u WHERE id = 1"
    expect_rows = 1
  }
}
`

// TestYDBCompatBinary_RunsTheAtlasVerbs drives ptah-compat against a live YDB
// database on each certified line, in the default profile, where YDB is a
// Ptah extension. schema apply builds a document with a table at the root and
// one in a directory and then finds it synced; schema inspect describes both,
// as HCL that applies back as synced and as YQL; schema diff finds nothing to
// change; migrate apply records two migrations in the Atlas revision table in
// the directory, and migrate down rolls one back; script exec writes a row and
// refuses expect_rows, because the driver's row count is not a measurement.
//
// schema apply plans the whole database, as the Atlas verb does, so it drops a
// table the document does not declare. The package's tests run one at a time
// and each leaves the database as it found it, which is what makes that safe
// here, as it makes drop-all safe in TestYDBBinary_AppliesReadsAndDropsASchema.
func TestYDBCompatBinary_RunsTheAtlasVerbs(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropCompatTables(c, conn)
			c.Cleanup(func() { dropCompatTables(c, conn) })
			work := c.TempDir()
			desired := writeCompatFile(c, work, "desired.hcl", compatDesired)
			scripts := writeCompatFile(c, work, "scripts.hcl", compatScripts)
			migrations := filepath.Join(work, "migrations")
			c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
			for name, body := range compatMigrations {
				writeCompatFile(c, migrations, name, body)
			}
			dir := "file://" + filepath.ToSlash(migrations)

			applied, _, applyErr := runCompat(ctx, binary, "schema", "apply", "--url", url,
				"--to", "file://"+desired, "--auto-approve")
			c.Assert(applyErr, qt.IsNil, qt.Commentf("schema apply:\n%s", applied))
			synced, _, syncedErr := runCompat(ctx, binary, "schema", "apply", "--url", url,
				"--to", "file://"+desired, "--dry-run")
			c.Assert(syncedErr, qt.IsNil, qt.Commentf("schema apply again:\n%s", synced))
			c.Assert(synced, qt.Equals, "Schema is synced, no changes to be made\n")

			inspected, _, inspectErr := runCompat(ctx, binary, "schema", "inspect", "--url", url)
			c.Assert(inspectErr, qt.IsNil, qt.Commentf("schema inspect:\n%s", inspected))
			c.Assert(inspected, qt.Contains, "table \""+compatRootTable+"\" {\n  column \"id\" {\n    type = Int64\n  }")
			c.Assert(inspected, qt.Contains, "table \"orders\" {\n  schema = schema."+compatDir+"\n")
			c.Assert(inspected, qt.Contains, "    type = Decimal(22,9)\n")
			reapplied, _, reapplyErr := runCompat(ctx, binary, "schema", "apply", "--url", url,
				"--to", "file://"+writeCompatFile(c, work, "inspected.hcl", inspected), "--dry-run")
			c.Assert(reapplyErr, qt.IsNil, qt.Commentf("schema apply of the inspected document:\n%s", reapplied))
			c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made\n")
			yql, _, yqlErr := runCompat(ctx, binary, "schema", "inspect", "--url", url, "--format", "{{ sql . }}")
			c.Assert(yqlErr, qt.IsNil, qt.Commentf("schema inspect --format sql:\n%s", yql))
			c.Assert(yql, qt.Contains, "CREATE TABLE `"+compatDir+"/orders` (\n    `id` Uint64 NOT NULL,\n")
			diffed, _, diffErr := runCompat(ctx, binary, "schema", "diff", "--from", url,
				"--to", "file://"+desired, "--dev-url", url)
			c.Assert(diffErr, qt.IsNil, qt.Commentf("schema diff:\n%s", diffed))
			c.Assert(diffed, qt.Equals, "Schemas are synced, no changes to be made.\n")

			hashed, _, hashErr := runCompat(ctx, binary, "migrate", "hash", "--dir", dir)
			c.Assert(hashErr, qt.IsNil, qt.Commentf("migrate hash:\n%s", hashed))
			migrated, _, migrateErr := runCompat(ctx, binary, "migrate", "apply", "--url", url, "--dir", dir,
				"--revisions-schema", compatDir)
			c.Assert(migrateErr, qt.IsNil, qt.Commentf("migrate apply:\n%s", migrated))
			status, _, statusErr := runCompat(ctx, binary, "migrate", "status", "--url", url, "--dir", dir,
				"--revisions-schema", compatDir)
			c.Assert(statusErr, qt.IsNil, qt.Commentf("migrate status:\n%s", status))
			c.Assert(status, qt.Contains, "Migration Status: OK")
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+compatDir+"/atlas_schema_revisions`"), qt.Equals, int64(2))
			rolledBack, _, downErr := runCompat(ctx, binary, "migrate", "down", "--url", url, "--dir", dir,
				"--revisions-schema", compatDir, "--to-version", "20260101000000")
			c.Assert(downErr, qt.IsNil, qt.Commentf("migrate down:\n%s", rolledBack))
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+compatDir+"/atlas_schema_revisions`"), qt.Equals, int64(1))
			c.Assert(tableColumns(c, conn, compatDir, "items"), qt.DeepEquals, []string{"items.id"})

			c.Assert(conn.Writer().ExecuteSQL(ctx,
				"UPSERT INTO `"+compatRootTable+"` (id, note) VALUES (1l, 'start'u)"), qt.IsNil)
			ran, _, runErr := runCompat(ctx, binary, "script", "exec", "--url", url, "--file", scripts,
				"--run", "note", "--quiet")
			c.Assert(runErr, qt.IsNil, qt.Commentf("script exec:\n%s", ran))
			c.Assert(rootNote(c, conn), qt.Equals, "set")
			_, refused, refusedErr := runCompat(ctx, binary, "script", "exec", "--url", url, "--file", scripts,
				"--run", "checked")
			c.Assert(refusedErr, qt.IsNotNil)
			c.Assert(refused, qt.Contains, `expect_rows is 1, and this driver does not report a row count`)
			c.Assert(rootNote(c, conn), qt.Equals, "set")
		})
	}
}

// buildCompatBinary builds ptah-compat from this checkout.
func buildCompatBinary(c *qt.C, ctx context.Context) string {
	c.Helper()
	_, file, _, ok := runtime.Caller(0)
	c.Assert(ok, qt.IsTrue)
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	binary := filepath.Join(c.TempDir(), "ptah-compat"+exeext.Suffix)
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/ptah-compat")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go build:\n%s", output))
	return binary
}

// runCompat runs the compatibility binary and returns its two streams apart:
// schema inspect writes the document to standard output and its notes to
// standard error.
func runCompat(ctx context.Context, binary string, args ...string) (stdout, stderr string, err error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err = cmd.Run()
	return out.String(), errOut.String(), err
}

// writeCompatFile writes body to name in dir and returns the path, with forward
// slashes so it can follow file://.
func writeCompatFile(c *qt.C, dir, name, body string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return filepath.ToSlash(path)
}

// dropCompatTables drops what the test creates: the root table, the tables in
// compatDir and the revision table there.
func dropCompatTables(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	c.Assert(conn.Writer().ExecuteSQL(context.Background(),
		"DROP TABLE IF EXISTS `"+compatRootTable+"`"), qt.IsNil)
	dropDirectory(c, conn, compatDir, "orders", "items")
}

// tableColumns reads the columns of one table back through the reader.
func tableColumns(c *qt.C, conn *dbschema.DatabaseConnection, schema, table string) []string {
	c.Helper()
	columns := make([]string, 0)
	for _, live := range readScoped(c, conn, []string{schema}).Tables {
		for _, column := range live.Columns {
			columns = append(columns, live.Name+"."+column.Name)
		}
	}
	return slices.DeleteFunc(columns, func(name string) bool { return !strings.HasPrefix(name, table+".") })
}

// rootNote reads the root table's one row back.
func rootNote(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var note string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT note FROM `"+compatRootTable+"` WHERE id = 1l").Scan(&note),
		qt.IsNil)
	return note
}

// ptah-compat schema apply takes the schema apply lock on YDB, a semaphore on
// the coordination node, as it takes an advisory lock on the engines that have
// one: while another session holds it, a run under --lock-timeout gives up
// with the timeout and creates nothing.
func TestYDBCompatBinary_SchemaApplyWaitsForTheLock(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropCompatTables(c, conn)
			c.Cleanup(func() { dropCompatTables(c, conn) })
			desired := writeCompatFile(c, c.TempDir(), "desired.hcl", compatDesired)

			lock, err := dblock.Acquire(ctx, openYDB(c, line), "ptah_schema_apply", 0)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = lock.Release(context.Background()) })
			_, stderr, applyErr := runCompat(ctx, binary, "schema", "apply", "--url", url,
				"--to", "file://"+desired, "--auto-approve", "--lock-timeout", "1s")

			c.Assert(applyErr, qt.IsNotNil)
			c.Assert(stderr, qt.Equals,
				"Error: acquire schema apply lock: timed out acquiring advisory lock \"ptah_schema_apply\" on ydb after 1s\n")
			c.Assert(tableNames(readScoped(c, conn, []string{compatDir})), qt.HasLen, 0)
		})
	}
}
