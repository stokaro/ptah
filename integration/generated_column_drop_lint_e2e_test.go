//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// These tests drop the column total of orders and add it back as a generated
// column, over a starting table where total is generated already (the shape a
// plan writes to rebuild a generated column) and one where total stores what
// clients wrote. Only the first loses no value, so only the first may pass the
// drop rules and the apply gate. Both shapes read the same in the file, so
// only the starting state tells them apart.

const (
	ordersGenerated = "total numeric GENERATED ALWAYS AS (price * qty) STORED"
	ordersStored    = "total numeric"
	// ordersRegenerate is the migration in the shape `ptah migrations
	// generate` writes it, one statement per change.
	ordersRegenerate = "ALTER TABLE orders DROP COLUMN total;\n" +
		"ALTER TABLE orders ADD COLUMN total numeric GENERATED ALWAYS AS (price * qty) STORED;\n"
	// ordersRegenerateInOne is the same migration in the shape `ptah-compat
	// migrate diff` writes it, one ALTER TABLE.
	ordersRegenerateInOne = "ALTER TABLE orders DROP COLUMN total, " +
		"ADD COLUMN total numeric GENERATED ALWAYS AS (price * qty) STORED;\n"
	// ordersReadBack answers the values of total and whether it is generated.
	ordersReadBack = `SELECT string_agg(total::text, ',' ORDER BY id) || ' generated=' ||
		(SELECT (attgenerated <> '')::text FROM pg_attribute WHERE attrelid = 'orders'::regclass AND attname = 'total')
		FROM orders`
	compatDropFinding = `Dropping non-virtual column "total"`
)

// ordersTable creates orders with total declared as total.
func ordersTable(total string) string {
	return "CREATE TABLE orders (id integer PRIMARY KEY, price numeric, qty integer, " + total + ");\n"
}

// writeMigrations writes files into a new directory and returns it.
func writeMigrations(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	return dir
}

// generatedDropLintRows are the lint runs over each starting table. drops is
// how many times the run reports the drop of total.
var generatedDropLintRows = []struct {
	name   string
	total  string
	change string
	exit   int
	drops  int
}{
	{name: "native, generated at the start", total: ordersGenerated, change: ordersRegenerate, exit: 0, drops: 0},
	{name: "native, stored at the start", total: ordersStored, change: ordersRegenerate, exit: 1, drops: 1},
}

// TestMigrationsLintReadsWhetherARegeneratedColumnWasGeneratedE2E lints the
// migration with `ptah migrations lint` against a dev database, which replays
// the directory and reads the state the migration starts from. DS102 reports
// the drop of a stored column the file adds back as generated, and not that of
// a generated one.
func TestMigrationsLintReadsWhetherARegeneratedColumnWasGeneratedE2E(t *testing.T) {
	for _, row := range generatedDropLintRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeMigrations(c, map[string]string{
				"0001_orders.up.sql": ordersTable(row.total),
				"0002_total.up.sql":  row.change,
			})

			linted := runPtahIn(c, "migrations", "lint", "--dir", dir, "--dev-url", postgresScratchDevURL(c, "ptah_regenerated_lint"))

			c.Assert(linted.ExitCode, qt.Equals, row.exit, historyComment(linted))
			c.Assert(strings.Count(linted.Stdout+linted.Stderr, "] DS102:"), qt.Equals, row.drops, historyComment(linted))
		})
	}
}

// compatDropLintRows are the compatibility lint runs. atlas v1.3.0 reads a
// drop and an add of one column in one ALTER TABLE as a change of the column
// and reports the drop where the add is a statement of its own, and refuses
// the one-statement form over a stored column; the compatibility surface
// reports both forms over a stored column and the two-statement form over a
// generated one.
var compatDropLintRows = []struct {
	name   string
	total  string
	change string
	exit   int
	drops  int
}{
	{name: "one statement, generated at the start", total: ordersGenerated, change: ordersRegenerateInOne, exit: 0, drops: 0},
	{name: "one statement, stored at the start", total: ordersStored, change: ordersRegenerateInOne, exit: 1, drops: 1},
	{name: "two statements, generated at the start", total: ordersGenerated, change: ordersRegenerate, exit: 1, drops: 1},
	{name: "two statements, stored at the start", total: ordersStored, change: ordersRegenerate, exit: 1, drops: 1},
}

// TestCompatMigrateLintReadsWhetherARegeneratedColumnWasGeneratedE2E lints
// each row with `ptah-compat migrate lint` against a dev database.
func TestCompatMigrateLintReadsWhetherARegeneratedColumnWasGeneratedE2E(t *testing.T) {
	for _, row := range compatDropLintRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeMigrations(c, map[string]string{
				"20260101000000_orders.sql": ordersTable(row.total),
				"20260102000000_total.sql":  row.change,
			})
			dirURL := "file://" + filepath.ToSlash(dir)
			compat := func(args ...string) clirun.Result {
				return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, args...)
			}
			hashed := compat("migrate", "hash", "--dir", dirURL)
			c.Assert(hashed.ExitCode, qt.Equals, 0, historyComment(hashed))

			linted := compat("migrate", "lint", "--dir", dirURL, "--dev-url", postgresScratchDevURL(c, "ptah_regenerated_lint"), "--latest", "1")

			c.Assert(linted.ExitCode, qt.Equals, row.exit, historyComment(linted))
			c.Assert(strings.Count(linted.Stdout, compatDropFinding), qt.Equals, row.drops, historyComment(linted))
		})
	}
}

// generatedDropUpRows are `ptah migrations up` runs of the migration over a
// table holding one row, written price 2, qty 3 and, where total stores what
// clients wrote, total 7.
var generatedDropUpRows = []struct {
	name   string
	total  string
	write  string
	exit   int
	refuse string
	want   string
}{
	{
		name:  "generated at the start",
		total: ordersGenerated,
		write: "INSERT INTO orders (id, price, qty) VALUES (1, 2, 3);\n",
		want:  "6 generated=true",
	},
	{
		name:   "stored at the start",
		total:  ordersStored,
		write:  "INSERT INTO orders (id, price, qty, total) VALUES (1, 2, 3, 7);\n",
		exit:   2,
		refuse: "0002_total.up.sql:1 DS102 error",
		want:   "7 generated=false",
	},
}

// TestMigrationsUpReadsWhetherARegeneratedColumnWasGeneratedE2E applies the
// directory's first version, writes the row, and runs `ptah migrations up`
// for the migration without --allow-destructive. The gate reads the target's
// generated columns before it refuses: it applies the rebuild of a generated
// column, and refuses to turn a stored column into a generated one, which
// leaves the values clients wrote in place.
func TestMigrationsUpReadsWhetherARegeneratedColumnWasGeneratedE2E(t *testing.T) {
	for _, row := range generatedDropUpRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target := includeChildFixture{database: postgresScratchDevURL(c, "ptah_regenerated_up")}
			dir := writeMigrations(c, map[string]string{"0001_orders.up.sql": ordersTable(row.total)})
			created := runPtahIn(c, "migrations", "up", "--db-url", target.database, "--migrations-dir", dir)
			c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
			execPostgresFamilySQL(c, c.Context(), target.database, row.write)
			c.Assert(os.WriteFile(filepath.Join(dir, "0002_total.up.sql"), []byte(ordersRegenerate), 0o600), qt.IsNil)

			applied := runPtahIn(c, "migrations", "up", "--db-url", target.database, "--migrations-dir", dir)

			c.Assert(applied.ExitCode, qt.Equals, row.exit, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, row.refuse)
			c.Assert(target.read(c, ordersReadBack), qt.Equals, row.want)
		})
	}
}
