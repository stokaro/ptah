//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A catalog types the literal a column default stores: PostgreSQL keeps
// `DEFAULT 'abc'` on a varchar column as `'abc'::character varying`. Each test
// here holds a table in a live database and a schema file, and runs `ptah
// schema apply --dry-run` and `ptah-compat migrate diff` over a directory whose
// one migration creates the live table.
//
// PostgreSQL and YugabyteDB spell each declared default on the server, and a
// default the server spells as the catalog holds it is the same default.
// Otherwise, and on CockroachDB, which has no such probe, the comparison folds
// the casts that cannot change the value and keeps the casts that can.

// defaultCastServers are the engines each test runs on.
var defaultCastServers = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "postgres", engine: dbtarget.PostgreSQL},
	{name: "cockroachdb", engine: dbtarget.CockroachDB},
}

// defaultCastTable is public.defaults with the column n declared as column.
func defaultCastTable(column string) string {
	return `CREATE TABLE "public"."defaults" ("id" integer PRIMARY KEY, "n" ` + column + `);` + "\n"
}

// defaultCastDryRun is `ptah schema apply --dry-run` against a database on
// engine's server holding live, with desired as the schema file.
func defaultCastDryRun(c *qt.C, engine dbtarget.Engine, live, desired string) clirun.Result {
	c.Helper()
	database := newPolicyClauseDatabase(c, engine)
	execPostgresFamilySQL(c, c.Context(), database, live)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	return runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")
}

// defaultCastMigrateDiff runs `ptah-compat migrate diff next` over a directory
// whose one migration is live, against desired, with a dev database on
// engine's server. It returns the run and the text of the migration it wrote,
// empty when it wrote none.
func defaultCastMigrateDiff(c *qt.C, engine dbtarget.Engine, live, desired string) (clirun.Result, string) {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"), []byte(live), 0o600), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	dirURL := "file://" + filepath.ToSlash(dir)
	compat := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, args...)
	}
	hashed := compat("migrate", "hash", "--dir", dirURL)
	c.Assert(hashed.ExitCode, qt.Equals, 0, historyComment(hashed))

	diffed := compat("migrate", "diff", "next", "--dir", dirURL, "--to", "file://"+filepath.ToSlash(schema),
		"--dev-url", newPolicyClauseDatabase(c, engine))

	written, err := filepath.Glob(filepath.Join(dir, "*_next.sql"))
	c.Assert(err, qt.IsNil)
	var text strings.Builder
	for _, path := range written {
		content, err := os.ReadFile(path)
		c.Assert(err, qt.IsNil)
		text.Write(content)
	}
	return diffed, text.String()
}

// defaultCastUnchanged are defaults whose two sides hold one value and are
// stored as one expression. A cast of a decimal to numeric is not stored, and
// the same cast on both sides changes nothing. CockroachDB stores
// `ARRAY[]::text[]` on a `text[]` column as `ARRAY[]`, without the cast to the
// column's own type.
var defaultCastUnchanged = []struct {
	name    string
	live    string
	desired string
}{
	{
		name:    "a cast of a decimal to numeric in the declaration only",
		live:    defaultCastTable(`numeric DEFAULT 1.5`),
		desired: defaultCastTable(`numeric DEFAULT (1.5::numeric)`),
	},
	{
		name:    "the same rounding cast on both sides",
		live:    defaultCastTable(`numeric DEFAULT (1.5::integer)`),
		desired: defaultCastTable(`numeric DEFAULT (1.5::integer)`),
	},
	{
		name:    "the same truncating cast on both sides",
		live:    defaultCastTable(`varchar(10) DEFAULT 'abcd'::varchar(3)`),
		desired: defaultCastTable(`varchar(10) DEFAULT 'abcd'::varchar(3)`),
	},
	{
		name:    "the same cast to date on both sides",
		live:    defaultCastTable(`timestamp DEFAULT now()::date`),
		desired: defaultCastTable(`timestamp DEFAULT now()::date`),
	},
	{
		name:    "an array cast to the column's type on both sides",
		live:    defaultCastTable(`text[] DEFAULT ARRAY[]::text[]`),
		desired: defaultCastTable(`text[] DEFAULT ARRAY[]::text[]`),
	},
}

// TestDefaultCastThatKeepsTheValuePlansNothingE2E holds each path to no change
// on every engine, and the native run to no warning that a default is compared
// as written: a run names only a column whose type or default it changes.
func TestDefaultCastThatKeepsTheValuePlansNothingE2E(t *testing.T) {
	for _, row := range defaultCastUnchanged {
		for _, server := range defaultCastServers {
			t.Run(row.name+"/"+server.name+"/ptah", func(t *testing.T) {
				c := qt.New(t)

				planned := defaultCastDryRun(c, server.engine, row.live, row.desired)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
				c.Assert(planned.Stderr, qt.Not(qt.Contains), "compared as written")
			})
			t.Run(row.name+"/"+server.name+"/migrate-diff", func(t *testing.T) {
				c := qt.New(t)

				diffed, written := defaultCastMigrateDiff(c, server.engine, row.live, row.desired)

				c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
				c.Assert(diffed.Stdout, qt.Equals, "The migration directory is synced with the desired state, no changes to be made\n")
				c.Assert(written, qt.Equals, "")
			})
		}
	}
}

// defaultCastChanged are defaults whose only difference is a cast that changes
// the value: a cast to integer rounds 1.5 to 2, a length truncates 'abcd', and
// a precision rounds 1.5.
var defaultCastChanged = []struct {
	name    string
	live    string
	desired string
}{
	{
		name:    "a cast that rounds in the declaration",
		live:    defaultCastTable(`numeric DEFAULT 1.5`),
		desired: defaultCastTable(`numeric DEFAULT (1.5::integer)`),
	},
	{
		name:    "a cast that rounds in the live table",
		live:    defaultCastTable(`numeric DEFAULT (1.5::integer)`),
		desired: defaultCastTable(`numeric DEFAULT 1.5`),
	},
	{
		name:    "a length that truncates",
		live:    defaultCastTable(`varchar(10) DEFAULT 'abcd'`),
		desired: defaultCastTable(`varchar(10) DEFAULT 'abcd'::varchar(3)`),
	},
	{
		name:    "a precision that rounds",
		live:    defaultCastTable(`numeric DEFAULT 1.5`),
		desired: defaultCastTable(`numeric DEFAULT '1.5'::numeric(2,0)`),
	},
}

// TestDefaultCastThatChangesTheValuePlansE2E holds each path to a new default
// on every engine, and the native run to no warning that a default is compared
// as written. The run warns only about a column whose default it changes, and
// CockroachDB, which offers no temporary table, refuses no column: without
// that reading every change there would carry the warning.
func TestDefaultCastThatChangesTheValuePlansE2E(t *testing.T) {
	const change = `ALTER TABLE "public"."defaults" ALTER COLUMN "n" SET DEFAULT `
	for _, row := range defaultCastChanged {
		for _, server := range defaultCastServers {
			t.Run(row.name+"/"+server.name+"/ptah", func(t *testing.T) {
				c := qt.New(t)

				planned := defaultCastDryRun(c, server.engine, row.live, row.desired)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Contains, change)
				c.Assert(planned.Stderr, qt.Not(qt.Contains), "compared as written")
			})
			t.Run(row.name+"/"+server.name+"/migrate-diff", func(t *testing.T) {
				c := qt.New(t)

				diffed, written := defaultCastMigrateDiff(c, server.engine, row.live, row.desired)

				c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
				c.Assert(written, qt.Contains, change)
			})
		}
	}
}

// spelledDefaultServers are the engines whose server spells a declared
// default before it is compared.
var spelledDefaultServers = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "postgres", engine: dbtarget.PostgreSQL},
	{name: "yugabytedb", engine: dbtarget.YugabyteDB},
}

// defaultInAnotherForm are defaults whose two sides hold one value and are
// stored as two expressions: PostgreSQL stores `DEFAULT 0` on a bigint column
// as 0 and `DEFAULT (0::bigint)` as '0'::bigint, and `DEFAULT 'abc'::text` on
// a varchar column as 'abc'::text.
var defaultInAnotherForm = []struct {
	name    string
	live    string
	desired string
}{
	{
		name:    "a cast to text in the declaration only",
		live:    defaultCastTable(`varchar(10) DEFAULT 'abc'`),
		desired: defaultCastTable(`varchar(10) DEFAULT 'abc'::text`),
	},
	{
		name:    "a cast of an integer to bigint in the declaration only",
		live:    defaultCastTable(`bigint DEFAULT 0`),
		desired: defaultCastTable(`bigint DEFAULT (0::bigint)`),
	},
}

// TestDefaultInAnotherFormPlansNothingE2E holds the dry run and the migration
// to no change on each engine, whether the server spells the declaration or
// not.
func TestDefaultInAnotherFormPlansNothingE2E(t *testing.T) {
	servers := append(slices.Clone(spelledDefaultServers), defaultCastServers[1])
	for _, row := range defaultInAnotherForm {
		for _, server := range servers {
			t.Run(row.name+"/"+server.name+"/ptah", func(t *testing.T) {
				c := qt.New(t)

				planned := defaultCastDryRun(c, server.engine, row.live, row.desired)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			})
		}
		for _, server := range defaultCastServers {
			t.Run(row.name+"/"+server.name+"/migrate-diff", func(t *testing.T) {
				c := qt.New(t)

				diffed, written := defaultCastMigrateDiff(c, server.engine, row.live, row.desired)

				c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
				c.Assert(written, qt.Equals, "")
			})
		}
	}
}

// retypedColumn is public.defaults with n a varchar(40) holding the default
// 'abc'. ALTER COLUMN TYPE casts the default to the new type and keeps it as
// varchar spelled it: on a column changed to text the catalog holds
// 'abc'::character varying, where the server spells a declared 'abc' as
// 'abc'::text.
var retypedColumn = defaultCastTable(`varchar(40) DEFAULT 'abc'`)

// TestDefaultOfARetypedColumnConvergesE2E changes n to text and keeps its
// default. The plan changes the type alone, as the community binary writes it,
// and a second run plans nothing.
func TestDefaultOfARetypedColumnConvergesE2E(t *testing.T) {
	desired := defaultCastTable(`text DEFAULT 'abc'`)
	for _, server := range spelledDefaultServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			database := newPolicyClauseDatabase(c, server.engine)
			execPostgresFamilySQL(c, c.Context(), database, retypedColumn)
			schema := writeDeclaredSchemaFile(c, "schema.sql", desired)

			planned := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")
			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
			replanned := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Contains, `ALTER COLUMN "n" TYPE text`)
			c.Assert(planned.Stdout, qt.Not(qt.Contains), "SET DEFAULT")
			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(storedColumnDefault(c, database), qt.Equals, "'abc'::character varying")
			c.Assert(replanned.ExitCode, qt.Equals, 0, historyComment(replanned))
			c.Assert(replanned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
	t.Run("postgres/migrate-diff", func(t *testing.T) {
		c := qt.New(t)

		diffed, written := defaultCastMigrateDiff(c, dbtarget.PostgreSQL, retypedColumn, desired)

		c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
		c.Assert(written, qt.Contains, `ALTER COLUMN "n" TYPE text`)
		c.Assert(written, qt.Not(qt.Contains), "SET DEFAULT")
	})
}

// TestChangedDefaultOfARetypedColumnPlansE2E changes n to text and its
// default to 'abd'. The plan sets the default with the type, and a second run
// plans nothing.
func TestChangedDefaultOfARetypedColumnPlansE2E(t *testing.T) {
	desired := defaultCastTable(`text DEFAULT 'abd'`)
	for _, server := range spelledDefaultServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			database := newPolicyClauseDatabase(c, server.engine)
			execPostgresFamilySQL(c, c.Context(), database, retypedColumn)
			schema := writeDeclaredSchemaFile(c, "schema.sql", desired)

			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
			replanned := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(storedColumnDefault(c, database), qt.Equals, "'abd'::text")
			c.Assert(replanned.ExitCode, qt.Equals, 0, historyComment(replanned))
			c.Assert(replanned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
	t.Run("postgres/migrate-diff", func(t *testing.T) {
		c := qt.New(t)

		diffed, written := defaultCastMigrateDiff(c, dbtarget.PostgreSQL, retypedColumn, desired)

		c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
		c.Assert(written, qt.Contains, `ALTER COLUMN "n" TYPE text`)
		c.Assert(written, qt.Contains, `SET DEFAULT 'abd'`)
	})
}

// storedColumnDefault reads back the default of public.defaults.n as the
// server prints it.
func storedColumnDefault(c *qt.C, database string) string {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, database))
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	var stored string
	c.Assert(db.QueryRowContext(c.Context(), `SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d
		JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
		WHERE d.adrelid = 'public.defaults'::regclass AND a.attname = 'n'`).Scan(&stored), qt.IsNil)
	return stored
}

// TestDefaultWithoutTheTempPrivilegeWarnsE2E connects as a role that may not
// create a temporary table, so the server cannot spell the declared default.
// The run names the column and compares the default as written: PostgreSQL
// stores '2020-01-01' on a timestamptz column with its time and zone filled
// in, so the unchanged default reads as a change. As the owner, which may, the
// same table plans nothing and warns about nothing. The column m reads the
// same written or stored, so the comparison leaves it alone and the warning
// does not name it. One column is named, so the warning names it rather than
// counting columns.
func TestDefaultWithoutTheTempPrivilegeWarnsE2E(t *testing.T) {
	const warning = "warning: column public.stamps.n has its type and default compared as written, " +
		"not in the form the server stores: the server refused the temporary table that spells them: " +
		"ERROR: permission denied to create temporary tables in database"
	const table = `CREATE TABLE "public"."stamps" ("n" timestamptz DEFAULT '2020-01-01', "m" integer DEFAULT 1);` + "\n"
	c := qt.New(t)
	database := newPolicyClauseDatabase(c, dbtarget.PostgreSQL)
	execPostgresFamilySQL(c, c.Context(), database, table)
	schema := writeDeclaredSchemaFile(c, "schema.sql", table)

	owner := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")
	refused := runPtahIn(c, "schema", "apply", "--db-url", noTempRoleURL(c, database), "--schema-file", schema, "--dry-run")

	c.Assert(owner.ExitCode, qt.Equals, 0, historyComment(owner))
	c.Assert(owner.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	c.Assert(owner.Stderr, qt.Not(qt.Contains), "compared as written")
	c.Assert(refused.ExitCode, qt.Equals, 0, historyComment(refused))
	c.Assert(refused.Stderr, qt.Contains, warning)
	c.Assert(refused.Stderr, qt.Not(qt.Contains), "public.stamps.m")
	c.Assert(refused.Stdout, qt.Contains, `ALTER COLUMN "n" SET DEFAULT`)
}
