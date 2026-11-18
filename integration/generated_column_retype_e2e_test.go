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

// PostgreSQL refuses to change the type of a column a generated column reads:
//
//	ERROR: cannot alter type of a column used by a generated column
//
// Most of these tests hold public.prices with nickname varchar(40), the stored
// generated column search_vector reading it, a GIN index and a partial GIN
// index on search_vector, a comment on it and one row, and declare nickname
// varchar(100). The others hold public.labels, whose two generated columns
// read nickname, one of them NOT NULL, with an index and a CHECK over both.
// Each shipped binary plans and applies the change, and each test reads the
// table back from the catalog.

const (
	generatedRetypeTable = "CREATE TABLE public.prices (id integer PRIMARY KEY, nickname varchar(40), note text, " +
		"search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, nickname)) STORED);\n" +
		"CREATE INDEX prices_search ON public.prices USING gin (search_vector);\n" +
		"CREATE INDEX user_search_index ON public.prices USING gin (search_vector) WHERE id > 0;\n" +
		"COMMENT ON COLUMN public.prices.search_vector IS 'search';\n"
	generatedRetypeRow     = "INSERT INTO public.prices (id, nickname) VALUES (1, 'apple');\n"
	generatedRetypeDesired = "CREATE TABLE public.prices (id integer PRIMARY KEY, nickname varchar(100), note text, " +
		"search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, nickname)) STORED);\n" +
		"CREATE INDEX prices_search ON public.prices USING gin (search_vector);\n" +
		"CREATE INDEX user_search_index ON public.prices USING gin (search_vector) WHERE id > 0;\n" +
		"COMMENT ON COLUMN public.prices.search_vector IS 'search';\n"
	// generatedRetypeReadBack answers nickname's type, the indexes of the
	// table, the generated values, the comment on search_vector and whether
	// note takes NULL.
	generatedRetypeReadBack = `SELECT format_type(a.atttypid, a.atttypmod)
		|| ' | ' || (SELECT string_agg(indexdef, ' | ' ORDER BY indexname) FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'prices')
		|| ' | ' || (SELECT string_agg(search_vector::text, ',' ORDER BY id) FROM public.prices)
		|| ' | ' || COALESCE(col_description('public.prices'::regclass,
			(SELECT attnum FROM pg_attribute WHERE attrelid = 'public.prices'::regclass AND attname = 'search_vector')), '')
		|| ' | note ' || (SELECT is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'prices' AND column_name = 'note')
		FROM pg_attribute a WHERE a.attrelid = 'public.prices'::regclass AND a.attname = 'nickname'`
	generatedRetypeIndexes = "CREATE UNIQUE INDEX prices_pkey ON public.prices USING btree (id) | " +
		"CREATE INDEX prices_search ON public.prices USING gin (search_vector) | " +
		"CREATE INDEX user_search_index ON public.prices USING gin (search_vector) WHERE (id > 0)"
	generatedRetypeAfter  = "character varying(100) | " + generatedRetypeIndexes + " | 'apple':1 | search | note YES"
	generatedRetypeBefore = "character varying(40) | " + generatedRetypeIndexes + " | 'apple':1 | search | note YES"

	labelsColumns = "a text GENERATED ALWAYS AS (upper(nickname)) STORED NOT NULL, " +
		"b text GENERATED ALWAYS AS (lower(nickname)) STORED, CONSTRAINT labels_a_b CHECK (a <> b));\n" +
		"CREATE INDEX labels_ab ON public.labels (a, b);\n"
	labelsTable   = "CREATE TABLE public.labels (id integer PRIMARY KEY, nickname varchar(40), " + labelsColumns
	labelsRow     = "INSERT INTO public.labels (id, nickname) VALUES (1, 'Apple');\n"
	labelsDesired = "CREATE TABLE public.labels (id integer PRIMARY KEY, nickname varchar(100), " + labelsColumns
	// labelsReadBack answers nickname's type, the indexes and the CHECK
	// constraints of the table, whether a takes NULL, the generated values,
	// and the collation of b.
	labelsReadBack = `SELECT format_type(a.atttypid, a.atttypmod)
		|| ' | ' || (SELECT string_agg(indexdef, ' | ' ORDER BY indexname) FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'labels')
		|| ' | ' || (SELECT string_agg(conname || ' ' || pg_get_constraintdef(oid), ' | ' ORDER BY conname)
			FROM pg_constraint WHERE conrelid = 'public.labels'::regclass AND contype = 'c')
		|| ' | a ' || (SELECT is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'labels' AND column_name = 'a')
		|| ' | ' || (SELECT string_agg(a || ' ' || b, ',' ORDER BY id) FROM public.labels)
		|| ' | b ' || (SELECT COALESCE(collation_name, 'default') FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'labels' AND column_name = 'b')
		FROM pg_attribute a WHERE a.attrelid = 'public.labels'::regclass AND a.attname = 'nickname'`
	labelsIndexes = "CREATE INDEX labels_ab ON public.labels USING btree (a, b) | " +
		"CREATE UNIQUE INDEX labels_pkey ON public.labels USING btree (id)"
	labelsConstraints = "labels_a_b CHECK ((a <> b)) | a NO"
	labelsAfter       = "character varying(100) | " + labelsIndexes + " | " + labelsConstraints + " | APPLE apple | b default"
)

// newGeneratedRetypeFixture is a database holding the table and its row, and a
// schema file declaring desired.
func newGeneratedRetypeFixture(c *qt.C, desired string) includeChildFixture {
	c.Helper()
	return newRebuildFixture(c, generatedRetypeTable+generatedRetypeRow, desired)
}

// newRebuildFixture is a database holding live, and a schema file declaring
// desired.
func newRebuildFixture(c *qt.C, live, desired string) includeChildFixture {
	c.Helper()
	fixture := includeChildFixture{database: postgresScratchDevURL(c, "ptah_generated_retype")}
	execPostgresFamilySQL(c, c.Context(), fixture.database, live)
	fixture.schema = filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(fixture.schema, []byte(desired), 0o600), qt.IsNil)
	return fixture
}

// generatedRetypeRows are the tables each binary rebuilds. readBack answers
// what the test compares with want after the apply.
var generatedRetypeRows = []struct {
	name     string
	live     string
	desired  string
	readBack string
	want     string
}{
	{
		name:     "a GIN and a partial GIN index",
		live:     generatedRetypeTable + generatedRetypeRow,
		desired:  generatedRetypeDesired,
		readBack: generatedRetypeReadBack,
		want:     generatedRetypeAfter,
	},
	{
		name:     "two generated columns, NOT NULL, an index and a CHECK over both",
		live:     labelsTable + labelsRow,
		desired:  labelsDesired,
		readBack: labelsReadBack,
		want:     labelsAfter,
	},
}

// TestSchemaApplyRetypesAColumnAGeneratedColumnReadsE2E applies through
// `ptah schema apply` and `ptah-compat schema apply`. The plan drops the
// generated columns, changes nickname and adds them back, so the table ends
// with the new type, its indexes, constraints and comments as they were, and
// the recomputed values. A second comparison finds nothing left to do.
func TestSchemaApplyRetypesAColumnAGeneratedColumnReadsE2E(t *testing.T) {
	for _, row := range generatedRetypeRows {
		t.Run(row.name+"/ptah", func(t *testing.T) {
			c := qt.New(t)
			fixture := newRebuildFixture(c, row.live, row.desired)

			applied := fixture.apply(c)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(fixture.read(c, row.readBack), qt.Equals, row.want)
			converged := fixture.compare(c)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(comparisonBody(c, converged.Stdout), qt.Equals, includeNoDiffs)
		})
		t.Run(row.name+"/ptah-compat", func(t *testing.T) {
			c := qt.New(t)
			fixture := newRebuildFixture(c, row.live, row.desired)

			applied := fixture.compatApply(c)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(fixture.read(c, row.readBack), qt.Equals, row.want)
			converged := fixture.compatDiff(c)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(converged.Stdout, qt.Equals, includeSynced)
		})
	}
}

// generatedRetypeRefusals are applies that must leave the table as it was:
// one where the partial index sits outside the selected scope, which the plan
// cannot restate and the guard refuses before the drop; one where an
// unrelated NOT NULL change after the drop fails on the row, which rolls the
// rebuild back with it; and one where a generated column has a collation of
// its own, which the add could not restate.
var generatedRetypeRefusals = []struct {
	name     string
	live     string
	desired  string
	flags    []string
	want     string
	readBack string
	before   string
}{
	{
		name:     "an index outside the scope",
		live:     generatedRetypeTable + generatedRetypeRow,
		readBack: generatedRetypeReadBack,
		before:   generatedRetypeBefore,
		desired: "CREATE TABLE public.prices (id integer PRIMARY KEY, nickname varchar(100), note text, " +
			"search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, nickname)) STORED);\n" +
			"CREATE INDEX prices_search ON public.prices USING gin (search_vector);\n" +
			"COMMENT ON COLUMN public.prices.search_vector IS 'search';\n",
		flags: []string{"--exclude", "public.prices.user_search_index"},
		want: `refusing to rebuild generated column public.prices.search_vector around the type change of nickname: ` +
			`dropping it would take index user_search_index, which the plan does not restate`,
	},
	{
		name:     "a NOT NULL change that fails",
		live:     generatedRetypeTable + generatedRetypeRow,
		readBack: generatedRetypeReadBack,
		before:   generatedRetypeBefore,
		desired: "CREATE TABLE public.prices (id integer PRIMARY KEY, nickname varchar(100), note text NOT NULL, " +
			"search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, nickname)) STORED);\n" +
			"CREATE INDEX prices_search ON public.prices USING gin (search_vector);\n" +
			"CREATE INDEX user_search_index ON public.prices USING gin (search_vector) WHERE id > 0;\n" +
			"COMMENT ON COLUMN public.prices.search_vector IS 'search';\n",
		want: `column "note" of relation "prices" contains null values (SQLSTATE 23502)`,
	},
	{
		name:     "a collation of its own",
		live:     strings.Replace(labelsTable, "b text GENERATED", `b text COLLATE "C" GENERATED`, 1) + labelsRow,
		desired:  strings.Replace(labelsDesired, "b text GENERATED", `b text COLLATE "C" GENERATED`, 1),
		readBack: labelsReadBack,
		before:   "character varying(40) | " + labelsIndexes + " | " + labelsConstraints + " | APPLE apple | b C",
		want:     `generated column public.labels.b reads column nickname, whose type this plan changes, and has the collation C`,
	},
}

// TestSchemaApplyLeavesAGeneratedColumnRebuildUndoneE2E applies each refusal
// through both binaries and reads the table back unchanged.
func TestSchemaApplyLeavesAGeneratedColumnRebuildUndoneE2E(t *testing.T) {
	for _, row := range generatedRetypeRefusals {
		t.Run(row.name+"/ptah", func(t *testing.T) {
			c := qt.New(t)
			fixture := newRebuildFixture(c, row.live, row.desired)

			applied := fixture.apply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, row.want)
			c.Assert(fixture.read(c, row.readBack), qt.Equals, row.before)
		})
		t.Run(row.name+"/ptah-compat", func(t *testing.T) {
			c := qt.New(t)
			fixture := newRebuildFixture(c, row.live, row.desired)

			applied := fixture.compatApply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 1, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, row.want)
			c.Assert(fixture.read(c, row.readBack), qt.Equals, row.before)
		})
	}
}

// TestMigrateDiffRetypesAColumnAGeneratedColumnReadsE2E writes the change as a
// versioned migration with `ptah-compat migrate diff` over a directory that
// creates the table, lints it, then applies the directory to a database whose
// table holds the row by the time the migration runs. The lint reports no
// destructive change: the column the file drops comes back as generated.
func TestMigrateDiffRetypesAColumnAGeneratedColumnReadsE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_prices.sql"), []byte(generatedRetypeTable), 0o600), qt.IsNil)
	desired := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(desired, []byte(generatedRetypeDesired), 0o600), qt.IsNil)
	dirURL := "file://" + filepath.ToSlash(dir)
	compat := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, args...)
	}
	hashed := compat("migrate", "hash", "--dir", dirURL)
	c.Assert(hashed.ExitCode, qt.Equals, 0, historyComment(hashed))

	diffed := compat("migrate", "diff", "retype", "--dir", dirURL, "--to", "file://"+filepath.ToSlash(desired),
		"--dev-url", postgresScratchDevURL(c, "ptah_generated_retype_dev"))

	c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
	linted := compat("migrate", "lint", "--dir", dirURL, "--dev-url", postgresScratchDevURL(c, "ptah_generated_retype_lint"), "--latest", "1")
	c.Assert(linted.ExitCode, qt.Equals, 0, historyComment(linted))
	c.Assert(linted.Stdout, qt.Not(qt.Contains), "destructive changes detected")
	target := includeChildFixture{database: postgresScratchDevURL(c, "ptah_generated_retype_target")}
	created := compat("migrate", "apply", "--dir", dirURL, "--url", target.database, "1")
	c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
	execPostgresFamilySQL(c, c.Context(), target.database, generatedRetypeRow)
	applied := compat("migrate", "apply", "--dir", dirURL, "--url", target.database)
	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(target.read(c, generatedRetypeReadBack), qt.Equals, generatedRetypeAfter)
}

// TestMigrationsUpAppliesAGeneratedColumnRebuildE2E writes the change with
// `ptah migrations generate` against a database holding the row and applies it
// with `ptah migrations up`. The destructive-statement check before the run
// leaves the rebuild's DROP COLUMN alone, so the run needs no
// --allow-destructive.
func TestMigrationsUpAppliesAGeneratedColumnRebuildE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newGeneratedRetypeFixture(c, generatedRetypeDesired)
	dir := c.TempDir()
	generated := runPtahIn(c, "migrations", "generate", "--db-url", fixture.database, "--schema-file", fixture.schema,
		"--migrations-dir", dir, "--name", "retype")
	c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))

	applied := runPtahIn(c, "migrations", "up", "--db-url", fixture.database, "--migrations-dir", dir)

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(fixture.read(c, generatedRetypeReadBack), qt.Equals, generatedRetypeAfter)
}
