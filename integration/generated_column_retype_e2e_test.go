//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// PostgreSQL refuses to change the type of a column a generated column reads:
//
//	ERROR: cannot alter type of a column used by a generated column
//
// These tests hold public.prices with nickname varchar(40), the stored
// generated column search_vector reading it, a GIN index and a partial GIN
// index on search_vector, a comment on it and one row, and declare nickname
// varchar(100). Each shipped binary plans and applies the change, and each
// test reads the table back from the catalog.

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
)

// newGeneratedRetypeFixture is a database holding the table and its row, and a
// schema file declaring desired.
func newGeneratedRetypeFixture(c *qt.C, desired string) includeChildFixture {
	c.Helper()
	fixture := includeChildFixture{database: postgresScratchDevURL(c, "ptah_generated_retype")}
	execPostgresFamilySQL(c, c.Context(), fixture.database, generatedRetypeTable+generatedRetypeRow)
	fixture.schema = filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(fixture.schema, []byte(desired), 0o600), qt.IsNil)
	return fixture
}

// TestSchemaApplyRetypesAColumnAGeneratedColumnReadsE2E applies through
// `ptah schema apply` and `ptah-compat schema apply`. The plan drops
// search_vector, changes nickname and adds search_vector back, so the table
// ends with the new type, both indexes as they were, the comment and the
// recomputed value. A second comparison finds nothing left to do.
func TestSchemaApplyRetypesAColumnAGeneratedColumnReadsE2E(t *testing.T) {
	t.Run("ptah", func(t *testing.T) {
		c := qt.New(t)
		fixture := newGeneratedRetypeFixture(c, generatedRetypeDesired)

		applied := fixture.apply(c)

		c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
		c.Assert(fixture.read(c, generatedRetypeReadBack), qt.Equals, generatedRetypeAfter)
		converged := fixture.compare(c)
		c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
		c.Assert(comparisonBody(c, converged.Stdout), qt.Equals, includeNoDiffs)
	})
	t.Run("ptah-compat", func(t *testing.T) {
		c := qt.New(t)
		fixture := newGeneratedRetypeFixture(c, generatedRetypeDesired)

		applied := fixture.compatApply(c)

		c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
		c.Assert(fixture.read(c, generatedRetypeReadBack), qt.Equals, generatedRetypeAfter)
		converged := fixture.compatDiff(c)
		c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
		c.Assert(converged.Stdout, qt.Equals, includeSynced)
	})
}

// generatedRetypeRefusals are applies that must leave the table as it was:
// one where the partial index sits outside the selected scope, which the plan
// cannot restate and the guard refuses before the drop, and one where an
// unrelated NOT NULL change after the drop fails on the row, which rolls the
// rebuild back with it.
var generatedRetypeRefusals = []struct {
	name    string
	desired string
	flags   []string
	want    string
}{
	{
		name: "an index outside the scope",
		desired: "CREATE TABLE public.prices (id integer PRIMARY KEY, nickname varchar(100), note text, " +
			"search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, nickname)) STORED);\n" +
			"CREATE INDEX prices_search ON public.prices USING gin (search_vector);\n" +
			"COMMENT ON COLUMN public.prices.search_vector IS 'search';\n",
		flags: []string{"--exclude", "public.prices.user_search_index"},
		want: `refusing to rebuild generated column public.prices.search_vector around the type change of nickname: ` +
			`dropping it would take index user_search_index, which the plan does not restate`,
	},
	{
		name: "a NOT NULL change that fails",
		desired: "CREATE TABLE public.prices (id integer PRIMARY KEY, nickname varchar(100), note text NOT NULL, " +
			"search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, nickname)) STORED);\n" +
			"CREATE INDEX prices_search ON public.prices USING gin (search_vector);\n" +
			"CREATE INDEX user_search_index ON public.prices USING gin (search_vector) WHERE id > 0;\n" +
			"COMMENT ON COLUMN public.prices.search_vector IS 'search';\n",
		want: `column "note" of relation "prices" contains null values (SQLSTATE 23502)`,
	},
}

// TestSchemaApplyLeavesAGeneratedColumnRebuildUndoneE2E applies each refusal
// through both binaries and reads the table back unchanged.
func TestSchemaApplyLeavesAGeneratedColumnRebuildUndoneE2E(t *testing.T) {
	for _, row := range generatedRetypeRefusals {
		t.Run(row.name+"/ptah", func(t *testing.T) {
			c := qt.New(t)
			fixture := newGeneratedRetypeFixture(c, row.desired)

			applied := fixture.apply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, row.want)
			c.Assert(fixture.read(c, generatedRetypeReadBack), qt.Equals, generatedRetypeBefore)
		})
		t.Run(row.name+"/ptah-compat", func(t *testing.T) {
			c := qt.New(t)
			fixture := newGeneratedRetypeFixture(c, row.desired)

			applied := fixture.compatApply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 1, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, row.want)
			c.Assert(fixture.read(c, generatedRetypeReadBack), qt.Equals, generatedRetypeBefore)
		})
	}
}

// TestMigrateDiffRetypesAColumnAGeneratedColumnReadsE2E writes the change as a
// versioned migration with `ptah-compat migrate diff` over a directory that
// creates the table, then applies the directory to a database whose table
// holds the row by the time the migration runs.
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
	target := includeChildFixture{database: postgresScratchDevURL(c, "ptah_generated_retype_target")}
	created := compat("migrate", "apply", "--dir", dirURL, "--url", target.database, "1")
	c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
	execPostgresFamilySQL(c, c.Context(), target.database, generatedRetypeRow)
	applied := compat("migrate", "apply", "--dir", dirURL, "--url", target.database)
	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(target.read(c, generatedRetypeReadBack), qt.Equals, generatedRetypeAfter)
}
