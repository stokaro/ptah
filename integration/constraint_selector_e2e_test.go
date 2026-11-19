//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// An exclusion names a primary key, a UNIQUE constraint and a CHECK
// constraint through their table. These tests hold all three on public.kinds,
// declare the table without the ones a row drops, and apply with a selector
// through both shipped binaries. Each test reads the constraints back from
// pg_constraint: the one the selector names stays and the others the desired
// schema leaves out are gone.
//
// internal/atlasfilter/constraint_selector_test.go holds the same selectors to
// the filter without a server.

const (
	constraintSelectorLive = "CREATE TABLE public.kinds (id int NOT NULL, code text NOT NULL, " +
		"CONSTRAINT kinds_pkey PRIMARY KEY (id), CONSTRAINT kinds_code_key UNIQUE (code), " +
		"CONSTRAINT code_len CHECK (length(code) > 0));\n"
	// constraintSelectorKeyed declares the key and neither of the others.
	constraintSelectorKeyed = "CREATE TABLE public.kinds (id int NOT NULL, code text NOT NULL, " +
		"CONSTRAINT kinds_pkey PRIMARY KEY (id));\n"
	// constraintSelectorColumnKeyed declares the key on its column, which
	// leaves the column nullable as written.
	constraintSelectorColumnKeyed = "CREATE TABLE public.kinds (id int PRIMARY KEY, code text NOT NULL);\n"
	// constraintSelectorBare declares none of the three.
	constraintSelectorBare = "CREATE TABLE public.kinds (id int NOT NULL, code text NOT NULL);\n"
	// constraintSelectorReadBack answers the constraints public.kinds holds.
	constraintSelectorReadBack = "SELECT string_agg(conname, ',' ORDER BY conname) FROM pg_constraint " +
		"WHERE conrelid = 'public.kinds'::regclass AND contype IN ('p', 'u', 'c')"
)

// constraintSelectorRows name a UNIQUE or CHECK constraint on a URL that names
// no schema.
// desired is the declared schema, dropped a statement the plan holds, kept the
// name it must not mention, and after the constraints the read-back answers.
var constraintSelectorRows = []struct {
	name, desired, dropped, kept, after string
	flags                               []string
}{
	{
		name:    "a unique constraint as an index",
		desired: constraintSelectorKeyed,
		flags:   []string{"--exclude", "public.kinds.kinds_code_key[type=index]"},
		dropped: `DROP CONSTRAINT "code_len"`, kept: `"kinds_code_key"`, after: "kinds_code_key,kinds_pkey",
	},
	{
		name:    "every index of the table",
		desired: constraintSelectorKeyed,
		flags:   []string{"--exclude", "public.kinds.*[type=index]"},
		dropped: `DROP CONSTRAINT "code_len"`, kept: `"kinds_code_key"`, after: "kinds_code_key,kinds_pkey",
	},
	{
		name:    "a unique constraint as a constraint",
		desired: constraintSelectorKeyed,
		flags:   []string{"--exclude", "public.kinds.kinds_code_key[type=constraint]"},
		dropped: `DROP CONSTRAINT "code_len"`, kept: `"kinds_code_key"`, after: "kinds_code_key,kinds_pkey",
	},
	{
		name:    "a unique constraint through its table",
		desired: constraintSelectorKeyed,
		flags:   []string{"--exclude", "kinds.kinds_code_key"},
		dropped: `DROP CONSTRAINT "code_len"`, kept: `"kinds_code_key"`, after: "kinds_code_key,kinds_pkey",
	},
	{
		name:    "a check constraint",
		desired: constraintSelectorKeyed,
		flags:   []string{"--exclude", "public.kinds.code_len[type=check]"},
		dropped: `DROP CONSTRAINT "kinds_code_key"`, kept: `"code_len"`, after: "code_len,kinds_pkey",
	},
	{
		name:    "every check of the table",
		desired: constraintSelectorKeyed,
		flags:   []string{"--exclude", "public.kinds.*[type=check]"},
		dropped: `DROP CONSTRAINT "kinds_code_key"`, kept: `"code_len"`, after: "code_len,kinds_pkey",
	},
}

// primaryKeySelectorRows name the primary key under the two types that name
// it, with the desired schema declaring no key, a table key and a column key.
// The community binary drops the key under both types.
var primaryKeySelectorRows = []struct {
	name, desired, dropped, kept, after string
	flags                               []string
}{
	{
		name:    "a primary key as an index",
		desired: constraintSelectorBare,
		flags:   []string{"--exclude", "public.kinds.kinds_pkey[type=index]"},
		dropped: `DROP CONSTRAINT "kinds_code_key"`, kept: `"kinds_pkey"`, after: "kinds_pkey",
	},
	{
		name:    "a primary key as a constraint",
		desired: constraintSelectorBare,
		flags:   []string{"--exclude", "public.kinds.kinds_pkey[type=constraint]"},
		dropped: `DROP CONSTRAINT "kinds_code_key"`, kept: `"kinds_pkey"`, after: "kinds_pkey",
	},
	{
		name:    "a declared table key as an index",
		desired: constraintSelectorKeyed,
		flags:   []string{"--exclude", "public.kinds.kinds_pkey[type=index]"},
		dropped: `DROP CONSTRAINT "kinds_code_key"`, kept: `"kinds_pkey"`, after: "kinds_pkey",
	},
	{
		name:    "a declared column key as a constraint",
		desired: constraintSelectorColumnKeyed,
		flags:   []string{"--exclude", "public.kinds.kinds_pkey[type=constraint]"},
		dropped: `DROP CONSTRAINT "kinds_code_key"`, kept: `"kinds_pkey"`, after: "kinds_pkey",
	},
}

func newConstraintSelectorFixture(c *qt.C, desired string) includeChildFixture {
	c.Helper()
	fixture := includeChildFixture{database: postgresScratchDevURL(c, "ptah_constraint_selector")}
	execPostgresFamilySQL(c, c.Context(), fixture.database, constraintSelectorLive)
	fixture.schema = filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(fixture.schema, []byte(desired), 0o600), qt.IsNil)
	return fixture
}

// TestExcludeKeepsANamedConstraintE2E applies through `ptah schema apply` and
// `ptah-compat schema apply` and reads the constraints back. A second
// comparison with the selector finds nothing left to do.
func TestExcludeKeepsANamedConstraintE2E(t *testing.T) {
	for _, row := range constraintSelectorRows {
		t.Run(row.name+"/ptah", func(t *testing.T) {
			c := qt.New(t)
			fixture := newConstraintSelectorFixture(c, row.desired)

			applied := fixture.apply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, row.dropped)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), row.kept)
			c.Assert(fixture.read(c, constraintSelectorReadBack), qt.Equals, row.after)
			converged := fixture.compare(c, row.flags...)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(comparisonBody(c, converged.Stdout), qt.Equals, includeNoDiffs)
		})
		t.Run(row.name+"/ptah-compat", func(t *testing.T) {
			c := qt.New(t)
			fixture := newConstraintSelectorFixture(c, row.desired)

			applied := fixture.compatApply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, row.dropped)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), row.kept)
			c.Assert(fixture.read(c, constraintSelectorReadBack), qt.Equals, row.after)
			converged := fixture.compatDiff(c, row.flags...)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(converged.Stdout, qt.Equals, includeSynced)
		})
		// The community binary drops the constraint for some of these
		// selectors. Strict mode refuses what that binary cannot do; it does
		// not drop what the selector names.
		t.Run(row.name+"/ptah-compat strict", func(t *testing.T) {
			c := qt.New(t)
			fixture := newConstraintSelectorFixture(c, row.desired)

			applied := fixture.strictCompatApply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, row.dropped)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), row.kept)
			c.Assert(fixture.read(c, constraintSelectorReadBack), qt.Equals, row.after)
		})
	}
}

// strictCompatApply is [includeChildFixture.compatApply] under
// PTAH_ATLAS_STRICT_COMPAT=1.
func (f includeChildFixture) strictCompatApply(c *qt.C, flags ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")},
		append([]string{"schema", "apply", "-u", f.database, "--to", f.schemaURL(),
			"--dev-url", postgresScratchDevURL(c, "ptah_include_dev"), "--auto-approve"}, flags...)...)
}

// TestExcludeKeepsANamedPrimaryKeyE2E applies through both shipped binaries
// and reads the key back. Afterwards no surface reports the excluded key:
// `schema compare` finds no difference on the key's column, `schema drift`
// finds no drift, and a second apply plans nothing.
func TestExcludeKeepsANamedPrimaryKeyE2E(t *testing.T) {
	for _, row := range primaryKeySelectorRows {
		t.Run(row.name+"/ptah", func(t *testing.T) {
			c := qt.New(t)
			fixture := newConstraintSelectorFixture(c, row.desired)

			applied := fixture.apply(c, row.flags...)
			compared := fixture.compare(c, row.flags...)
			drifted := fixture.drift(c, row.flags...)
			again := fixture.apply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, row.dropped)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), row.kept)
			c.Assert(fixture.read(c, constraintSelectorReadBack), qt.Equals, row.after)
			c.Assert(compared.ExitCode, qt.Equals, 0, historyComment(compared))
			c.Assert(comparisonBody(c, compared.Stdout), qt.Equals, includeNoDiffs)
			c.Assert(drifted.ExitCode, qt.Equals, 0, historyComment(drifted))
			c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
		t.Run(row.name+"/ptah-compat", func(t *testing.T) {
			c := qt.New(t)
			fixture := newConstraintSelectorFixture(c, row.desired)

			applied := fixture.compatApply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, row.dropped)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), row.kept)
			c.Assert(fixture.read(c, constraintSelectorReadBack), qt.Equals, row.after)
			converged := fixture.compatDiff(c, row.flags...)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(converged.Stdout, qt.Equals, includeSynced)
		})
		t.Run(row.name+"/ptah-compat strict", func(t *testing.T) {
			c := qt.New(t)
			fixture := newConstraintSelectorFixture(c, row.desired)

			applied := fixture.strictCompatApply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, row.dropped)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), row.kept)
			c.Assert(fixture.read(c, constraintSelectorReadBack), qt.Equals, row.after)
		})
	}
}
