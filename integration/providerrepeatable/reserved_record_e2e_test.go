//go:build integration

package providerrepeatable_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// These tests read a fixture database whose record of repeatable migrations
// an up run created. The fixture reports the record as reserved storage of
// purpose REPEATABLE, unless FIXTURE_REPEATABLE_RESERVATION=off, when it
// reports the record as a table of the schema.

// recordDeclaration declares the tables the migrations created and nothing
// else.
const recordDeclaration = "tables:\n" +
	"  users:\n    columns:\n" +
	"      id: {type: INTEGER, primary: true, not_null: true}\n" +
	"      name: {type: TEXT, not_null: true}\n" +
	"  run_v1:\n    columns:\n" +
	"      id: {type: INTEGER, primary: true, not_null: true}\n"

// extraDeclaration declares one table more, so a plan has a change to apply.
const extraDeclaration = recordDeclaration +
	"  extra:\n    columns:\n" +
	"      id: {type: INTEGER, primary: true, not_null: true}\n"

// recordNameDeclaration declares a table of the record's name besides.
const recordNameDeclaration = recordDeclaration +
	"  schema_migrations_repeatable:\n    columns:\n" +
	"      identity: {type: TEXT, primary: true, not_null: true}\n"

// recordRefusal is the refusal of a declaration that takes the record's name.
const recordRefusal = `is reserved for the record of repeatable migrations by provider "example.org/fixture"`

// appliedRecordTarget is a fixture target whose up run applied the directory,
// started with environment, and a schema file holding declaration.
func appliedRecordTarget(c *qt.C, declaration string, environment ...string) (*recordTarget, string) {
	c.Helper()
	r := newRecordTarget(c, "repeatable-record", environment...)
	result, _ := r.up(c)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
	schema := filepath.Join(c.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte(declaration), 0o600), qt.IsNil)
	return r, schema
}

// plan runs `ptah schema plan --json` with the schema file, writing the plan
// beside it.
func (r *recordTarget) plan(c *qt.C, schema string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, "schema", "plan", "--provider-config", r.manifest, "--db-url", r.address,
		"--schema-file", schema, "--output", filepath.Join(filepath.Dir(schema), "change.plan.json"), "--json")
}

// apply runs `ptah schema apply` over the plan the last plan wrote.
func (r *recordTarget) apply(c *qt.C, schema string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, "schema", "apply", "--provider-config", r.manifest, "--db-url", r.address,
		"--plan", filepath.Join(filepath.Dir(schema), "change.plan.json"), "--auto-approve", "--json")
}

// A reserved record is the provider's storage: a declaration that leaves it
// out plans the table it adds and drops nothing, and after the apply the
// record keeps its row.
func TestProviderReservedRepeatableRecord_HappyPath(t *testing.T) {
	c := qt.New(t)
	r, schema := appliedRecordTarget(c, extraDeclaration)
	before := r.record(c)

	plan := r.plan(c, schema)
	apply := r.apply(c, schema)

	c.Assert(plan.ExitCode, qt.Equals, 0, qt.Commentf("%s", plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, `CREATE TABLE \"extra\"`)
	c.Assert(plan.Stdout, qt.Not(qt.Contains), `DROP`)
	c.Assert(apply.ExitCode, qt.Equals, 0, qt.Commentf("%s", apply.Stderr))
	c.Assert(r.query(c, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'extra'`), qt.DeepEquals, []string{"extra"})
	c.Assert(r.record(c), qt.DeepEquals, before)
	c.Assert(before, qt.HasLen, 1)
}

// A declaration that takes the record's name is refused before anything
// runs, and the record keeps its row.
func TestProviderReservedRepeatableRecord_FailurePath(t *testing.T) {
	c := qt.New(t)
	r, schema := appliedRecordTarget(c, recordNameDeclaration)
	before := r.record(c)

	plan := r.plan(c, schema)

	c.Assert(plan.ExitCode, qt.Equals, 2, qt.Commentf("%s", plan.Stdout))
	c.Assert(plan.Stderr, qt.Contains, "table main.schema_migrations_repeatable "+recordRefusal)
	c.Assert(r.record(c), qt.DeepEquals, before)
}

// Without the reservation the provider reports the record as a table of the
// schema, and a declaration that leaves it out plans to drop it. The
// reservation is what keeps it.
func TestProviderUnreservedRepeatableRecord(t *testing.T) {
	c := qt.New(t)
	r, schema := appliedRecordTarget(c, recordDeclaration, "FIXTURE_REPEATABLE_RESERVATION=off")

	plan := r.plan(c, schema)

	c.Assert(plan.ExitCode, qt.Equals, 0, qt.Commentf("%s", plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, `DROP TABLE \"schema_migrations_repeatable\";`)
}
