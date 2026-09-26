//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// functionsWithOutArguments leave RETURNS out, and PostgreSQL records the type
// their OUT and INOUT arguments imply: the one argument's type, or record for
// two. Compared as an empty clause, the declaration differs from
// pg_get_function_result, and every plan drops these functions and creates them
// again (stokaro/ptah#3690).
const functionsWithOutArguments = `CREATE FUNCTION one_out(a integer, OUT b text) LANGUAGE sql AS $$SELECT 'x'$$;
CREATE FUNCTION two_out(a integer, OUT b integer, OUT c text) LANGUAGE sql AS $$SELECT 1, 'x'$$;
CREATE FUNCTION in_out(INOUT a int4) LANGUAGE sql AS $$SELECT 1$$;`

// TestSchemaApplyFindsFunctionsWithOutArgumentsSyncedE2E plans the schema file
// against the database the same SQL built: nothing is planned.
func TestSchemaApplyFindsFunctionsWithOutArgumentsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, functionsWithOutArguments, functionsWithOutArguments)
	target := databaseBuiltFrom(c, functionsWithOutArguments)

	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

	c.Assert(out, qt.Contains, "Schema is synced")
}

// TestSchemaDiffFromADatabaseFindsFunctionsWithOutArgumentsSyncedE2E is the
// same comparison through schema diff.
func TestSchemaDiffFromADatabaseFindsFunctionsWithOutArgumentsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, functionsWithOutArguments, functionsWithOutArguments)
	source := databaseBuiltFrom(c, functionsWithOutArguments)

	out := runPtahNative(c, "schema", "diff", "--from", source, "--to", "file://"+schema)

	c.Assert(out, qt.Contains, "Schemas are synced")
}

// TestMigrateDiffFindsFunctionsWithOutArgumentsSyncedE2E replays a directory
// whose one migration is the schema file and diffs: the directory is synced.
// The replay creates functions, which a dev database takes only when it is
// declared disposable, as this scratch database is.
func TestMigrateDiffFindsFunctionsWithOutArgumentsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	dir, schema := writeRewrittenMigrationProject(c, functionsWithOutArguments, functionsWithOutArguments)
	dev, _ := scratchReplayDatabase(c)

	out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
		"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
}

// TestSchemaApplyPlansAFunctionWhoseImpliedReturnDiffersE2E is the control: a
// function the database holds as SETOF text is not the one a declaration
// without RETURNS describes, and the return type change is planned.
func TestSchemaApplyPlansAFunctionWhoseImpliedReturnDiffersE2E(t *testing.T) {
	c := qt.New(t)
	built := `CREATE FUNCTION listed(a integer, OUT b text) RETURNS SETOF text LANGUAGE sql AS $$SELECT 'x'$$;`
	declared := `CREATE FUNCTION listed(a integer, OUT b text) LANGUAGE sql AS $$SELECT 'x'$$;`
	_, schema := writeRewrittenMigrationProject(c, built, declared)
	target := databaseBuiltFrom(c, built)

	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

	c.Assert(out, qt.Contains, "-- Modify function listed: returns")
}
