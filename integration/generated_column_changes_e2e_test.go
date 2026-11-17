//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// generatedColumnChange is a stored generated column whose expression changes
// beside its NOT NULL, and what PostgreSQL holds for the column once the plan
// is applied.
type generatedColumnChange struct {
	name    string
	initial string
	desired string
	// want is the column's NOT NULL, its constraint's name and its expression,
	// as the catalog reads them back.
	want string
}

var generatedColumnChanges = []generatedColumnChange{
	{
		name:    "the column gains NOT NULL",
		initial: "CREATE TABLE tasks (id int PRIMARY KEY, title text, code text GENERATED ALWAYS AS (upper(title)) STORED);\n",
		desired: "CREATE TABLE tasks (id int PRIMARY KEY, title text, code text NOT NULL GENERATED ALWAYS AS (lower(title)) STORED);\n",
		want:    "true:tasks_code_not_null:lower(title)",
	},
	{
		name:    "the column's NOT NULL takes another name",
		initial: "CREATE TABLE tasks (id int PRIMARY KEY, title text, code text CONSTRAINT old_nn NOT NULL GENERATED ALWAYS AS (upper(title)) STORED);\n",
		desired: "CREATE TABLE tasks (id int PRIMARY KEY, title text, code text CONSTRAINT code_nn NOT NULL GENERATED ALWAYS AS (lower(title)) STORED);\n",
		want:    "true:code_nn:lower(title)",
	},
}

// heldGeneratedColumn reads the NOT NULL, the NOT NULL constraint's name and
// the expression of tasks.code. PostgreSQL 18 records a NOT NULL as a
// constraint of its own, so the name reads back from pg_constraint.
const heldGeneratedColumn = `SELECT a.attnotnull::text || ':' ||
  coalesce((SELECT c.conname FROM pg_constraint AS c WHERE c.conrelid = a.attrelid AND c.contype = 'n' AND c.conkey = ARRAY[a.attnum]), '-') || ':' ||
  pg_get_expr(d.adbin, d.adrelid)
FROM pg_attribute AS a
JOIN pg_attrdef AS d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = 'tasks'::regclass AND a.attname = 'code'`

// TestSchemaApplyChangesAGeneratedColumnsNotNullBesideItsExpressionE2E applies
// each change with the native `schema apply` on PostgreSQL, reads the column
// back from the catalog, and plans once more: the plan is empty. A plan that
// changes the expression alone leaves the NOT NULL to the next run.
func TestSchemaApplyChangesAGeneratedColumnsNotNullBesideItsExpressionE2E(t *testing.T) {
	for _, test := range generatedColumnChanges {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			created := runPtahIn(c, "schema", "apply", "--db-url", target,
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", test.initial), "--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
			file := writeDeclaredSchemaFile(c, "schema.sql", test.desired)

			changed := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(changed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", changed.Stdout, changed.Stderr))
			c.Assert(heldColumns(c, target, heldGeneratedColumn), qt.Equals, test.want)
			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")
			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestMigrateDiffChangesAGeneratedColumnsNotNullBesideItsExpressionE2E runs the
// compatibility `migrate diff` and `migrate apply` for each change, reads the
// column back, and expects the next `migrate diff` to find the directory
// synced. The community binary refuses an expression change, so there is no
// file of its to match.
func TestMigrateDiffChangesAGeneratedColumnsNotNullBesideItsExpressionE2E(t *testing.T) {
	for _, test := range generatedColumnChanges {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			dir := filepath.Join(work, "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "20200101000000_init.sql"), []byte(test.initial), 0o600), qt.IsNil)
			schema := filepath.Join(work, "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.desired), 0o600), qt.IsNil)
			dirURL, schemaURL := "file://"+filepath.ToSlash(dir), "file://"+filepath.ToSlash(schema)
			hashed, err := runCompatVerb("migrate", "hash", "--dir", dirURL)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", hashed))
			dev, _ := scratchReplayDatabase(c)
			target, _ := scratchReplayDatabase(c)

			diffed, err := runCompatVerb("migrate", "diff", "change", "--dir", dirURL, "--to", schemaURL, "--dev-url", dev)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", diffed))
			applied, err := runCompatVerb("migrate", "apply", "--dir", dirURL, "--url", target)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			again, err := runCompatVerb("migrate", "diff", "--dir", dirURL, "--to", schemaURL, "--dev-url", dev, "--dry-run")

			c.Assert(heldColumns(c, target, heldGeneratedColumn), qt.Equals, test.want)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
			c.Assert(again, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}

// TestSchemaApplyGainsNotNullOnAGeneratedColumnItCannotRecomputeE2E applies the
// same change on CockroachDB and YugabyteDB, which have no SET EXPRESSION. The
// plan warns that the expression needs a manual migration and still plans the
// NOT NULL, which both engines accept on a stored generated column: the column
// reads back NOT NULL under the expression the database held. A plan that stops
// at the warning leaves the column nullable on every run. What the next run
// reports about the expression alone is stokaro/ptah#6160.
func TestSchemaApplyGainsNotNullOnAGeneratedColumnItCannotRecomputeE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "cockroachdb", engine: dbtarget.CockroachDB},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := declaredSchemaTarget(c, test.engine)
			created := runPtahIn(c, "schema", "apply", "--db-url", target,
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", generatedColumnChanges[0].initial), "--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))

			changed := runPtahIn(c, "schema", "apply", "--db-url", target,
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", generatedColumnChanges[0].desired), "--auto-approve")

			c.Assert(changed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", changed.Stdout, changed.Stderr))
			c.Assert(changed.Stdout, qt.Contains, "manual migration required")
			c.Assert(changed.Stdout, qt.Contains, `ALTER TABLE "public"."tasks" ALTER COLUMN "code" SET NOT NULL;`)
			c.Assert(heldColumns(c, target, heldGeneratedColumnNullability), qt.Equals, "NO:upper(title)")
		})
	}
}

// TestSchemaApplyRefusesANamedNotNullOnAGeneratedColumnItCannotRecomputeE2E_FailurePath
// declares the named NOT NULL the rename row starts from, on CockroachDB and
// YugabyteDB. Neither keeps a NOT NULL constraint's name, so the declaration is
// refused before any statement reaches the server, and a rename of that name is
// never planned there.
func TestSchemaApplyRefusesANamedNotNullOnAGeneratedColumnItCannotRecomputeE2E_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "cockroachdb", engine: dbtarget.CockroachDB},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := declaredSchemaTarget(c, test.engine)

			refused := runPtahIn(c, "schema", "apply", "--db-url", target,
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", generatedColumnChanges[1].initial), "--auto-approve")

			c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
			c.Assert(refused.Stderr, qt.Contains, "this target does not keep a NOT NULL constraint name (old_nn)")
		})
	}
}

// heldGeneratedColumnNullability reads whether tasks.code admits NULL and its
// expression, from the information schema both engines carry.
const heldGeneratedColumnNullability = `SELECT is_nullable || ':' || generation_expression
FROM information_schema.columns WHERE table_name = 'tasks' AND column_name = 'code'`
