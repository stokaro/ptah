//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	// declaredSerialRead prints the default and the nullability of column c of
	// public.parent, and the sequence the column owns, as the server does.
	declaredSerialRead = "SELECT coalesce(column_default, '') || ' | ' || is_nullable || ' | ' || " +
		"coalesce(pg_get_serial_sequence('public.parent', 'c'), '') FROM information_schema.columns " +
		"WHERE table_schema = 'public' AND table_name = 'parent' AND column_name = 'c'"
	// declaredSerialNext takes the next value the column draws.
	declaredSerialNext = "INSERT INTO parent (id) VALUES (1000) RETURNING c"
	// declaredSerialSequences counts the sequences of public.
	declaredSerialSequences = "SELECT count(*) FROM pg_class WHERE relkind = 'S' AND relnamespace = 'public'::regnamespace"
)

// TestSchemaApplyDeclaredSerialE2E_HappyPath declares a serial type over a plain column.
// The comparison left a serial declaration alone: `bigint` against `bigserial` read as
// synced, and `int` against `serial` planned DROP NOT NULL on every run. Each row reads
// the column and the sequences back, takes the next value, and a second run finds nothing
// to do.
func TestSchemaApplyDeclaredSerialE2E_HappyPath(t *testing.T) {
	const want = "nextval('parent_c_seq'::regclass) | NO | public.parent_c_seq"
	tests := []struct {
		name, built, desired string
		sequences, next      int
	}{
		{
			name:      "bigint against bigserial",
			built:     "CREATE TABLE parent (id bigint PRIMARY KEY, c bigint NOT NULL);\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c bigserial);\n",
			sequences: 1, next: 1,
		},
		{
			name:      "int against serial",
			built:     "CREATE TABLE parent (id bigint PRIMARY KEY, c int);\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c serial);\n",
			sequences: 1, next: 1,
		},
		{
			name:      "smallint against smallserial",
			built:     "CREATE TABLE parent (id bigint PRIMARY KEY, c smallint);\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c smallserial);\n",
			sequences: 1, next: 1,
		},
		{
			name:      "int against bigserial",
			built:     "CREATE TABLE parent (id bigint PRIMARY KEY, c int);\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c bigserial);\n",
			sequences: 1, next: 1,
		},
		{
			name:      "rows the sequence moves past",
			built:     "CREATE TABLE parent (id bigint PRIMARY KEY, c bigint NOT NULL);\nINSERT INTO parent VALUES (1, 1), (2, 7);\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c bigserial);\n",
			sequences: 1, next: 8,
		},
		{
			name:      "rows below the first value",
			built:     "CREATE TABLE parent (id bigint PRIMARY KEY, c bigint NOT NULL);\nINSERT INTO parent VALUES (1, -7), (2, 0);\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c bigserial);\n",
			sequences: 1, next: 1,
		},
		{
			name:      "NULL rows the sequence fills",
			built:     "CREATE TABLE parent (id bigint PRIMARY KEY, c int);\nINSERT INTO parent VALUES (1, NULL), (2, NULL);\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c serial);\n",
			sequences: 1, next: 3,
		},
		{
			name:      "a plain default",
			built:     "CREATE TABLE parent (id bigint PRIMARY KEY, c bigint NOT NULL DEFAULT 5);\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c bigserial);\n",
			sequences: 1, next: 1,
		},
		{
			name:      "a sequence the column does not own, which leaves",
			built:     "CREATE SEQUENCE free;\nCREATE TABLE parent (id bigint PRIMARY KEY, c bigint NOT NULL DEFAULT nextval('free'));\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c bigserial);\n",
			sequences: 1, next: 1,
		},
		{
			name:      "a sequence the column does not own, which stays",
			built:     "CREATE SEQUENCE free;\nCREATE TABLE parent (id bigint PRIMARY KEY, c bigint NOT NULL DEFAULT nextval('free'));\n",
			desired:   "CREATE SEQUENCE free;\nCREATE TABLE parent (id bigint PRIMARY KEY, c bigserial);\n",
			sequences: 2, next: 1,
		},
		{
			name:      "a sequence under the name the column takes, which nothing declares",
			built:     "CREATE SEQUENCE parent_c_seq;\nCREATE TABLE parent (id bigint PRIMARY KEY, c bigint NOT NULL);\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c bigserial);\n",
			sequences: 1, next: 1,
		},
		{
			name:      "serial against serial, control",
			built:     "CREATE TABLE parent (id bigint PRIMARY KEY, c bigserial);\n",
			desired:   "CREATE TABLE parent (id bigint PRIMARY KEY, c bigserial);\n",
			sequences: 1, next: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, test.built)

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, declaredSerialRead), qt.Equals, want)
			c.Assert(readInt(c, target, declaredSerialSequences), qt.Equals, test.sequences)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
			c.Assert(readInt(c, target, declaredSerialNext), qt.Equals, test.next)
		})
	}
}

// TestSchemaApplyDeclaredSerialE2E_CockroachDB declares a serial type on CockroachDB, whose
// server builds every serial spelling as INT8 over unique_rowid(). `serial` compared as a
// 4-byte integer planned ALTER COLUMN ... TYPE integer on every run, on a column the
// declaration itself built.
func TestSchemaApplyDeclaredSerialE2E_CockroachDB(t *testing.T) {
	tests := []struct{ name, built, desired string }{
		{
			name:    "bigint against bigserial",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, c INT8);\n",
			desired: "CREATE TABLE parent (id INT8 PRIMARY KEY, c bigserial);\n",
		},
		{
			name:    "int against serial",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, c int);\n",
			desired: "CREATE TABLE parent (id INT8 PRIMARY KEY, c serial);\n",
		},
		{
			name:    "serial against serial, control",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, c serial);\n",
			desired: "CREATE TABLE parent (id INT8 PRIMARY KEY, c serial);\n",
		},
		{
			name:    "smallserial against smallserial, control",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, c smallserial);\n",
			desired: "CREATE TABLE parent (id INT8 PRIMARY KEY, c smallserial);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), test.built)

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, "SELECT column_default || ' | ' || is_nullable FROM information_schema.columns "+
				"WHERE table_schema = 'public' AND table_name = 'parent' AND column_name = 'c'"), qt.Equals, "unique_rowid() | NO")
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestMigrationsDeclaredSerialE2E_TheRollbackRestoresThePlainColumn rolls the plan back:
// the column loses the default and the sequence leaves after it.
func TestMigrationsDeclaredSerialE2E_TheRollbackRestoresThePlainColumn(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, "CREATE TABLE parent (id bigint PRIMARY KEY, c int NOT NULL);\n")
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE parent (id bigint PRIMARY KEY, c serial);\n"), 0o600), qt.IsNil)

	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "serial")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(readInt(c, target, declaredSerialSequences), qt.Equals, 1)

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(columnKeyString(c, target, declaredSerialRead), qt.Equals, " | NO | ")
	c.Assert(readInt(c, target, declaredSerialSequences), qt.Equals, 0)
}

// TestMigrateDiffDeclaredSerialE2E writes the plan for `int` against `serial` through the
// compatibility surface. It wrote DROP NOT NULL and no sequence. Strict mode leaves out
// what the community binary does not write: the NULL fill and the statement that moves
// the sequence past the rows.
func TestMigrateDiffDeclaredSerialE2E(t *testing.T) {
	const creates = `CREATE SEQUENCE "parent_c_seq" AS integer`
	tests := []struct {
		name, strict string
		wantSetval   bool
	}{
		{name: "the default surface", strict: "0", wantSetval: true},
		{name: "strict mode", strict: "1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_ATLAS_STRICT_COMPAT", test.strict)
			dir, schema := writeRewrittenMigrationProject(c, "CREATE TABLE parent (id bigint PRIMARY KEY, c int);\n",
				"CREATE TABLE parent (id bigint PRIMARY KEY, c serial);\n")

			plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", postgresUniqueEngine.emptyDB(c), "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			c.Assert(plan, qt.Contains, creates)
			c.Assert(plan, qt.Contains, `ALTER SEQUENCE "parent_c_seq" OWNED BY "parent"."c"`)
			c.Assert(plan, qt.Not(qt.Contains), "DROP NOT NULL")
			c.Assert(strings.Contains(plan, "setval"), qt.Equals, test.wantSetval)
		})
	}
}
