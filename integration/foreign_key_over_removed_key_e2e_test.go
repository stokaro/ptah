//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

const (
	// keyMoveChild is a child table whose foreign key the declaration keeps as it is.
	keyMoveChild = "CREATE TABLE child (id bigint PRIMARY KEY, p bigint, CONSTRAINT child_fk FOREIGN KEY (p) REFERENCES parent (id));\n"
	// keyMoveChildOnCode is the same foreign key over parent.code.
	keyMoveChildOnCode = "CREATE TABLE child (id bigint PRIMARY KEY, p bigint, CONSTRAINT child_fk FOREIGN KEY (p) REFERENCES parent (code));\n"
	// keyMoveRows is what every row below keeps through the plan.
	keyMoveRows = "INSERT INTO parent (id, code) VALUES (1, 1);\nINSERT INTO child VALUES (1, 1);\n"
	// keyMoveMoved moves the primary key of parent from id to code and keeps a key
	// over id for the foreign key.
	keyMoveMoved = "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code), CONSTRAINT parent_id_key UNIQUE (id));\n"
	// keyMoveHeld is parent with its primary key on id.
	keyMoveHeld = "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\n"
)

// keyMoveDefinitions reads every constraint of the public schema but NOT NULL and
// those of the revision tables, as the server prints it, by table and name.
func keyMoveDefinitions(c *qt.C, target string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var definitions string
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT string_agg(conname || ': ' || pg_get_constraintdef(oid), '|' ORDER BY conrelid::regclass::text, conname)
FROM pg_constraint WHERE connamespace = 'public'::regnamespace AND contype <> 'n' AND conrelid::regclass::text NOT LIKE 'schema_migrations%'`).Scan(&definitions), qt.IsNil)
	return strings.Split(definitions, "|")
}

// keyMoveApply runs the native binary's `schema apply` of desired over the database
// at target.
func keyMoveApply(c *qt.C, target, desired string, schemas ...string) clirun.Result {
	c.Helper()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	return runPtahIn(c, append([]string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve"}, schemas...)...)
}

// TestSchemaApplyForeignKeyOverRemovedKeyE2E_HappyPath removes a key a foreign key
// the declaration keeps depends on. The server refused the drop with `cannot drop
// constraint parent_pkey on table parent because other objects depend on it`, in
// both statement styles. Each row reads the constraints back as the server prints
// them, a row survives the rebuild, the server still enforces the foreign key, and a
// second run finding nothing proves the plan ended in the declared state.
func TestSchemaApplyForeignKeyOverRemovedKeyE2E_HappyPath(t *testing.T) {
	childKey := "child_pkey: PRIMARY KEY (id)"
	tests := []struct {
		name, built, desired string
		want                 []string
	}{
		{
			name:    "a primary key moved to another column",
			built:   keyMoveHeld + keyMoveChild + keyMoveRows,
			desired: keyMoveMoved + keyMoveChild,
			want:    []string{"child_fk: FOREIGN KEY (p) REFERENCES parent(id)", childKey, "parent_id_key: UNIQUE (id)", "parent_pkey: PRIMARY KEY (code)"},
		},
		{
			name:    "a primary key dropped with a UNIQUE over its column declared",
			built:   keyMoveHeld + keyMoveChild + keyMoveRows,
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_id_key UNIQUE (id));\n" + keyMoveChild,
			want:    []string{"child_fk: FOREIGN KEY (p) REFERENCES parent(id)", childKey, "parent_id_key: UNIQUE (id)"},
		},
		{
			name:    "a UNIQUE over the same column survives beside the moved primary key",
			built:   keyMoveHeld + keyMoveChild + keyMoveRows + "ALTER TABLE parent ADD CONSTRAINT parent_id_key UNIQUE (id);\n",
			desired: keyMoveMoved + keyMoveChild,
			want:    []string{"child_fk: FOREIGN KEY (p) REFERENCES parent(id)", childKey, "parent_id_key: UNIQUE (id)", "parent_pkey: PRIMARY KEY (code)"},
		},
		{
			name: "a UNIQUE replaced under another name",
			built: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code_old UNIQUE (code));\n" +
				keyMoveChildOnCode + keyMoveRows,
			desired: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code_new UNIQUE (code));\n" + keyMoveChildOnCode,
			want:    []string{"child_fk: FOREIGN KEY (p) REFERENCES parent(code)", childKey, "parent_code_new: UNIQUE (code)", "parent_pkey: PRIMARY KEY (id)"},
		},
		{
			name: "a UNIQUE redefined under its name",
			built: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, x int, CONSTRAINT parent_code_key UNIQUE (code));\n" +
				keyMoveChildOnCode + keyMoveRows,
			desired: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, x int, CONSTRAINT parent_code_key UNIQUE (code) INCLUDE (x));\n" + keyMoveChildOnCode,
			want:    []string{"child_fk: FOREIGN KEY (p) REFERENCES parent(code)", childKey, "parent_code_key: UNIQUE (code) INCLUDE (x)", "parent_pkey: PRIMARY KEY (id)"},
		},
		{
			name: "a unique index replaced by a UNIQUE constraint",
			built: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\nCREATE UNIQUE INDEX parent_code_ui ON parent (code);\n" +
				keyMoveChildOnCode + keyMoveRows,
			desired: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code_key UNIQUE (code));\n" + keyMoveChildOnCode,
			want:    []string{"child_fk: FOREIGN KEY (p) REFERENCES parent(code)", childKey, "parent_code_key: UNIQUE (code)", "parent_pkey: PRIMARY KEY (id)"},
		},
		{
			name: "a UNIQUE constraint replaced by a unique index",
			built: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code_key UNIQUE (code));\n" +
				keyMoveChildOnCode + keyMoveRows,
			desired: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\nCREATE UNIQUE INDEX parent_code_ui ON parent (code);\n" + keyMoveChildOnCode,
			want:    []string{"child_fk: FOREIGN KEY (p) REFERENCES parent(code)", childKey, "parent_pkey: PRIMARY KEY (id)"},
		},
		{
			name: "the foreign key's actions, deferral and comment come back",
			built: keyMoveHeld +
				"CREATE TABLE child (id bigint PRIMARY KEY, p bigint, CONSTRAINT child_fk FOREIGN KEY (p) REFERENCES parent (id) ON DELETE SET NULL (p) DEFERRABLE INITIALLY DEFERRED);\n" +
				"COMMENT ON CONSTRAINT child_fk ON child IS 'keep me';\n" + keyMoveRows,
			desired: keyMoveMoved +
				"CREATE TABLE child (id bigint PRIMARY KEY, p bigint, CONSTRAINT child_fk FOREIGN KEY (p) REFERENCES parent (id) ON DELETE SET NULL (p) DEFERRABLE INITIALLY DEFERRED);\n" +
				"COMMENT ON CONSTRAINT child_fk ON child IS 'keep me';\n",
			want: []string{"child_fk: FOREIGN KEY (p) REFERENCES parent(id) ON DELETE SET NULL (p) DEFERRABLE INITIALLY DEFERRED", childKey, "parent_id_key: UNIQUE (id)", "parent_pkey: PRIMARY KEY (code)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, test.built, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(keyMoveDefinitions(c, target), qt.DeepEquals, test.want)
			c.Assert(readInt(c, target, "SELECT p FROM child WHERE id = 1"), qt.Equals, 1)
			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), "SET CONSTRAINTS ALL IMMEDIATE; INSERT INTO child VALUES (2, 99)")
			c.Assert(err, qt.ErrorMatches, `(?s).*violates foreign key constraint "child_fk".*`)
			again := keyMoveApply(c, target, test.desired, "--schemas", "public")
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyForeignKeyOverRemovedKeyE2E_AKeyInAnotherSchema moves a primary key
// of a table a declared schema holds, under a foreign key from another declared schema.
func TestSchemaApplyForeignKeyOverRemovedKeyE2E_AKeyInAnotherSchema(t *testing.T) {
	c := qt.New(t)
	const child = "CREATE TABLE child (id bigint PRIMARY KEY, p bigint, CONSTRAINT child_fk FOREIGN KEY (p) REFERENCES app.parent (id));\n"
	target := databaseBuiltFrom(c, "CREATE SCHEMA app;\nCREATE TABLE app.parent (id bigint PRIMARY KEY, code bigint NOT NULL);\n"+child)
	desired := "CREATE SCHEMA app;\nCREATE TABLE app.parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code), CONSTRAINT parent_id_key UNIQUE (id));\n" + child

	run := keyMoveApply(c, target, desired, "--schemas", "public,app")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(keyMoveDefinitions(c, target), qt.DeepEquals, []string{"child_fk: FOREIGN KEY (p) REFERENCES app.parent(id)", "child_pkey: PRIMARY KEY (id)"})
	c.Assert(readInt(c, target, "SELECT count(*) FROM pg_constraint WHERE conname = 'parent_pkey' AND pg_get_constraintdef(oid) = 'PRIMARY KEY (code)'"), qt.Equals, 1)
	again := keyMoveApply(c, target, desired, "--schemas", "public,app")
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")
}

// TestSchemaApplyForeignKeyOverRemovedKeyE2E_ALeavesAKeyThatStays is the control for
// the server's own answer about which index a foreign key depends on: the foreign key
// was built on the UNIQUE, the primary key over the same column moves, and the plan
// neither drops nor validates the foreign key.
func TestSchemaApplyForeignKeyOverRemovedKeyE2E_ALeavesAKeyThatStays(t *testing.T) {
	c := qt.New(t)

	target, run := runExactApply(c,
		"CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_id_key UNIQUE (id));\n"+
			"ALTER TABLE parent ADD CONSTRAINT parent_pkey PRIMARY KEY (id);\n"+keyMoveChild,
		keyMoveMoved+keyMoveChild)

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Not(qt.Contains), "child_fk")
	c.Assert(keyMoveDefinitions(c, target), qt.DeepEquals, []string{"child_fk: FOREIGN KEY (p) REFERENCES parent(id)", "child_pkey: PRIMARY KEY (id)", "parent_id_key: UNIQUE (id)", "parent_pkey: PRIMARY KEY (code)"})
}

// TestSchemaApplyForeignKeyOverRemovedKeyE2E_FailurePath holds what stops the plan.
// A declaration that leaves the foreign key no key is refused before the server
// changes. A foreign key from a schema the run leaves out, and rows the rebuilt foreign
// key rejects, stop at the server, and the plan's one transaction takes every statement
// back: the primary key and the foreign key read back as they were.
func TestSchemaApplyForeignKeyOverRemovedKeyE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name, built, desired, refusal string
	}{
		{
			name:    "the declaration leaves no key under the foreign key",
			built:   keyMoveHeld + keyMoveChild,
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code));\n" + keyMoveChild,
			refusal: `requires referenced columns id on table "parent" to be declared unique`,
		},
		{
			name:    "a foreign key in a schema the run leaves out",
			built:   keyMoveHeld + keyMoveChild + "CREATE SCHEMA reporting;\nCREATE TABLE reporting.u (id bigint PRIMARY KEY, p bigint REFERENCES public.parent (id));\n",
			desired: keyMoveMoved + keyMoveChild,
			refusal: "cannot drop constraint parent_pkey on table parent because other objects depend on it",
		},
		{
			name: "a row the rebuilt foreign key rejects",
			built: keyMoveHeld + "CREATE TABLE child (id bigint PRIMARY KEY, p bigint);\nINSERT INTO child VALUES (1, 99);\n" +
				"ALTER TABLE child ADD CONSTRAINT child_fk FOREIGN KEY (p) REFERENCES parent (id) NOT VALID;\n",
			desired: keyMoveMoved + keyMoveChild,
			refusal: `violates foreign key constraint "child_fk"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, test.built, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, test.refusal)
			c.Assert(readInt(c, target, "SELECT count(*) FROM pg_constraint WHERE conname = 'parent_pkey' AND pg_get_constraintdef(oid) = 'PRIMARY KEY (id)'"), qt.Equals, 1)
			c.Assert(readInt(c, target, "SELECT count(*) FROM pg_constraint WHERE conname = 'child_fk'"), qt.Equals, 1)
		})
	}
}

// TestMigrationsGenerateForeignKeyOverRemovedKeyE2E_RunsBothWays runs the plan through a
// migration pair. The down file moves the primary key back under the same foreign key,
// and each direction reads back from the server.
func TestMigrationsGenerateForeignKeyOverRemovedKeyE2E_RunsBothWays(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, keyMoveHeld+keyMoveChild+keyMoveRows)
	before := keyMoveDefinitions(c, target)
	dir := exactGeneratedFile(c, target, keyMoveMoved+keyMoveChild)

	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(keyMoveDefinitions(c, target), qt.DeepEquals, []string{"child_fk: FOREIGN KEY (p) REFERENCES parent(id)", "child_pkey: PRIMARY KEY (id)", "parent_id_key: UNIQUE (id)", "parent_pkey: PRIMARY KEY (code)"})

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(keyMoveDefinitions(c, target), qt.DeepEquals, before)
	c.Assert(readInt(c, target, "SELECT p FROM child WHERE id = 1"), qt.Equals, 1)
}
