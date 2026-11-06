//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	// declaredKeyRead prints the primary key of public.parent and the foreign keys
	// that reference the table as the server does.
	declaredKeyRead = "SELECT coalesce((SELECT string_agg(conname, ',') FROM pg_constraint " +
		"WHERE conrelid = 'public.parent'::regclass AND contype = 'p'), '') || ' | ' || " +
		"coalesce((SELECT string_agg(conname, ',') FROM pg_constraint " +
		"WHERE confrelid = 'public.parent'::regclass AND contype = 'f'), '')"
	// declaredKeyIdentity prints the catalog identity of the key's index and of a
	// foreign key over it. A rebuild changes the first, and a drop and an add around
	// the key change the second.
	declaredKeyIdentity = "SELECT coalesce((SELECT conindid::text FROM pg_constraint " +
		"WHERE conrelid = 'public.parent'::regclass AND contype = 'p'), '') || ' | ' || " +
		"coalesce((SELECT oid::text FROM pg_constraint " +
		"WHERE confrelid = 'public.parent'::regclass AND contype = 'f'), '')"
	// declaredKeyChild is a table whose foreign key depends on the key.
	declaredKeyChild = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, " +
		"CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parent (id));\n"
)

// TestSchemaApplyDeclaredKeyNameE2E_HappyPath declares a primary key name over the
// columns the live key holds. A SQL schema read that as synced and a Go annotation
// rebuilt the key. Each row reads the name back, the catalog identity of the key's index
// and of a foreign key over it stay, and a second run finds nothing to do.
func TestSchemaApplyDeclaredKeyNameE2E_HappyPath(t *testing.T) {
	tests := []struct{ name, built, desired, want string }{
		{
			name:    "a table's key",
			built:   "CREATE TABLE parent (id bigint PRIMARY KEY);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_custom PRIMARY KEY (id));\n",
			want:    "parent_custom | ",
		},
		{
			name:    "a column's key",
			built:   "CREATE TABLE parent (id bigint PRIMARY KEY);\n",
			desired: "CREATE TABLE parent (id bigint CONSTRAINT parent_custom PRIMARY KEY);\n",
			want:    "parent_custom | ",
		},
		{
			name:    "the name the server would give it",
			built:   "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_old PRIMARY KEY (id));\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (id));\n",
			want:    "parent_pkey | ",
		},
		{
			name:    "a foreign key over the key",
			built:   "CREATE TABLE parent (id bigint PRIMARY KEY);\n" + declaredKeyChild,
			desired: "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_custom PRIMARY KEY (id));\n" + declaredKeyChild,
			want:    "parent_custom | child_fk",
		},
		{
			name:    "no name declared, control",
			built:   "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_old PRIMARY KEY (id));\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, PRIMARY KEY (id));\n",
			want:    "parent_old | ",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, test.built)
			before := columnKeyString(c, target, declaredKeyIdentity)

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, declaredKeyRead), qt.Equals, test.want)
			c.Assert(columnKeyString(c, target, declaredKeyIdentity), qt.Equals, before)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyDeclaredKeyNameE2E_GoAnnotation takes the name from a Go annotation,
// over a foreign key the rebuild would have to drop.
func TestSchemaApplyDeclaredKeyNameE2E_GoAnnotation(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, "CREATE TABLE parent (id bigint PRIMARY KEY);\n"+declaredKeyChild)
	before := columnKeyString(c, target, declaredKeyIdentity)
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "entities.go"), []byte(`package entities

//ptah:schema:table name="parent"
type Parent struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true"
	ID int64
	//ptah:schema:constraint name="parent_custom" type="PRIMARY KEY" columns="id"
	_ int
}

//ptah:schema:table name="child"
type Child struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="pid" type="BIGINT" foreign="parent(id)" foreign_key_name="child_fk"
	PID int64
}
`), 0o600), qt.IsNil)

	run := runPtahIn(c, "schema", "apply", "--db-url", target, "--root-dir", root, "--auto-approve")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(columnKeyString(c, target, declaredKeyRead), qt.Equals, "parent_custom | child_fk")
	c.Assert(columnKeyString(c, target, declaredKeyIdentity), qt.Equals, before)
	again := runPtahIn(c, "schema", "apply", "--db-url", target, "--root-dir", root, "--auto-approve")
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")
}

// TestSchemaApplyDeclaredKeyNameE2E_CockroachDB renames the key on CockroachDB, whose
// server refuses a key's drop in a statement of its own.
func TestSchemaApplyDeclaredKeyNameE2E_CockroachDB(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), "CREATE TABLE parent (id INT8 PRIMARY KEY);\n"+declaredKeyChild)
	desired := "CREATE TABLE parent (id INT8 NOT NULL, CONSTRAINT parent_custom PRIMARY KEY (id));\n" + declaredKeyChild

	run := keyedApply(c, target, desired)

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(columnKeyString(c, target, declaredKeyRead), qt.Equals, "parent_custom | child_fk")
	again := keyedApply(c, target, desired)
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")
}

// TestSchemaApplyDeclaredKeyNameE2E_TheNameIsHeld declares a name that an object the
// same plan removes holds. That object leaves ahead of the rename, which would collide
// with it.
func TestSchemaApplyDeclaredKeyNameE2E_TheNameIsHeld(t *testing.T) {
	const desired = "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_custom PRIMARY KEY (id));\n"
	tests := []struct{ name, built, also string }{
		{
			name:  "a UNIQUE",
			built: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_custom UNIQUE (code));\n",
		},
		{
			name:  "an index of the table",
			built: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\nCREATE INDEX parent_custom ON parent (code);\n",
		},
		{
			name:  "an index of another table",
			built: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\nCREATE TABLE other (id bigint);\nCREATE INDEX parent_custom ON other (id);\n",
			also:  "CREATE TABLE other (id bigint);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, test.built)

			run := keyedApply(c, target, desired+test.also)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, declaredKeyRead), qt.Equals, "parent_custom | ")
			again := keyedApply(c, target, desired+test.also)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestMigrationsDeclaredKeyNameE2E_TheRollbackRenamesTheKeyBack rolls the rename back
// over a foreign key, which a rebuild would stop at.
func TestMigrationsDeclaredKeyNameE2E_TheRollbackRenamesTheKeyBack(t *testing.T) {
	c := qt.New(t)
	built := "CREATE TABLE parent (id bigint PRIMARY KEY);\n" + declaredKeyChild
	target := databaseBuiltFrom(c, built)
	before := columnKeyString(c, target, declaredKeyIdentity)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_custom PRIMARY KEY (id));\n"+declaredKeyChild), 0o600), qt.IsNil)

	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "key")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(columnKeyString(c, target, declaredKeyRead), qt.Equals, "parent_custom | child_fk")

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(columnKeyString(c, target, declaredKeyRead), qt.Equals, "parent_pkey | child_fk")
	c.Assert(columnKeyString(c, target, declaredKeyIdentity), qt.Equals, before)
}
