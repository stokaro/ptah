//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

const (
	// referencedKeyChild is a foreign key over parent's columns in one order.
	referencedKeyChild = "CREATE TABLE child (id int PRIMARY KEY, pa int, pb int, CONSTRAINT child_fk FOREIGN KEY (pa, pb) REFERENCES parent (a, b));\n"
	// referencedKeyReversed holds the key in the other order.
	referencedKeyReversed = "CREATE TABLE parent (a int NOT NULL, b int NOT NULL, PRIMARY KEY (b, a));\n"
	// referencedKeyRead prints the key of public.parent as the server does.
	referencedKeyRead = "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'public.parent'::regclass AND contype = 'p'"
)

// TestSchemaApplyReferencedKeyOrderE2E_PostgreSQL holds a primary key in another order
// than the foreign key over it. PostgreSQL takes that, and the plan refused it before
// the server. Each row reads the key back, a row through the foreign key proves the
// columns pair by name, and a second run finds nothing to do.
func TestSchemaApplyReferencedKeyOrderE2E_PostgreSQL(t *testing.T) {
	tests := []struct{ name, built string }{
		{name: "the plan creates the tables", built: "CREATE TABLE keep (id int PRIMARY KEY);\n"},
		{
			name: "the plan moves the key under the foreign key",
			built: "CREATE TABLE parent (a int NOT NULL, b int NOT NULL, PRIMARY KEY (a, b));\n" + referencedKeyChild +
				"INSERT INTO parent VALUES (1, 2);\nINSERT INTO child VALUES (1, 1, 2);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, test.built)
			desired := referencedKeyReversed + referencedKeyChild

			run := keyedApply(c, target, desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, referencedKeyRead), qt.Equals, "PRIMARY KEY (b, a)")
			again := keyedApply(c, target, desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyReferencedKeyOrderE2E_CockroachDB takes the same order on creation, and
// when the plan moves the key under the foreign key. The move stands in one statement,
// the server keeps the foreign key through it, and the rows stay.
func TestSchemaApplyReferencedKeyOrderE2E_CockroachDB(t *testing.T) {
	const foreignKeys = "SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.child'::regclass AND contype = 'f'"
	tests := []struct {
		name, built string
		wantRows    int
	}{
		{name: "the plan creates the tables", built: "CREATE TABLE keep (id INT8 PRIMARY KEY);\n"},
		{
			name: "the plan moves the key under the foreign key",
			built: "CREATE TABLE parent (a INT8 NOT NULL, b INT8 NOT NULL, PRIMARY KEY (a, b));\n" + referencedKeyChild +
				"INSERT INTO parent VALUES (1, 2);\nINSERT INTO child VALUES (1, 1, 2);\n",
			wantRows: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), test.built)
			desired := referencedKeyReversed + referencedKeyChild

			run := keyedApply(c, target, desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, referencedKeyRead), qt.Equals, "PRIMARY KEY (b ASC, a ASC)")
			c.Assert(readInt(c, target, foreignKeys), qt.Equals, 1)
			c.Assert(readInt(c, target, "SELECT count(*) FROM child"), qt.Equals, test.wantRows)
			again := keyedApply(c, target, desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyReferencedKeyOrderE2E_FailurePath is the control: MySQL answers this
// order with Error 6125 and MariaDB with errno 150, so the plan refuses it before the
// server, and the database reads back with no table.
func TestSchemaApplyReferencedKeyOrderE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		want   string
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, want: `requires referenced columns a, b on table "parent" to be declared unique`},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, want: `requires referenced columns a, b on table "parent" to be`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, target := newMySQLFamilyScratch(c, test.engine).builtFrom(c, "CREATE TABLE keep (id int PRIMARY KEY);\n")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE keep (id int PRIMARY KEY);\n"+referencedKeyReversed+referencedKeyChild), 0o600), qt.IsNil)

			run := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, test.want)
			c.Assert(readInt(c, target, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('parent', 'child')"), qt.Equals, 0)
		})
	}
}
