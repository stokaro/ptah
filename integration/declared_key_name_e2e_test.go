//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestSchemaApplyDeclaredKeyNameE2E_HappyPath names a primary key on a table the
// database holds. The server took `parent_pkey` and the next run reported the
// schema synced, so the declaration and the catalog disagreed for good. Each row
// reads the name back from the catalog, and a second run finds nothing to do.
func TestSchemaApplyDeclaredKeyNameE2E_HappyPath(t *testing.T) {
	tests := []struct{ name, built, desired, want string }{
		{
			name:    "a table with no key gains a key the table names",
			built:   "CREATE TABLE parent (id bigint NOT NULL);\nINSERT INTO parent VALUES (1);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (id));\n",
			want:    "parent_pk: PRIMARY KEY (id)",
		},
		{
			name:    "a table with no key gains a key the column names",
			built:   "CREATE TABLE parent (id bigint NOT NULL);\nINSERT INTO parent VALUES (1);\n",
			desired: "CREATE TABLE parent (id bigint CONSTRAINT parent_pk PRIMARY KEY);\n",
			want:    "parent_pk: PRIMARY KEY (id)",
		},
		{
			name:    "a quoted name keeps its case",
			built:   "CREATE TABLE parent (id bigint NOT NULL);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT \"Parent Key\" PRIMARY KEY (id));\n",
			want:    "Parent Key: PRIMARY KEY (id)",
		},
		{
			name:    "the key carries a payload",
			built:   "CREATE TABLE parent (id bigint NOT NULL, code bigint);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint, CONSTRAINT parent_pk PRIMARY KEY (id) INCLUDE (code));\n",
			want:    "parent_pk: PRIMARY KEY (id) INCLUDE (code)",
		},
		{
			name:    "a key moved to another column takes the name",
			built:   "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\nINSERT INTO parent VALUES (1, 1);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (code));\n",
			want:    "parent_pk: PRIMARY KEY (code)",
		},
		{
			name:    "a named key moved and renamed by the column",
			built:   "CREATE TABLE parent (id bigint CONSTRAINT old_name PRIMARY KEY, code bigint NOT NULL);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint CONSTRAINT parent_pk PRIMARY KEY);\n",
			want:    "parent_pk: PRIMARY KEY (code)",
		},
		{
			name: "a key moved under a foreign key takes the name",
			built: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL UNIQUE);\n" +
				"CREATE TABLE child (id bigint PRIMARY KEY, parent_code bigint REFERENCES parent (code));\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL UNIQUE, code bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (code));\n" +
				"CREATE TABLE child (id bigint PRIMARY KEY, parent_code bigint REFERENCES parent (code));\n",
			want: "parent_pk: PRIMARY KEY (code)",
		},
		{
			name:    "a UNIQUE holds the name the key takes",
			built:   "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pk UNIQUE (id));\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (id));\n",
			want:    "parent_pk: PRIMARY KEY (id)",
		},
		{
			name:    "an old key and a UNIQUE that holds the name",
			built:   "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_pk UNIQUE (code));\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (code));\n",
			want:    "parent_pk: PRIMARY KEY (code)",
		},
		{
			name: "a foreign key over a UNIQUE that holds the name",
			built: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pk UNIQUE (code));\n" +
				"CREATE TABLE child (id bigint PRIMARY KEY, parent_code bigint REFERENCES parent (code));\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (code));\n" +
				"CREATE TABLE child (id bigint PRIMARY KEY, parent_code bigint REFERENCES parent (code));\n",
			want: "parent_pk: PRIMARY KEY (code)",
		},
		{
			name:    "a declaration with no name takes the server's",
			built:   "CREATE TABLE parent (id bigint NOT NULL);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, PRIMARY KEY (id));\n",
			want:    "parent_pkey: PRIMARY KEY (id)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, test.built)

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, columnKeyRead), qt.Equals, test.want)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyDeclaredKeyNameE2E_OnCockroachDB names a key on a table that has
// none, which the server takes in place of its hidden key.
func TestSchemaApplyDeclaredKeyNameE2E_OnCockroachDB(t *testing.T) {
	tests := []struct{ name, desired string }{
		{name: "the table names it", desired: "CREATE TABLE parent (id INT8 NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (id));\n"},
		{name: "the column names it", desired: "CREATE TABLE parent (id INT8 CONSTRAINT parent_pk PRIMARY KEY);\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), "CREATE TABLE parent (id INT8 NOT NULL);\n")

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, "SELECT conname FROM pg_constraint WHERE conrelid = 'public.parent'::regclass AND contype = 'p'"), qt.Equals, "parent_pk")
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyDeclaredKeyNameE2E_AServerThatNamesEveryKey is the control: MySQL
// and MariaDB call every primary key PRIMARY, so the plan writes no name, moves the
// key in one statement, and a second run must not plan it again under the declared
// name.
func TestSchemaApplyDeclaredKeyNameE2E_AServerThatNamesEveryKey(t *testing.T) {
	const (
		noKey   = "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL);\n"
		oldKey  = "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (id));\n"
		desired = "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (code));\n"
	)
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}
	tables := []struct{ name, built string }{
		{name: "a table with no key", built: noKey},
		{name: "a table whose key moves", built: oldKey},
	}
	for _, engine := range engines {
		for _, table := range tables {
			t.Run(engine.name+" "+table.name, func(t *testing.T) {
				c := qt.New(t)
				name, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, table.built)
				schema := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)

				run := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

				c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
				c.Assert(columnKeyString(c, target, "SELECT CONCAT(INDEX_NAME, ': ', COLUMN_NAME) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '"+name+"' AND TABLE_NAME = 'parent' AND INDEX_NAME = 'PRIMARY'"), qt.Equals, "PRIMARY: code")
				again := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
				c.Assert(again.Stdout, qt.Contains, "Schema is synced")
			})
		}
	}
}
