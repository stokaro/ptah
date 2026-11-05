//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// keyBeforeColumnsChild is a foreign key the declaration keeps over parent.id.
const keyBeforeColumnsChild = "CREATE TABLE child (id bigint PRIMARY KEY, p bigint, CONSTRAINT child_fk FOREIGN KEY (p) REFERENCES parent (id));\n"

// keyBeforeColumnsNullable counts the columns of parent that allow NULL.
const keyBeforeColumnsNullable = "SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.parent'::regclass AND attnum > 0 AND NOT attisdropped AND NOT attnotnull"

// keyBeforeColumnsKey prints the primary key of public.parent as the server does, and
// the empty string where it holds none.
func keyBeforeColumnsKey(c *qt.C, target string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var key string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT coalesce((SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'public.parent'::regclass AND contype = 'p'), '')",
	).Scan(&key), qt.IsNil)
	return key
}

// TestSchemaApplyPrimaryKeyBeforeColumnsE2E_HappyPath changes a column in a way the key
// the plan removes stands in the way of. The server stopped the plan at `column "id" is
// in a primary key` or `multiple primary keys for table "parent" are not allowed`. Each
// row reads the key and the nullable columns back, and a second run finds nothing to do.
func TestSchemaApplyPrimaryKeyBeforeColumnsE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name, built, desired, key string
		nullable                  int
	}{
		{
			name:     "moved, with the old column nullable",
			built:    "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\nINSERT INTO parent VALUES (1, 1);\n",
			desired:  "CREATE TABLE parent (id bigint, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code));\n",
			key:      "PRIMARY KEY (code)",
			nullable: 1,
		},
		{
			name:     "dropped, with the column nullable",
			built:    "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\nINSERT INTO parent VALUES (1, 1);\n",
			desired:  "CREATE TABLE parent (id bigint, code bigint NOT NULL);\n",
			nullable: 1,
		},
		{
			name:     "narrowed, with the column it leaves nullable",
			built:    "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (id, code));\nINSERT INTO parent VALUES (1, 1);\n",
			desired:  "CREATE TABLE parent (id bigint NOT NULL, code bigint, CONSTRAINT parent_pkey PRIMARY KEY (id));\n",
			key:      "PRIMARY KEY (id)",
			nullable: 1,
		},
		{
			name:     "moved under a foreign key, with the old column nullable and unique",
			built:    "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\n" + keyBeforeColumnsChild + "INSERT INTO parent VALUES (1, 1);\nINSERT INTO child VALUES (1, 1);\n",
			desired:  "CREATE TABLE parent (id bigint, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code), CONSTRAINT parent_id_key UNIQUE (id));\n" + keyBeforeColumnsChild,
			key:      "PRIMARY KEY (code)",
			nullable: 1,
		},
		{
			name:    "moved to a new column that declares it",
			built:   "CREATE TABLE parent (id bigint PRIMARY KEY);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);\n",
			key:     "PRIMARY KEY (code)",
		},
		{
			name:    "moved to a new column the table's key names",
			built:   "CREATE TABLE parent (id bigint PRIMARY KEY);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code));\n",
			key:     "PRIMARY KEY (code)",
		},
		{
			name:    "a table with no key gains one over a new column",
			built:   "CREATE TABLE parent (id bigint NOT NULL);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code));\n",
			key:     "PRIMARY KEY (code)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, test.built, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(keyBeforeColumnsKey(c, target), qt.Equals, test.key)
			c.Assert(readInt(c, target, keyBeforeColumnsNullable), qt.Equals, test.nullable)
			again := keyMoveApply(c, target, test.desired, "--schemas", "public")
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyPrimaryKeyBeforeColumnsE2E_ADuplicateTakesEverythingBack stops at the
// new key's rows, after the old key has gone and the column has lost its NOT NULL. The
// plan's one transaction restores both.
func TestSchemaApplyPrimaryKeyBeforeColumnsE2E_ADuplicateTakesEverythingBack(t *testing.T) {
	c := qt.New(t)

	target, run := runExactApply(c,
		"CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\nINSERT INTO parent VALUES (1, 1), (2, 1);\n",
		"CREATE TABLE parent (id bigint, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code));\n")

	c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, "DROP CONSTRAINT \"parent_pkey\";\nALTER TABLE \"parent\" ALTER COLUMN \"id\" DROP NOT NULL;")
	c.Assert(run.Stderr, qt.Contains, `could not create unique index "parent_pkey"`)
	c.Assert(keyBeforeColumnsKey(c, target), qt.Equals, "PRIMARY KEY (id)")
	c.Assert(readInt(c, target, keyBeforeColumnsNullable), qt.Equals, 0)
}

// TestMigrationsGeneratePrimaryKeyBeforeColumnsE2E_RunsBothWays runs the plan through a
// migration pair. The down file puts NOT NULL and the key back on the old column.
func TestMigrationsGeneratePrimaryKeyBeforeColumnsE2E_RunsBothWays(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\n")
	dir := exactGeneratedFile(c, target, "CREATE TABLE parent (id bigint, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code));\n")

	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(keyBeforeColumnsKey(c, target), qt.Equals, "PRIMARY KEY (code)")
	c.Assert(readInt(c, target, keyBeforeColumnsNullable), qt.Equals, 1)

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(keyBeforeColumnsKey(c, target), qt.Equals, "PRIMARY KEY (id)")
	c.Assert(readInt(c, target, keyBeforeColumnsNullable), qt.Equals, 0)
}
