//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// SQL Server keeps constraints in the namespace of the views, routines and
// tables of their table's schema, and Ptah writes a name for a primary key, a
// column CHECK and a foreign key the declaration leaves unnamed. A view or a
// table under that name fails with error 2714, so the declaration is refused
// before anything runs.

// sqlServerTablesAndViews lists the tables and views a database holds, beside
// the apply log, as schema.name in byte order.
const sqlServerTablesAndViews = `SELECT ISNULL(STRING_AGG(CONVERT(nvarchar(max), SCHEMA_NAME(schema_id) + N'.' + name), N',')
	WITHIN GROUP (ORDER BY SCHEMA_NAME(schema_id) + N'.' + name COLLATE Latin1_General_BIN), N'')
	FROM sys.objects WHERE type IN ('U', 'V') AND name <> N'ptah_schema_apply_log'`

// TestSchemaApplySQLServerRefusesAnObjectNamedAsAnUnnamedConstraintE2E
// applies each declaration to an empty database. Each is refused, and the
// database holds nothing.
func TestSchemaApplySQLServerRefusesAnObjectNamedAsAnUnnamedConstraintE2E(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   string
	}{
		{
			name:   "a view named as an unnamed primary key",
			schema: "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY);\nGO\nCREATE VIEW t1_pkey AS SELECT 1 AS x;\nGO\n",
			want:   `target view dbo.t1_pkey and constraint dbo.t1_pkey on dbo.t1 are one name`,
		},
		{
			name:   "a view named as an unnamed column check",
			schema: "CREATE TABLE t1 (id INT NOT NULL, c INT CHECK (c > 0));\nGO\nCREATE VIEW t1_c_check AS SELECT 1 AS x;\nGO\n",
			want:   `target view dbo.t1_c_check and constraint dbo.t1_c_check on dbo.t1 are one name`,
		},
		{
			name: "a table named as an unnamed foreign key",
			schema: "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY);\nGO\n" +
				"CREATE TABLE t2 (id INT NOT NULL, pid INT REFERENCES t1(id));\nGO\n" +
				"CREATE TABLE fk_t2_pid (id INT NOT NULL);\nGO\n",
			want: `target table dbo.fk_t2_pid and constraint dbo.fk_t2_pid on dbo.t2 are one name`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbURL := sqlServerRoleScratchDatabase(c)

			got := applySchemaText(c, dbURL, test.schema)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.want)
			c.Assert(heldColumns(c, dbURL, sqlServerTablesAndViews), qt.Equals, "")
		})
	}
}

// TestSchemaApplySQLServerAppliesAViewBesideANamedPrimaryKeyE2E is the
// control: a primary key the declaration names leaves the derived name free,
// so the view under it is created beside the table.
func TestSchemaApplySQLServerAppliesAViewBesideANamedPrimaryKeyE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)

	got := applySchemaText(c, dbURL,
		"CREATE TABLE t1 (id INT NOT NULL CONSTRAINT pk_t1 PRIMARY KEY);\nGO\nCREATE VIEW t1_pkey AS SELECT 1 AS x;\nGO\n")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(heldColumns(c, dbURL, sqlServerTablesAndViews), qt.Equals, "dbo.t1,dbo.t1_pkey")
}
