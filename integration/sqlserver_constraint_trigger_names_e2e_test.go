//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// SQL Server keeps the constraints and the DML triggers of a table in the
// namespace of its schema, beside tables, views, routines and sequences and
// beside the constraints and triggers of other tables. Measured on SQL Server
// 2025 under a case-insensitive collation: CREATE VIEW [dbo].[REPORT] beside
// the primary key Report of dbo.t1 fails with error 2714, and so does CREATE
// TRIGGER [Audit] beside the procedure dbo.AUDIT (stokaro/ptah#6767).
// Measured on SQL Server 2022, the primary key pk of dbo.t2 beside the
// primary key pk of dbo.t1 fails with error 2714, and so does the trigger
// Audit of dbo.t2 beside the trigger Audit of dbo.t1.

// sqlServerNamespaceObjects reads the tables, views, routines, constraints
// and triggers the database at url holds, each as its stored name and its
// type, ordered by name under a binary collation. The table a native apply
// logs its runs in, and its key, are left out.
func sqlServerNamespaceObjects(c *qt.C, url string) string {
	c.Helper()
	return readCaseOnlyFact(c, url, `SELECT ISNULL(STRING_AGG(CONCAT(name, N' ', RTRIM(type)) COLLATE Latin1_General_BIN, N',')
			WITHIN GROUP (ORDER BY name COLLATE Latin1_General_BIN), N'')
		FROM sys.objects WHERE is_ms_shipped = 0 AND type IN ('U', 'V', 'P', 'FN', 'PK', 'UQ', 'C', 'F', 'TR')
			AND name NOT LIKE N'ptah[_]%' AND OBJECT_NAME(parent_object_id) IS DISTINCT FROM N'ptah_schema_apply_log'`)
}

const (
	reportKeyTable = "CREATE TABLE dbo.t1 (id INT NOT NULL CONSTRAINT Report PRIMARY KEY)"
	reportView     = "CREATE VIEW dbo.REPORT AS SELECT 1 AS x;\nGO\n"
)

// TestSchemaApplyRefusesASQLServerConstraintOrTriggerNamedLikeAnotherKindE2E
// applies, on a case-insensitive database, a schema that names a constraint
// or a trigger like an object of another kind or like a constraint or a
// trigger of another table, or creates an object beside a constraint of that
// name the database keeps or drops after the create. The plan is refused before
// anything runs, naming both objects, and the database keeps what it held.
func TestSchemaApplyRefusesASQLServerConstraintOrTriggerNamedLikeAnotherKindE2E(t *testing.T) {
	tests := []struct {
		name   string
		held   []string
		schema string
		want   string
		kept   string
	}{
		{
			name:   "a view beside a declared primary key",
			held:   []string{reportKeyTable},
			schema: reportKeyTable + ";\nGO\n" + reportView,
			want: "target view dbo.REPORT and constraint dbo.Report on dbo.t1 may have the same catalog identity, and " +
				"sqlserver keeps both kinds in one namespace in a schema",
			kept: "Report PK,t1 U",
		},
		{
			name:   "a view beside the primary key an unnamed declaration keeps",
			held:   []string{reportKeyTable},
			schema: "CREATE TABLE dbo.t1 (id INT NOT NULL PRIMARY KEY);\nGO\n" + reportView,
			want: "the plan creates view dbo.REPORT, and the database keeps constraint dbo.Report on dbo.t1, which it may " +
				"read as one name; sqlserver keeps both kinds in one namespace in a schema, so the create would fail " +
				"while the constraint exists",
			kept: "Report PK,t1 U",
		},
		{
			name: "a trigger beside a procedure",
			held: []string{"CREATE TABLE dbo.t2 (id INT NOT NULL)"},
			schema: "CREATE TABLE dbo.t2 (id INT NOT NULL);\nGO\n" +
				"CREATE TRIGGER dbo.Audit ON dbo.t2 AFTER INSERT AS BEGIN SET NOCOUNT ON; END;\nGO\n" +
				"CREATE PROCEDURE dbo.AUDIT AS SELECT 1 AS x;\nGO\n",
			want: "target procedure dbo.AUDIT and trigger dbo.Audit on dbo.t2 may have the same catalog identity",
			kept: "t2 U",
		},
		{
			name: "a constraint and a trigger in place of views",
			held: []string{
				"CREATE TABLE dbo.t1 (id INT NOT NULL CONSTRAINT PK_t1 PRIMARY KEY, n INT NULL)",
				"CREATE VIEW dbo.Report AS SELECT 1 AS x", "CREATE VIEW dbo.Audit AS SELECT 1 AS x",
			},
			schema: "CREATE TABLE dbo.t1 (id INT NOT NULL CONSTRAINT PK_t1 PRIMARY KEY, n INT NULL, CONSTRAINT REPORT UNIQUE (n));\nGO\n" +
				"CREATE TRIGGER dbo.AUDIT ON dbo.t1 AFTER INSERT AS BEGIN SET NOCOUNT ON; END;\nGO\n",
			want: "the plan creates constraint dbo.REPORT on dbo.t1 and drops view dbo.Report, and sqlserver keeps both " +
				"kinds in one namespace in a schema, where it may read the two names as one, so the create would fail " +
				"while the view exists; make the change in two migrations",
			kept: "Audit V,PK_t1 PK,Report V,t1 U",
		},
		{
			name: "two primary keys of two tables",
			schema: "CREATE TABLE dbo.t1 (id INT NOT NULL CONSTRAINT pk PRIMARY KEY);\nGO\n" +
				"CREATE TABLE dbo.t2 (id INT NOT NULL CONSTRAINT pk PRIMARY KEY);\nGO\n",
			want: "target constraint dbo.pk on dbo.t1 and constraint dbo.pk on dbo.t2 are one name, and sqlserver keeps " +
				"the constraints of every table in one namespace in a schema",
			kept: "",
		},
		{
			name: "two triggers of two tables",
			held: []string{"CREATE TABLE dbo.t1 (id INT NOT NULL)", "CREATE TABLE dbo.t2 (id INT NOT NULL)"},
			schema: "CREATE TABLE dbo.t1 (id INT NOT NULL);\nGO\nCREATE TABLE dbo.t2 (id INT NOT NULL);\nGO\n" +
				"CREATE TRIGGER dbo.Audit ON dbo.t1 AFTER INSERT AS BEGIN SET NOCOUNT ON; END;\nGO\n" +
				"CREATE TRIGGER dbo.Audit ON dbo.t2 AFTER INSERT AS BEGIN SET NOCOUNT ON; END;\nGO\n",
			want: "target trigger dbo.Audit on dbo.t1 and trigger dbo.Audit on dbo.t2 are one name, and sqlserver keeps " +
				"the triggers of every table in one namespace in a schema",
			kept: "t1 U,t2 U",
		},
		{
			name: "a unique constraint moved to another table",
			held: []string{
				"CREATE TABLE dbo.t1 (id INT NOT NULL, a INT NOT NULL CONSTRAINT uq UNIQUE)",
				"CREATE TABLE dbo.t2 (id INT NOT NULL, a INT NOT NULL)",
			},
			schema: "CREATE TABLE dbo.t1 (id INT NOT NULL, a INT NOT NULL);\nGO\n" +
				"CREATE TABLE dbo.t2 (id INT NOT NULL, a INT NOT NULL CONSTRAINT uq UNIQUE);\nGO\n",
			want: "the plan creates constraint dbo.uq on dbo.t2 and drops constraint dbo.uq on dbo.t1, and sqlserver keeps " +
				"the constraints of every table in one namespace in a schema, where the two are one name, so the create " +
				"would fail while the constraint exists; make the change in two migrations",
			kept: "t1 U,t2 U,uq UQ",
		},
		{
			name:   "a table replaced by one under its primary key name",
			held:   []string{"CREATE TABLE dbo.t1 (id INT NOT NULL CONSTRAINT pk PRIMARY KEY)"},
			schema: "CREATE TABLE dbo.t2 (id INT NOT NULL CONSTRAINT pk PRIMARY KEY);\nGO\n",
			want: "the plan creates constraint dbo.pk on dbo.t2 and drops constraint dbo.pk on dbo.t1, and sqlserver keeps " +
				"the constraints of every table in one namespace in a schema",
			kept: "pk PK,t1 U",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := sqlServerDatabaseHolding(c, "SQL_Latin1_General_CP1_CI_AS", test.held...)
			schema := schemaFilePath(c, "schema.sql", test.schema)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{},
				"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(result.Stderr, qt.Contains, test.want)
			c.Assert(sqlServerNamespaceObjects(c, target), qt.Equals, test.kept)
		})
	}
}

// TestSchemaApplyKeepsSQLServerConstraintNamesApartE2E is the control: a
// case-sensitive database reads REPORT and Report as two names, so the view
// is created beside the primary key, and a second run finds nothing to do.
func TestSchemaApplyKeepsSQLServerConstraintNamesApartE2E(t *testing.T) {
	c := qt.New(t)
	target := sqlServerDatabaseHolding(c, "Latin1_General_100_CS_AS", reportKeyTable)
	schema := schemaFilePath(c, "schema.sql", "CREATE TABLE dbo.t1 (id INT NOT NULL PRIMARY KEY);\nGO\n"+reportView)
	apply := func() clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{},
			"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
	}

	applied := apply()
	again := apply()

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
	c.Assert(sqlServerNamespaceObjects(c, target), qt.Equals, "REPORT V,Report PK,t1 U")
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// TestSchemaApplyMovesASQLServerTriggerToAnotherTableE2E is the control for
// the rows above: the plan drops a trigger before it creates one, so a trigger
// that moves to another table under its name is applied, and a second run
// finds nothing to do.
func TestSchemaApplyMovesASQLServerTriggerToAnotherTableE2E(t *testing.T) {
	c := qt.New(t)
	tables := "CREATE TABLE dbo.t1 (id INT NOT NULL);\nGO\nCREATE TABLE dbo.t2 (id INT NOT NULL);\nGO\n"
	target := sqlServerDatabaseHolding(c, "SQL_Latin1_General_CP1_CI_AS",
		"CREATE TABLE dbo.t1 (id INT NOT NULL)", "CREATE TABLE dbo.t2 (id INT NOT NULL)",
		"CREATE TRIGGER dbo.Audit ON dbo.t1 AFTER INSERT AS BEGIN SET NOCOUNT ON; END")
	schema := schemaFilePath(c, "schema.sql", tables+"CREATE TRIGGER dbo.Audit ON dbo.t2 AFTER INSERT AS BEGIN SET NOCOUNT ON; END;\nGO\n")
	apply := func() clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{},
			"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
	}

	applied := apply()
	again := apply()

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
	c.Assert(readCaseOnlyFact(c, target, `SELECT ISNULL(STRING_AGG(CONCAT(name, N' on ', OBJECT_NAME(parent_id)), N','), N'')
		FROM sys.triggers WHERE is_ms_shipped = 0`), qt.Equals, "Audit on t2")
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}
