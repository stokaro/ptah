package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// SQL Server keeps the tables, views, routines and sequences of a schema in
// one namespace. A declaration that gives two of them one name, as the
// collation compares names, cannot be created: measured on SQL Server 2025,
// CREATE OR ALTER PROCEDURE [dbo].[REPORT] beside the view dbo.Report fails
// with error 2010 under a case-insensitive collation (stokaro/ptah#6741).

// reportView is the view dbo.Report beside the given declarations.
func reportView(desired schemamodel.Database) *schemamodel.Database {
	desired.Views = append(desired.Views, schemamodel.View{Name: "dbo.Report", Body: "SELECT 1 AS a"})
	return &desired
}

// reportProcedure is a procedure named name.
func reportProcedure(name string) schemamodel.Database {
	return schemamodel.Database{Functions: []schemamodel.Function{
		{Name: name, Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN SELECT 2 END"},
	}}
}

func TestValidateDesiredSchema_SQLServerNamesakesOfTwoKinds_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    string
	}{
		{
			name:    "a procedure whose name differs from a view's only in case",
			desired: reportView(reportProcedure("REPORT")),
			want: `target view dbo\.Report and procedure dbo\.REPORT may have the same catalog identity, ` +
				`and sqlserver keeps both kinds in one namespace in a schema`,
		},
		{
			name:    "a table spelled as a view",
			desired: reportView(schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Report", Name: "Report"}}}),
			want: `target table dbo\.Report and view dbo\.Report are one name, and sqlserver keeps both kinds in one ` +
				`namespace in a schema`,
		},
		{
			name: "a sequence spelled as a function",
			desired: &schemamodel.Database{
				Functions: []schemamodel.Function{{Name: "next_id", Returns: "int", Body: "BEGIN RETURN 1 END"}},
				Sequences: []schemamodel.Sequence{{Schema: "dbo", Name: "next_id"}},
			},
			want: `target function dbo\.next_id and sequence dbo\.next_id are one name`,
		},
		{
			name: "a procedure spelled as a function",
			desired: &schemamodel.Database{Functions: []schemamodel.Function{
				{Name: "x", Returns: "int", Body: "BEGIN RETURN 1 END"},
				{Name: "x", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN SELECT 1 END"},
			}},
			want: `target function dbo\.x and procedure dbo\.x are one name, and sqlserver keeps both kinds in one ` +
				`namespace in a schema`,
		},
		{
			name:    "a non-ASCII procedure beside a view offline",
			desired: reportView(reportProcedure("Größe")),
			want: `without a connection Ptah cannot tell whether target view dbo\.Report and procedure dbo\.Größe ` +
				`are one name under the database collation, and sqlserver keeps both kinds in one namespace in a schema`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), test.desired,
				catalog.ServerInfo{Dialect: platform.SQLServer})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*`+test.want+`.*`)
		})
	}
}

// TestValidateDesiredSchema_SQLServerRoutineInTheDefaultSchema_FailurePath
// declares the function f and the function dbo.f with two argument lists.
// An unqualified routine is in the default schema, so they are one routine,
// and SQL Server has no overloading: applied under Latin1_General_100_CI_AS,
// the second CREATE OR ALTER FUNCTION [f] replaces the first, and the
// database ends with one dbo.f. Two routines of one kind are two overloads to
// the declaration check, which leaves them to the renderer, so the refusal
// names the overloading.
func TestValidateDesiredSchema_SQLServerRoutineInTheDefaultSchema_FailurePath(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "f", Parameters: "@a int", Returns: "int", Language: "sql", Body: "BEGIN RETURN 1 END"},
		{Name: "dbo.f", Parameters: "@a nvarchar(10)", Returns: "int", Language: "sql", Body: "BEGIN RETURN 2 END"},
	}}

	err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), desired,
		catalog.ServerInfo{Dialect: platform.SQLServer})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*function "dbo.f" is declared twice, as f\(@a int\) and dbo.f\(@a nvarchar\(10\)\), `+
		`and sqlserver has no routine overloading.*`)
}

// TestCompareWithDatabase_SQLServerNamesakesOfTwoKinds_FailurePath declares
// the procedure dbo.REPORT beside the view dbo.Report on a server that reads
// the two names as one.
func TestCompareWithDatabase_SQLServerNamesakesOfTwoKinds_FailurePath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabase(c.Context(), caseInsensitiveDatabase, reportView(reportProcedure("REPORT")),
		&catalog.Database{Views: []catalog.View{{Name: "Report", Body: "SELECT 1 AS a"}}}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*target view dbo\.Report and procedure dbo\.REPORT may have the same catalog identity.*`)
	c.Assert(diff, qt.IsNil)
}

// TestCompareWithDatabase_SQLServerNamesakesOfTwoKinds_HappyPath is the
// control: a case-sensitive server reads REPORT and Report as two names, so
// the procedure is created beside the view.
func TestCompareWithDatabase_SQLServerNamesakesOfTwoKinds_HappyPath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabase(c.Context(), caseSensitiveDatabase, reportView(reportProcedure("REPORT")),
		&catalog.Database{Views: []catalog.View{{Name: "Report", Body: "SELECT 1 AS a"}}}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(namedObjectChanges(diff), qt.DeepEquals, []string{"routine added REPORT"})
}

// TestValidateDesiredSchema_NamesakesOfTwoKinds_HappyPath declares objects of
// two kinds that are two names, or that a target keeps in two namespaces.
func TestValidateDesiredSchema_NamesakesOfTwoKinds_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		desired *schemamodel.Database
	}{
		{name: "two ASCII names offline", dialect: platform.SQLServer, desired: reportView(reportProcedure("report_rows"))},
		{name: "one name in two schemas", dialect: platform.SQLServer, desired: reportView(reportProcedure("sales.Report"))},
		{
			// PostgreSQL keeps routines apart from relations.
			name:    "a PostgreSQL function named as a view",
			dialect: platform.Postgres,
			desired: &schemamodel.Database{
				Views:     []schemamodel.View{{Name: "public.report", Body: "SELECT 1 AS a"}},
				Functions: []schemamodel.Function{{Name: "public.report", Returns: "integer", Language: "sql", Body: "SELECT 1"}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), test.desired,
				catalog.ServerInfo{Dialect: test.dialect})

			c.Assert(err, qt.IsNil)
		})
	}
}

// SQL Server keeps the constraints and the triggers of a table in that
// namespace too, in the schema of the table. Measured on SQL Server 2025:
// CREATE VIEW [dbo].[REPORT] beside the primary key Report of dbo.t1 fails with
// error 2714, and so does CREATE TRIGGER [Audit] beside the procedure
// dbo.AUDIT (stokaro/ptah#6767).

// reportTable is the table dbo.t1, given as table, with a column n, beside
// the given declarations.
func reportTable(desired schemamodel.Database, table schemamodel.Table, n schemamodel.Field) schemamodel.Database {
	table.StructName = "T1"
	n.StructName, n.Name, n.Type = "T1", "n", "INT"
	desired.Tables = append(desired.Tables, table)
	desired.Fields = append(desired.Fields, n)
	return desired
}

func TestValidateDesiredSchema_SQLServerConstraintsAndTriggersBesideOtherKinds_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    string
	}{
		{
			name:    "a primary key named as a view",
			desired: reportView(reportTable(schemamodel.Database{}, schemamodel.Table{Name: "t1", PrimaryKeyName: "REPORT"}, schemamodel.Field{})),
			want: `target view dbo\.Report and constraint dbo\.REPORT on dbo\.t1 may have the same catalog identity, ` +
				`and sqlserver keeps both kinds in one namespace in a schema`,
		},
		{
			name: "a column check named as a procedure",
			desired: new(reportTable(reportProcedure("Report"), schemamodel.Table{Name: "t1"},
				schemamodel.Field{Check: "n > 0", CheckName: "REPORT"})),
			want: `target procedure dbo\.Report and constraint dbo\.REPORT on dbo\.t1 may have the same catalog identity`,
		},
		{
			name: "a column foreign key named as a table",
			desired: new(reportTable(schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Parent", Name: "parent"}}},
				schemamodel.Table{Name: "t1"}, schemamodel.Field{Foreign: "parent(id)", ForeignKeyName: "parent"})),
			want: `target table dbo\.parent and constraint dbo\.parent on dbo\.t1 are one name`,
		},
		{
			name: "a derived foreign key name named as a view",
			desired: reportView(reportTable(schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Parent", Name: "parent"}},
				Fields: []schemamodel.Field{{StructName: "Parent", Name: "id", Type: "INT", Primary: true}},
			}, schemamodel.Table{Name: "t1"}, schemamodel.Field{Foreign: "parent(id)", ForeignKeyName: "Report", ForeignKeyNameDerived: true})),
			want: `target view dbo\.Report and constraint dbo\.Report on dbo\.t1 are one name`,
		},
		{
			// Ptah derives fk_t1_n for the unnamed key and writes it.
			name: "an unnamed foreign key beside a table of its derived name",
			desired: new(reportTable(schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Parent", Name: "parent"}, {StructName: "Clash", Name: "fk_t1_n"}},
				Fields: []schemamodel.Field{{StructName: "Parent", Name: "id", Type: "INT", Primary: true}},
			}, schemamodel.Table{Name: "t1"}, schemamodel.Field{Foreign: "parent(id)"})),
			want: `target table dbo\.fk_t1_n and constraint dbo\.fk_t1_n on dbo\.t1 are one name`,
		},
		{
			// Measured on SQL Server 2025: CREATE VIEW t1_pkey beside the
			// table t1, whose key Ptah writes as t1_pkey, fails with error 2714.
			name: "an unnamed primary key beside a view of its name",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T1", Name: "t1", PrimaryKey: []string{"id"}}},
				Fields: []schemamodel.Field{{StructName: "T1", Name: "id", Type: "INT", Primary: true}},
				Views:  []schemamodel.View{{Name: "T1_PKEY", Body: "SELECT 1 AS a"}},
			},
			want: `target view dbo\.T1_PKEY and constraint dbo\.t1_pkey on dbo\.t1 may have the same catalog identity`,
		},
		{
			name: "an unnamed primary key on the column beside a procedure of its name",
			desired: new(reportTable(reportProcedure("t1_pkey"), schemamodel.Table{Name: "t1"},
				schemamodel.Field{Primary: true})),
			want: `target procedure dbo\.t1_pkey and constraint dbo\.t1_pkey on dbo\.t1 are one name`,
		},
		{
			name:    "an unnamed column check beside a procedure of its name",
			desired: new(reportTable(reportProcedure("t1_n_check"), schemamodel.Table{Name: "t1"}, schemamodel.Field{Check: "n > 0"})),
			want:    `target procedure dbo\.t1_n_check and constraint dbo\.t1_n_check on dbo\.t1 are one name`,
		},
		{
			name: "a table-level constraint named as a sequence in its table's schema",
			desired: &schemamodel.Database{
				Tables:      []schemamodel.Table{{StructName: "T1", Schema: "sales", Name: "t1"}},
				Constraints: []schemamodel.Constraint{{StructName: "T1", Name: "next_id", Type: "UNIQUE", Columns: []string{"n"}}},
				Sequences:   []schemamodel.Sequence{{Schema: "sales", Name: "next_id"}},
			},
			want: `target sequence sales\.next_id and constraint sales\.next_id on sales\.t1 are one name`,
		},
		{
			name: "a trigger named as a procedure",
			desired: &schemamodel.Database{
				Functions: reportProcedure("AUDIT").Functions,
				Triggers:  []schemamodel.Trigger{{Name: "Audit", Table: "t2", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SET NOCOUNT ON; END"}},
			},
			want: `target procedure dbo\.AUDIT and trigger dbo\.Audit on dbo\.t2 may have the same catalog identity`,
		},
		{
			name: "a trigger whose name carries its schema",
			desired: &schemamodel.Database{
				Functions: reportProcedure("sales.Audit").Functions,
				Triggers:  []schemamodel.Trigger{{Name: "sales.Audit", Table: "sales.t2", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SET NOCOUNT ON; END"}},
			},
			want: `target procedure sales\.Audit and trigger sales\.Audit on sales\.t2 are one name`,
		},
		{
			name: "a trigger named as a constraint",
			desired: new(reportTable(schemamodel.Database{
				Triggers: []schemamodel.Trigger{{Name: "UQ_n", Table: "t1", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SET NOCOUNT ON; END"}},
			}, schemamodel.Table{Name: "t1"}, schemamodel.Field{Check: "n > 0", CheckName: "UQ_n"})),
			want: `target constraint dbo\.UQ_n on dbo\.t1 and trigger dbo\.UQ_n on dbo\.t1 are one name`,
		},
		{
			name: "two primary keys of two tables named alike",
			desired: &schemamodel.Database{Tables: []schemamodel.Table{
				{StructName: "T1", Name: "t1", PrimaryKeyName: "pk"},
				{StructName: "T2", Name: "t2", PrimaryKeyName: "pk"},
			}},
			want: `target constraint dbo\.pk on dbo\.t1 and constraint dbo\.pk on dbo\.t2 are one name, and sqlserver keeps ` +
				`the constraints of every table in one namespace in a schema`,
		},
		{
			name: "a check and a primary key of two tables in another case",
			desired: new(reportTable(schemamodel.Database{Tables: []schemamodel.Table{{StructName: "T2", Name: "t2", PrimaryKeyName: "RULE"}}},
				schemamodel.Table{Name: "t1"}, schemamodel.Field{Check: "n > 0", CheckName: "Rule"})),
			want: `target constraint dbo\.RULE on dbo\.t2 and constraint dbo\.Rule on dbo\.t1 may have the same catalog identity`,
		},
		{
			name: "two triggers of two tables named alike",
			desired: &schemamodel.Database{Triggers: []schemamodel.Trigger{
				{Name: "Audit", Table: "t1", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SET NOCOUNT ON; END"},
				{Name: "Audit", Table: "t2", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SET NOCOUNT ON; END"},
			}},
			want: `target trigger dbo\.Audit on dbo\.t1 and trigger dbo\.Audit on dbo\.t2 are one name, and sqlserver keeps ` +
				`the triggers of every table in one namespace in a schema`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), test.desired,
				catalog.ServerInfo{Dialect: platform.SQLServer})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*`+test.want+`.*`)
		})
	}
}

func TestValidateDesiredSchema_SQLServerConstraintsAndTriggersBesideOtherKinds_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		desired *schemamodel.Database
	}{
		{
			// A constraint is in the schema of its table.
			name:    "a primary key named as a view of another schema",
			dialect: platform.SQLServer,
			desired: reportView(reportTable(schemamodel.Database{}, schemamodel.Table{Schema: "sales", Name: "t1", PrimaryKeyName: "Report"}, schemamodel.Field{})),
		},
		{
			// The plan writes the declared name, not the one Ptah would
			// derive for a key nobody named.
			name:    "a named primary key beside a view of the derived name",
			dialect: platform.SQLServer,
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T1", Name: "t1", PrimaryKey: []string{"id"}, PrimaryKeyName: "pk_t1"}},
				Fields: []schemamodel.Field{{StructName: "T1", Name: "id", Type: "INT", Primary: true}},
				Views:  []schemamodel.View{{Name: "t1_pkey", Body: "SELECT 1 AS a"}},
			},
		},
		{
			// A constraint is in the schema of its table.
			name:    "an unnamed primary key beside a view of its name in another schema",
			dialect: platform.SQLServer,
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T1", Schema: "sales", Name: "t1", PrimaryKey: []string{"id"}}},
				Fields: []schemamodel.Field{{StructName: "T1", Name: "id", Type: "INT", Primary: true}},
				Views:  []schemamodel.View{{Name: "t1_pkey", Body: "SELECT 1 AS a"}},
			},
		},
		{
			// MySQL names every primary key PRIMARY and an unnamed check
			// t1_chk_1, and keeps both apart from views.
			name:    "a MySQL unnamed primary key and check beside views of the SQL Server names",
			dialect: platform.MySQL,
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T1", Name: "t1", PrimaryKey: []string{"id"}}},
				Fields: []schemamodel.Field{
					{StructName: "T1", Name: "id", Type: "INT", Primary: true},
					{StructName: "T1", Name: "n", Type: "INT", Check: "n > 0"},
				},
				Views: []schemamodel.View{{Name: "t1_pkey", Body: "SELECT 1 AS a"}, {Name: "t1_n_check", Body: "SELECT 1 AS a"}},
			},
		},
		{
			// MySQL keeps a constraint apart from views and routines.
			name:    "a MySQL check named as a view",
			dialect: platform.MySQL,
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T1", Name: "t1"}},
				Fields: []schemamodel.Field{{StructName: "T1", Name: "n", Type: "INT", Check: "n > 0", CheckName: "report"}},
				Views:  []schemamodel.View{{Name: "report", Body: "SELECT 1 AS a"}},
			},
		},
		{
			name:    "two primary keys named alike in two schemas",
			dialect: platform.SQLServer,
			desired: &schemamodel.Database{Tables: []schemamodel.Table{
				{StructName: "T1", Name: "t1", PrimaryKeyName: "pk"},
				{StructName: "T2", Schema: "sales", Name: "t2", PrimaryKeyName: "pk"},
			}},
		},
		{
			// PostgreSQL names a check in its table.
			name:    "two PostgreSQL checks of two tables named alike",
			dialect: platform.Postgres,
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T1", Name: "t1"}, {StructName: "T2", Name: "t2"}},
				Fields: []schemamodel.Field{
					{StructName: "T1", Name: "n", Type: "INTEGER", Check: "n > 0", CheckName: "positive"},
					{StructName: "T2", Name: "n", Type: "INTEGER", Check: "n > 0", CheckName: "positive"},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), test.desired,
				catalog.ServerInfo{Dialect: test.dialect})

			c.Assert(err, qt.IsNil)
		})
	}
}

// heldReportKey is a database holding the table dbo.t1, whose primary key the
// server named Report.
func heldReportKey() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t1", Type: "TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
		}}},
		Constraints: []catalog.Constraint{{Name: "Report", TableName: "t1", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}}},
	}
}

// unnamedReportKey declares dbo.t1 with an unnamed primary key, beside the
// view dbo.REPORT.
func unnamedReportKey() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T1", Name: "t1", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{{StructName: "T1", Name: "id", Type: "INT", Primary: true}},
		Views:  []schemamodel.View{{Name: "REPORT", Body: "SELECT 1 AS a"}},
	}
}

func TestCompareWithDatabase_SQLServerCreatesBesideAKeptConstraintOrTrigger_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		held    *catalog.Database
		want    string
	}{
		{
			name:    "a view beside the primary key an unnamed declaration keeps",
			desired: unnamedReportKey(),
			held:    heldReportKey(),
			want: `the plan creates view dbo\.REPORT, and the database keeps constraint dbo\.Report on dbo\.t1, which it ` +
				`may read as one name; sqlserver keeps both kinds in one namespace in a schema, so the create would fail ` +
				`while the constraint exists; give the view another name, or rename or drop the constraint first`,
		},
		{
			name: "a trigger beside the primary key an unnamed declaration keeps",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T1", Name: "t1", PrimaryKey: []string{"id"}}},
				Fields: []schemamodel.Field{{StructName: "T1", Name: "id", Type: "INT", Primary: true}},
				Triggers: []schemamodel.Trigger{{
					Name: "REPORT", Table: "t1", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SET NOCOUNT ON; END",
				}},
			},
			held: heldReportKey(),
			want: `the plan creates trigger dbo\.REPORT on dbo\.t1, and the database keeps constraint dbo\.Report on dbo\.t1, ` +
				`which it may read as one name`,
		},
		{
			name: "the primary key of a created table beside the key an unnamed declaration keeps",
			desired: func() *schemamodel.Database {
				desired := unnamedReportKey()
				desired.Views = nil
				desired.Tables = append(desired.Tables, schemamodel.Table{StructName: "T2", Name: "t2", PrimaryKeyName: "REPORT"})
				return desired
			}(),
			held: heldReportKey(),
			want: `the plan creates constraint dbo\.REPORT on dbo\.t2, and the database keeps constraint dbo\.Report on dbo\.t1, ` +
				`which it may read as one name; sqlserver keeps the constraints of every table in one namespace in a schema, ` +
				`so the create would fail while the constraint exists; give the constraint another name, or rename or drop ` +
				`the constraint first`,
		},
		{
			// The renderer writes t2_pkey for the key of t2 nobody named.
			name: "the unnamed primary key of a created table beside a key the database keeps under its name",
			desired: func() *schemamodel.Database {
				desired := unnamedReportKey()
				desired.Views = nil
				desired.Tables = append(desired.Tables, schemamodel.Table{StructName: "T2", Name: "t2", PrimaryKey: []string{"id"}})
				desired.Fields = append(desired.Fields, schemamodel.Field{StructName: "T2", Name: "id", Type: "INT", Primary: true})
				return desired
			}(),
			held: func() *catalog.Database {
				held := heldReportKey()
				held.Constraints[0].Name = "T2_PKEY"
				return held
			}(),
			want: `the plan creates constraint dbo\.t2_pkey on dbo\.t2, and the database keeps constraint dbo\.T2_PKEY on dbo\.t1, ` +
				`which it may read as one name`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabase(c.Context(), caseInsensitiveDatabase, test.desired, test.held, nil, must.Must(builtin.New()))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*`+test.want+`.*`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// The controls: a case-sensitive database reads REPORT and Report as two
// names, and a constraint or a trigger the plan drops, alone or with its
// table, is the planner's to judge.
func TestCompareWithDatabase_SQLServerCreatesBesideAKeptConstraintOrTrigger_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		connection collationConnection
		desired    *schemamodel.Database
		held       *catalog.Database
		want       []string
	}{
		{
			name:       "a case-sensitive database",
			connection: caseSensitiveDatabase,
			desired:    unnamedReportKey(),
			held:       heldReportKey(),
			want:       []string{"view added REPORT"},
		},
		{
			// The server is asked about the declared name of the key, so
			// the case-sensitive answer keeps it apart from the view.
			name:       "a declared primary key on a case-sensitive database",
			connection: caseSensitiveDatabase,
			desired: func() *schemamodel.Database {
				desired := unnamedReportKey()
				desired.Tables[0].PrimaryKeyName = "Report"
				return desired
			}(),
			held: heldReportKey(),
			want: []string{"view added REPORT"},
		},
		{
			name:       "a constraint the plan drops",
			connection: caseInsensitiveDatabase,
			desired:    unnamedReportKey(),
			held: func() *catalog.Database {
				held := heldReportKey()
				held.Constraints[0].Name = "pk_t1"
				held.Constraints = append(held.Constraints, catalog.Constraint{Name: "Report", TableName: "t1", Type: "UNIQUE", ColumnNames: []string{"id"}})
				return held
			}(),
			want: []string{"view added REPORT"},
		},
		{
			// The trigger of a table the plan drops is dropped by name, so the
			// planner judges it beside the procedure.
			name:       "a trigger the plan drops",
			connection: caseInsensitiveDatabase,
			desired:    &schemamodel.Database{Functions: reportProcedure("AUDIT").Functions},
			held: &catalog.Database{
				Tables:   []catalog.Table{{Name: "t2", Type: "TABLE", Columns: []catalog.Column{{Name: "id", DataType: "int", IsNullable: "NO"}}}},
				Triggers: []catalog.Trigger{{Name: "Audit", Table: "t2", Timing: "AFTER", Event: "INSERT", Body: "BEGIN SET NOCOUNT ON; END"}},
			},
			want: []string{"routine added AUDIT", "table removed t2", "trigger removed Audit on t2"},
		},
		{
			name:       "the constraint of a table the plan drops",
			connection: caseInsensitiveDatabase,
			desired:    &schemamodel.Database{Views: []schemamodel.View{{Name: "REPORT", Body: "SELECT 1 AS a"}}},
			held:       heldReportKey(),
			want:       []string{"table removed t1", "view added REPORT"},
		},
		{
			// The server is asked about the declared name of a key the
			// database does not hold too.
			name:       "a declared primary key of a created table on a case-sensitive database",
			connection: caseSensitiveDatabase,
			desired: func() *schemamodel.Database {
				desired := unnamedReportKey()
				desired.Tables[0].PrimaryKeyName = "Report"
				return desired
			}(),
			held: &catalog.Database{},
			want: []string{"table added t1", "view added REPORT"},
		},
		{
			name:       "the primary key of a created table on a case-sensitive database",
			connection: caseSensitiveDatabase,
			desired: func() *schemamodel.Database {
				desired := unnamedReportKey()
				desired.Views = nil
				desired.Tables = append(desired.Tables, schemamodel.Table{StructName: "T2", Name: "t2", PrimaryKeyName: "REPORT"})
				return desired
			}(),
			held: heldReportKey(),
			want: []string{"table added t2"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabase(c.Context(), test.connection, test.desired, test.held, nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(namedObjectChanges(diff), qt.DeepEquals, test.want)
		})
	}
}
