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
			want:    `target table dbo\.Report and view dbo\.Report may have the same catalog identity`,
		},
		{
			name: "a sequence spelled as a function",
			desired: &schemamodel.Database{
				Functions: []schemamodel.Function{{Name: "next_id", Returns: "int", Body: "BEGIN RETURN 1 END"}},
				Sequences: []schemamodel.Sequence{{Schema: "dbo", Name: "next_id"}},
			},
			want: `target function dbo\.next_id and sequence dbo\.next_id may have the same catalog identity`,
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
