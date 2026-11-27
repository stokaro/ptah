//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// Without a connection, nothing says how a SQL Server database compares two
// names: under Hungarian_CI_AS `ccs` and `cscs` are one name, so a plan that
// creates dbo.cscs and drops dbo.ccs leaves that database with no function at
// all. The Go comparison API with no connection therefore refuses every plan
// that creates an object and drops another of its kind, and a plan made with
// a connection pairs the names as the database does (stokaro/ptah#6746).

// sqlServerContraction is a database of collation holding the function
// dbo.ccs, read back, with a connection to it.
func sqlServerContraction(c *qt.C, collation string) (url string, read *catalog.Database) {
	c.Helper()
	url = sqlServerCollatedDatabase(c, collation)
	conn := connectDevDialect(c, url)
	_, err := conn.ExecContext(c.Context(), "CREATE FUNCTION [dbo].[ccs]() RETURNS int AS BEGIN RETURN 1 END")
	c.Assert(err, qt.IsNil)
	read, err = conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	return url, read
}

// declaredCscs declares the function dbo.cscs, which returns 2.
func declaredCscs() *schemamodel.Database {
	return &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "dbo.cscs", Returns: "int", Body: "BEGIN RETURN 2 END"},
	}}
}

// sqlServerFunctions reads the functions the database at url holds, by their
// stored names, each with what it returns.
func sqlServerFunctions(c *qt.C, url string) string {
	c.Helper()
	return readCaseOnlyFact(c, url, `SELECT ISNULL(STRING_AGG(CONCAT(o.name, N' ',
			CASE WHEN m.definition LIKE N'%RETURN 2%' THEN N'2' ELSE N'1' END) COLLATE Latin1_General_BIN, N',')
			WITHIN GROUP (ORDER BY o.name COLLATE Latin1_General_BIN), N'')
		FROM sys.objects AS o JOIN sys.sql_modules AS m ON m.object_id = o.object_id
		WHERE o.type = 'FN'`)
}

// TestPlanningASQLServerContractionWithoutAConnection_FailurePath compares
// the declaration with the database as read, but without a connection, on a
// Hungarian database and on a Latin one. Neither plan is written, whatever
// the collation, and the database keeps its function.
func TestPlanningASQLServerContractionWithoutAConnection_FailurePath(t *testing.T) {
	for _, collation := range []string{"Hungarian_CI_AS", "Latin1_General_100_CI_AS"} {
		t.Run(collation, func(t *testing.T) {
			c := qt.New(t)
			url, read := sqlServerContraction(c, collation)
			runtime := must.Must(builtin.New())

			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declaredCscs(), read,
				catalog.ServerInfo{Dialect: platform.SQLServer}, nil, runtime)
			c.Assert(err, qt.IsNil)
			statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, platform.SQLServer)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*the plan creates function dbo\.cscs and drops function ccs, and without a `+
				`connection Ptah cannot tell whether they are one name under the database collation.*`)
			c.Assert(statements, qt.HasLen, 0)
			c.Assert(sqlServerFunctions(c, url), qt.Equals, "ccs 1")
		})
	}
}

// TestPlanningASQLServerContractionWithAConnection_HappyPath compares the
// declaration with the database on a connection and runs the plan. The
// Hungarian database reads the two names as one function, so the plan
// replaces it; the Latin one reads them as two, so the plan is an ordinary
// rename. Either way the database holds the declared function afterwards. A
// plan that paired the names the other way would leave the Hungarian database
// with none: its CREATE OR ALTER changes dbo.ccs, and its DROP of dbo.ccs then
// removes it.
func TestPlanningASQLServerContractionWithAConnection_HappyPath(t *testing.T) {
	tests := []struct {
		collation string
		want      string
	}{
		{collation: "Hungarian_CI_AS", want: "cscs 2"},
		{collation: "Latin1_General_100_CI_AS", want: "cscs 2"},
	}
	for _, test := range tests {
		t.Run(test.collation, func(t *testing.T) {
			c := qt.New(t)
			url, read := sqlServerContraction(c, test.collation)
			conn := connectDevDialect(c, url)
			runtime := must.Must(builtin.New())

			diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, declaredCscs(), read, nil, runtime)
			c.Assert(err, qt.IsNil)
			statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, platform.SQLServer)
			c.Assert(err, qt.IsNil)
			for _, statement := range statements {
				_, err := conn.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}

			c.Assert(sqlServerFunctions(c, url), qt.Equals, test.want)
		})
	}
}
