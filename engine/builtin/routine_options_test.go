package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
)

// routineSchema declares one function carrying facets.
func routineSchema(facets schemaext.Facets) *schemamodel.Database {
	return &schemamodel.Database{Functions: []schemamodel.Function{{
		Name: "dbo.audit_it", Returns: "int", Language: "sql", Security: "DEFINER",
		Body: "BEGIN RETURN 1; END", Facets: facets,
	}}}
}

// TestRenderSQL_ARoutineRunsAsItsPrincipalOnSQLServer pins that the SQL
// Server owner's principal reaches the routine's header.
func TestRenderSQL_ARoutineRunsAsItsPrincipalOnSQLServer(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{Principal: "auditor"}))

	statements, _, err := builtin.GetOrderedCreateStatementsReportingOmissions(
		routineSchema(facets), platform.SQLServer, capability.ForDialect(platform.SQLServer))

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "WITH EXECUTE AS N'auditor'")
}

// TestRenderSQL_RefusesARoutinesPrincipalElsewhere pins that another target
// refuses the principal rather than create a routine that runs as its owner or
// its caller instead. Only SQL Server names one.
func TestRenderSQL_RefusesARoutinesPrincipalElsewhere(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.MariaDB, platform.Oracle} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			facets := must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{Principal: "auditor"}))

			statements, _, err := builtin.GetOrderedCreateStatementsReportingOmissions(
				routineSchema(facets), dialect, capability.ForDialect(dialect))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*feature facet "ptah.run/mssql/routine-options" is not registered for target.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRenderSQL_RefusesAnotherOwnersRoutineSettingOnSQLServer pins that SQL
// Server's routine owner accepts its own setting only.
func TestRenderSQL_RefusesAnotherOwnersRoutineSettingOnSQLServer(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(schemaext.NewFacets(&mysqlschema.DesiredColumnSettings{Charset: "utf8mb4"}))

	statements, _, err := builtin.GetOrderedCreateStatementsReportingOmissions(
		routineSchema(facets), platform.SQLServer, capability.ForDialect(platform.SQLServer))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*feature facet "ptah.run/mysql/column-settings" is not registered for target.*`)
	c.Assert(statements, qt.IsNil)
}
