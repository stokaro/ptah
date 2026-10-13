package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/routineargs"
	"ptah.run/migration/schemadiff"
)

func parameterRoutine(principal, parameters string) *schemamodel.Database {
	return &schemamodel.Database{Functions: []schemamodel.Function{{Name: "dbo.p", Kind: "procedure", Parameters: parameters, Language: "sql", Security: "DEFINER", Volatility: "VOLATILE", Body: "BEGIN SELECT @a; END", Facets: must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{Principal: principal, Recompile: true}))}}, FeatureCoverage: must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
}
func heldParameterRoutine() *catalog.Database {
	return &catalog.Database{Functions: []catalog.Function{{Schema: "dbo", Name: "p", Kind: "procedure", Parameters: "@a int", Language: "sql", Security: "DEFINER", Volatility: "VOLATILE", Body: "BEGIN SELECT @a; END", Facets: must.Must(mssqlschema.WithObservedRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{Principal: "auditor", Recompile: true}))}}, FeatureCoverage: must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))}
}

// Optional AS has no effect on comparison or the identity used for owner facets.
func TestCompareSQLServerRoutineParameters(t *testing.T) {
	c := qt.New(t)
	desired := parameterRoutine("auditor", "@a AS INT")
	current := heldParameterRoutine()
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "sqlserver", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.FunctionsModified, qt.HasLen, 0)
	c.Assert(diff.FunctionsAdded, qt.HasLen, 0)
	c.Assert(diff.FunctionsRemoved, qt.HasLen, 0)
	c.Assert(desired.Functions[0].Parameters, qt.Equals, "@a AS INT")
	c.Assert(current.Functions[0].Parameters, qt.Equals, "@a int")
}

func TestCompareSQLServerRoutineParameters_KeepsPrincipalChanges(t *testing.T) {
	c := qt.New(t)
	diff, err := schemadiff.CompareWithDialect(t.Context(), parameterRoutine("reviewer", "@a AS INT"), heldParameterRoutine(), "sqlserver", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.FunctionsModified, qt.HasLen, 1)
	c.Assert(diff.FunctionsModified[0].Changes, qt.HasLen, 0)
	c.Assert(diff.FunctionsModified[0].FeatureChanges, qt.HasLen, 1)
	c.Assert(diff.FunctionsModified[0].FeatureChanges[0].Value, qt.DeepEquals, &mssqldiff.RoutineOptions{Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor", Recompile: true}, After: &mssqlschema.DesiredRoutineOptions{Principal: "reviewer", Recompile: true}})
}

// Per-routine knowledge limits keep the same meaning after parameter lowering.
func TestCompareSQLServerRoutineParameters_KeepsCoverage_FailurePath(t *testing.T) {
	c := qt.New(t)
	desired := parameterRoutine("auditor", "@a AS INT")
	desired.Functions[0].Facets = schemaext.Facets{}
	subject := routineargs.Subject(objectidentity.NewBuilder(identifier.ForDialect("sqlserver")), "procedure", "dbo.p", "@a AS INT")
	desired.FeatureCoverage = must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{Kind: mssqlschema.RoutineOptionsKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "source cannot describe the options"}}}))
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, heldParameterRoutine(), "sqlserver", must.Must(builtin.New()))
	c.Assert(err, qt.ErrorMatches, `.*schema comparison is incomplete:.*source cannot describe the options`)
	c.Assert(diff, qt.IsNil)
}

func TestCompareSQLServerRoutineParameters_KeepsTypeChanges(t *testing.T) {
	for _, parameters := range []string{"@a AS BIGINT", "@a AS DECIMAL(10,2)", "@a AS NVARCHAR(50)"} {
		t.Run(parameters, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDialect(t.Context(), parameterRoutine("auditor", parameters), heldParameterRoutine(), "sqlserver", must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsModified, qt.HasLen, 1)
			c.Assert(diff.FunctionsModified[0].Changes["parameters"], qt.Not(qt.Equals), "")
		})
	}
}
