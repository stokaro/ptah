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
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func TestRoutineCoverageUsesConnectionIdentifiers(t *testing.T) {
	for _, test := range []struct {
		name, declaredName, observedName, observedSchema, wantSchema string
	}{
		{"connection default", "f", "f", "", "app"},
		{"explicit schema", "public.f", "f", "public", "public"},
		{"quoted components", `"Mixed.Schema"."Mixed.Function"`, "Mixed.Function", "Mixed.Schema", `"Mixed.Schema"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
			declared := schemamodel.Function{Name: test.declaredName, Parameters: "value integer", Returns: "integer", Language: "sql", Volatility: "VOLATILE", Body: "SELECT value"}
			observed := catalog.Function{Name: test.observedName, Schema: test.observedSchema, Parameters: "value integer", Returns: "integer", Language: "sql", Volatility: "VOLATILE", Body: "SELECT value",
				Facets: must.Must(pgroutine.WithObservedProperties(schemaext.Facets{}, pgroutine.Properties{Strict: true}))}
			desired := &schemamodel.Database{Functions: []schemamodel.Function{declared}, FeatureCoverage: routineSubjectCoverage(c, schemaext.Desired, builder.Function(declared.Name, "integer"))}
			current := &catalog.Database{Functions: []catalog.Function{observed}, FeatureCoverage: routineSubjectCoverage(c, schemaext.Observed, builder.Function(observed.QualifiedName(), "integer"))}
			semantics := identifier.ForDialect("postgres")
			semantics.DefaultSchema = "app"
			info := catalog.ServerInfo{Dialect: "postgres", Schema: "app", IdentifierSemantics: semantics}

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, info, nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsAdded, qt.HasLen, 0)
			c.Assert(diff.FunctionsRemoved, qt.HasLen, 0)
			c.Assert(diff.FunctionsModified, qt.HasLen, 1)
			c.Assert(diff.FunctionsModified[0].FeatureChanges, qt.HasLen, 1)
			change := diff.FunctionsModified[0].FeatureChanges[0]
			c.Assert(change.Subject.Schema.Normalized, qt.Equals, test.wantSchema)
			c.Assert(change.Subject.Signature, qt.Equals, objectidentity.NewParts(objectidentity.Part{Source: "integer", Normalized: "integer"}))
			c.Assert(current.FeatureCoverage.SubjectRecords()[0].Subject, qt.Equals, builder.Function(observed.QualifiedName(), "integer"))
		})
	}
}

func TestRoutineCoverageRefusesCollidingClaims(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	desired := &schemamodel.Database{
		Functions: []schemamodel.Function{{Name: "f", Parameters: "value integer", Returns: "integer", Language: "sql", Body: "SELECT value"}},
		FeatureCoverage: routineSubjectCoverage(c, schemaext.Desired,
			builder.Function("f", "integer"), builder.Function("app.f", "integer")),
	}
	semantics := identifier.ForDialect("postgres")
	semantics.DefaultSchema = "app"
	info := catalog.ServerInfo{Dialect: "postgres", Schema: "app", IdentifierSemantics: semantics}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, &catalog.Database{}, info, nil, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(diff, qt.IsNil)
	c.Assert(desired.FeatureCoverage.SubjectRecords(), qt.HasLen, 2)
}

func routineSubjectCoverage(c *qt.C, representation schemaext.Representation, subjects ...objectidentity.ID) schemaext.Coverage {
	c.Helper()
	records := make([]schemaext.SubjectCoverage, 0, len(subjects))
	for _, subject := range subjects {
		records = append(records, schemaext.SubjectCoverage{Kind: pgroutine.PropertiesKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	}
	coverage, err := pgroutine.PropertiesCoverage(representation, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only named overloads are described"}, records)
	c.Assert(err, qt.IsNil)
	return coverage
}

func TestRoutineCoverageFollowsAnExcludedOwner(t *testing.T) {
	c := qt.New(t)
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Function("f", "")
	desired := &schemamodel.Database{
		Functions:       []schemamodel.Function{{Name: "f", Dialects: []string{"mysql"}, Returns: "integer", Language: "sql", Body: "SELECT 1"}},
		FeatureCoverage: routineSubjectCoverage(c, schemaext.Desired, subject),
	}

	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, &catalog.Database{}, "postgres", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(desired.FeatureCoverage.SubjectRecords(), qt.HasLen, 1)
}

func TestProcedureCoverageUsesConnectionIdentifiers(t *testing.T) {
	c := qt.New(t)
	desired, current := principalRoutines("reviewer", "auditor", "BEGIN RETURN 1; END")
	desired.Functions[0].Name, desired.Functions[0].Kind = "p", "procedure"
	current.Functions[0].Name, current.Functions[0].Kind, current.Functions[0].Schema = "p", "procedure", ""
	subject := objectidentity.NewBuilder(identifier.ForDialect("sqlserver")).Procedure("p", "")
	records := []schemaext.SubjectCoverage{{Kind: mssqlschema.RoutineOptionsKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}
	knowledge := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only named routines are described"}
	desired.FeatureCoverage = must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Desired, knowledge, records))
	current.FeatureCoverage = must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Observed, knowledge, records))
	semantics := identifier.ForDialect("sqlserver")
	semantics.DefaultSchema = "app"
	info := catalog.ServerInfo{Dialect: "sqlserver", Schema: "app", IdentifierSemantics: semantics}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, info, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.FunctionsModified, qt.HasLen, 1)
	c.Assert(diff.FunctionsModified[0].FeatureChanges, qt.HasLen, 1)
	c.Assert(diff.FunctionsModified[0].FeatureChanges[0].Subject.Schema.Normalized, qt.Equals, "app")
	c.Assert(diff.FunctionsModified[0].FeatureChanges[0].Subject.Kind, qt.Equals, objectidentity.KindProcedure)
}
