package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func TestTableCoverageBindingPreservesExplicitSchemasAndKnowledge(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	semantics := identifier.ForDialect("clickhouse")
	semantics.DefaultSchema = "connection_database"
	subject := objectidentity.NewBuilder(semantics).TableParts("other_database", "events")
	coverage := desiredTableCoverage(c, []schemaext.SubjectCoverage{{Kind: chschema.TableKind, Subject: subject,
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "incomplete declaration"}}})
	desired := &schemamodel.Database{FeatureCoverage: coverage, Tables: []schemamodel.Table{{Name: "events", Schema: "other_database", StructName: "Event"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64", Primary: true}}}
	options := &config.CompareOptions{Dialect: "clickhouse", IdentifierSemantics: &semantics}
	diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), desired, &catalog.Database{}, options, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.Features, qt.HasLen, 1)
	c.Assert(diagnostics.Features[0].Subject, qt.Equals, subject)
	c.Assert(diff.TablePreparation.Source[0].Desired.FeatureCoverage.SubjectRecords(), qt.DeepEquals, coverage.SubjectRecords())
	c.Assert(desired.FeatureCoverage, qt.DeepEquals, coverage)
}

func TestTableCoverageBindingRefusesCollidingClaims(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("clickhouse")
	builder := objectidentity.NewBuilder(semantics)
	var records []schemaext.SubjectCoverage
	for _, schema := range []string{"", "connection_database"} {
		records = append(records, schemaext.SubjectCoverage{Kind: chschema.TableKind, Subject: builder.TableParts(schema, "events"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	}
	desired := &schemamodel.Database{FeatureCoverage: desiredTableCoverage(c, records), Tables: []schemamodel.Table{{Name: "events", StructName: "Event"}}}
	semantics.DefaultSchema = "connection_database"
	info := catalog.ServerInfo{Dialect: "clickhouse", IdentifierSemantics: semantics}
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, &catalog.Database{}, info, nil, must.Must(builtin.New()))
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(diff, qt.IsNil)
	c.Assert(desired.FeatureCoverage.SubjectRecords(), qt.HasLen, 2)
}

func desiredTableCoverage(c *qt.C, subjects []schemaext.SubjectCoverage) schemaext.Coverage {
	c.Helper()
	models := must.Must(builtin.New()).Codecs().Definitions()
	model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
		return v.Kind == chschema.TableKind && v.Representation == schemaext.Desired
	})]
	coverage, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: model,
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only named tables were captured"}}}, subjects)
	c.Assert(err, qt.IsNil)
	return coverage
}
