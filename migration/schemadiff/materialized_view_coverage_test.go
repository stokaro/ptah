package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A local view and its coverage must bind to the connection's database
// together. Otherwise an unread storage clause or refresh schedule disappears
// before the replacement checks whether it would lose that setting.
func TestMaterializedViewCoverageKeepsLocalLimits(t *testing.T) {
	for _, kind := range []schemaext.Kind{chschema.RefreshKind, chschema.ViewStorageKind} {
		for _, schema := range []string{"", "analytics"} {
			t.Run(string(kind)+"/"+schema, func(t *testing.T) {
				c := qt.New(t)
				runtime := must.Must(builtin.New())
				models := runtime.Codecs().Definitions()
				model := models[slices.IndexFunc(models, func(model schemaext.CodecIdentity) bool {
					return model.Kind == kind && model.Representation == schemaext.Observed
				})]
				subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, schema, "daily")
				coverage, err := schemaext.NewCoverage(schemaext.Observed,
					[]schemaext.KindCoverage{{Model: model, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})}},
					[]schemaext.SubjectCoverage{{Kind: kind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unreadable"}}})
				c.Assert(err, qt.IsNil)
				current := &catalog.Database{MatViews: []catalog.MaterializedView{{Schema: schema, Name: "daily", Body: "SELECT 1 AS c"}}, FeatureCoverage: coverage}
				desired := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "daily", Body: "SELECT 2 AS c"}}}

				diff, err := schemadiff.CompareWithOptions(t.Context(), desired, current, liveOptions(), runtime)

				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err, qt.ErrorMatches, `.*materialized view daily is replaced, and its ".*" could not be read \(unreadable\).*`)
				c.Assert(diff, qt.IsNil)
				c.Assert(current.FeatureCoverage.SubjectRecords()[0].Subject, qt.Equals, subject)
			})
		}
	}
}
