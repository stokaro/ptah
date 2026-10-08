package features_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
)

func TestSchemaConversion_ChangefeedRoundTripPreservesConsumersAndUnknownState(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	feed := ydbschema.ChangefeedSpec{Name: "updates", Mode: "NEW_IMAGE", Format: "JSON",
		VirtualTimestamps: true, ResolvedTimestamps: "PT5S", InitialScan: true, UserSIDs: true,
		SchemaChanges: true, TopicMinActivePartitions: 2, TopicAutoPartitioning: true,
		RetentionPeriod: "PT2H", Disabled: true, Consumers: []ast.TopicConsumerSpec{{Name: "worker",
			Important: true, ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"}, AvailabilityPeriod: "PT3H"}}}
	objects, err := schemaext.NewObjects(ydbschema.ObservedObject("shop", "items", feed))
	c.Assert(err, qt.IsNil)
	unknown := ydbschema.ChangefeedRef("shop", "items", "unread")
	knowledge := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unmodeled server setting"}
	coverage, err := ydbschema.ChangefeedCoverage(schemaext.Observed, []schemaext.SubjectCoverage{{Kind: ydbschema.ChangefeedKind, Subject: unknown, Knowledge: knowledge}})
	c.Assert(err, qt.IsNil)
	current := &catalog.Database{FeatureObjects: objects, FeatureCoverage: coverage, Tables: []catalog.Table{{Name: "items", Schema: "shop", Columns: []catalog.Column{{Name: "id", DataType: "Int64", IsNullable: "NO", IsPrimaryKey: true}}}}}
	declared, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), current, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	got, err := ydbschema.DesiredChangefeeds(declared.FeatureObjects, "shop", "items")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []ydbschema.ChangefeedSpec{feed})
	c.Assert(declared.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, unknown), qt.Equals, knowledge)
	roundTrip, err := goschematodb.ToDBSchema(t.Context(), declared, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	got, err = ydbschema.ObservedChangefeeds(roundTrip.FeatureObjects, "shop", "items")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []ydbschema.ChangefeedSpec{feed})
	c.Assert(roundTrip.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, unknown), qt.Equals, knowledge)
	got[0].Consumers[0].SupportedCodecs[0] = "changed"
	original, err := ydbschema.ObservedChangefeeds(current.FeatureObjects, "shop", "items")
	c.Assert(err, qt.IsNil)
	c.Assert(original[0].Consumers[0].SupportedCodecs, qt.DeepEquals, []string{"raw", "gzip"})
}
