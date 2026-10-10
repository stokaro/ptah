package schemacensus

// White-box testing required: controls verify the census's private model selection and copy path.
// Public measurements cannot distinguish a lost fixture from a correctly empty
// schema, so these controls check the input before either surface measures it.

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
)

func TestFeatureCensusIncludesEveryBundledDesiredCodec(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	selected, err := schemaext.NewRegistry(featureCodecs()...)
	c.Assert(err, qt.IsNil)
	expected := slices.DeleteFunc(runtime.Codecs().Definitions(), func(definition schemaext.CodecIdentity) bool { return definition.Representation != schemaext.Desired })

	c.Assert(len(expected) > 0, qt.IsTrue)
	c.Assert(selected.Definitions(), qt.DeepEquals, expected)
	c.Assert(Fields(), qt.Contains, "ydbschema.ChangefeedSpec.TopicMinActivePartitions")
	c.Assert(Fields(), qt.Contains, "ydbschema.DesiredChangefeed.Spec")
}

func TestCensusCopiesPreserveFeatureObjectsAndCoverage(t *testing.T) {
	c := qt.New(t)
	source := tableChangefeedFixture()
	copyOfSchema := deepCopyDatabase(source)
	c.Assert(copyOfSchema.FeatureObjects.Equal(source.FeatureObjects), qt.IsTrue)
	c.Assert(copyOfSchema.FeatureCoverage.Equal(source.FeatureCoverage), qt.IsTrue)
	ablated := Ablate(source, "schemamodel.Field.Comment")
	c.Assert(ablated.FeatureObjects.Equal(source.FeatureObjects), qt.IsTrue)
	c.Assert(ablated.FeatureCoverage.Equal(source.FeatureCoverage), qt.IsTrue)
}

func TestCensusAblatesNestedOwnerValuesWithoutChangingTheSource(t *testing.T) {
	for _, test := range []struct {
		name       string
		field      string
		partitions uint64
		codecs     []string
	}{
		{name: "stream setting", field: "ydbschema.ChangefeedSpec.TopicMinActivePartitions", codecs: []string{"raw"}},
		{name: "nested consumer", field: "ydbtopic.ConsumerSpec.SupportedCodecs", partitions: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := tableChangefeedFixture()
			c.Assert(Populated(source, test.field), qt.IsTrue)
			ablated := Ablate(source, test.field)
			c.Assert(Populated(ablated, test.field), qt.IsFalse)
			values, err := ydbschema.DesiredChangefeeds(ablated.FeatureObjects, "", "t")
			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.HasLen, 1)
			c.Assert(values[0].TopicMinActivePartitions, qt.Equals, test.partitions)
			c.Assert(values[0].Consumers[0].SupportedCodecs, qt.DeepEquals, test.codecs)
			c.Assert(source.FeatureObjects.Equal(tableChangefeedFixture().FeatureObjects), qt.IsTrue)
			c.Assert(ablated.FeatureCoverage.Equal(source.FeatureCoverage), qt.IsTrue)
		})
	}
}

func TestCensusAblationRetainsFacetTargetScope(t *testing.T) {
	c := qt.New(t)
	source := tableClickHouseSettingsFixture()
	ablated := Ablate(source, "chschema.DesiredTable.TTL")
	c.Assert(ablated.Tables[0].Facets.TargetScope(chschema.TableKind), qt.DeepEquals, []string{"clickhouse"})
	c.Assert(Populated(source, "chschema.DesiredTable.TTL"), qt.IsTrue)
	c.Assert(Populated(ablated, "chschema.DesiredTable.TTL"), qt.IsFalse)
}
