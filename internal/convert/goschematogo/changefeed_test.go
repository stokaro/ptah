package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_Changefeed writes a YDB table's changefeeds and their consumers
// as the annotations the parser reads them back from, so a schema read from a
// database and written as Go keeps its streams.
func TestRender_Changefeed(t *testing.T) {
	c := qt.New(t)
	changefeeds := []ydbschema.ChangefeedSpec{
		{Name: "updates", Mode: "NEW_AND_OLD_IMAGES", Format: "JSON", VirtualTimestamps: true,
			ResolvedTimestamps: "PT10S", InitialScan: true, UserSIDs: true, SchemaChanges: true,
			TopicMinActivePartitions: 2, TopicAutoPartitioning: true, RetentionPeriod: "PT12H",
			Consumers: []ydbtopic.ConsumerSpec{
				{Name: "audit", Important: true, ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"}},
				{Name: "late", AvailabilityPeriod: "PT1H"},
			}},
		{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"},
	}
	var objects []schemaext.Object
	for _, stream := range changefeeds {
		objects = append(objects, ydbschema.DesiredObject("", "items", stream))
	}
	db := &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, nil)),
		Tables:          []schemamodel.Table{{StructName: "Item", Name: "items", PrimaryKey: []string{"id"}}},
		Fields:          []schemamodel.Field{{StructName: "Item", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true}},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	reparsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", string(files[0].Data))

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains, `//ptah:schema:changefeed:consumer changefeed="updates" name="late"`)
	c.Assert(reparsed.Tables, qt.HasLen, 1)
	c.Assert(reparsed.FeatureObjects.Equal(db.FeatureObjects), qt.IsTrue)
}
