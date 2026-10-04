package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_Changefeed writes a YDB table's changefeeds and their consumers
// as the annotations the parser reads them back from, so a schema read from a
// database and written as Go keeps its streams.
func TestRender_Changefeed(t *testing.T) {
	c := qt.New(t)
	changefeeds := []ast.ChangefeedSpec{
		{Name: "updates", Mode: "NEW_AND_OLD_IMAGES", Format: "JSON", VirtualTimestamps: true,
			ResolvedTimestamps: "PT10S", InitialScan: true, UserSIDs: true, SchemaChanges: true,
			TopicMinActivePartitions: 2, TopicAutoPartitioning: true, RetentionPeriod: "PT12H",
			Consumers: []ast.TopicConsumerSpec{
				{Name: "audit", Important: true, ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"}},
				{Name: "late", AvailabilityPeriod: "PT1H"},
			}},
		{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"},
	}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", PrimaryKey: []string{"id"}, Changefeeds: changefeeds}},
		Fields: []schemamodel.Field{{StructName: "Item", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true}},
	}

	files, err := goschematogo.Render(db, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	reparsed, err := goschema.ParseSource("schema.go", string(files[0].Data))

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains, `//ptah:schema:changefeed:consumer changefeed="updates" name="late"`)
	c.Assert(reparsed.Tables, qt.HasLen, 1)
	c.Assert(reparsed.Tables[0].Changefeeds, qt.DeepEquals, changefeeds)
}
