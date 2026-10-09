package atlasreport_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlasreport"
)

// An inspected ClickHouse skipping index carries its type and granularity as
// the owner's facet. The HCL document writes them as the index's ClickHouse
// platform properties, so reading the document back and decoding it restores
// the same settings instead of a loss diagnostic.
func TestSchemaInspectHCLExportsSkippingIndexSettings(t *testing.T) {
	c := qt.New(t)
	settings := (&chschema.ObservedIndex{IndexType: "bloom_filter(0.01)", Granularity: 4}).Desired()
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event"}},
		Fields: []schemamodel.Field{{Name: "payload", StructName: "Event", Type: "String"}},
		Indexes: []schemamodel.Index{{
			Name: "idx_payload", StructName: "Event", TableName: "events", Fields: []string{"payload"},
			Facets: must.Must(must.Must(schemaext.NewFacets(settings)).WithTargetScope(chschema.IndexKind, "clickhouse")),
		}},
	}
	var diagnostics strings.Builder
	report := newInspectReport(c, db, &catalog.Database{}, catalog.ServerInfo{Dialect: "clickhouse"}, &diagnostics, atlasreport.SchemaInspectReportOptions{})

	document, err := report.MarshalHCL()

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.String(), qt.Equals, "")
	c.Assert(document, qt.Contains, `platform "clickhouse"`)
	parsed, err := atlashcl.Parse([]byte(document), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Indexes, qt.HasLen, 1)
	c.Assert(parsed.Indexes[0].Overrides, qt.DeepEquals, map[string]map[string]string{"clickhouse": {"type": "bloom_filter(0.01)", "granularity": "4"}})
	decoded, err := schemaproperties.DecodeIndexes(t.Context(), parsed, "clickhouse", inspectRuntime(c))
	c.Assert(err, qt.IsNil)
	value, found, err := schemaext.FacetAs[*chschema.DesiredIndex](decoded.Indexes[0].Facets, chschema.IndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value, qt.DeepEquals, settings)
	c.Assert(db.Indexes[0].Overrides, qt.IsNil)
}
