package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

func TestRenderIndexPropertiesRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name   string
		fields []string
		parts  []schemamodel.IndexPart
	}{
		{"columns", []string{"id"}, nil},
		{"expression", []string{"abs(id)"}, []schemamodel.IndexPart{{Expr: "abs(id)"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "events", StructName: "Event"}},
				Fields: []schemamodel.Field{{StructName: "Event", Name: "id", FieldName: "ID", Type: "BIGINT"}},
				Indexes: []schemamodel.Index{{Name: "by_id", StructName: "Event", TableName: "events", Fields: test.fields, Parts: test.parts,
					Overrides: map[string]map[string]string{
						"clickhouse": {"type.state": "default", "granularity": "18446744073709551615"},
						"other":      {"future": "", "escaped": "line\n\"quote\"\\path ${value} %{if true}"},
					},
				}},
			}
			rendered, err := atlashclrender.Render(db)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered.Diagnostics, qt.HasLen, 0)
			parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Indexes, qt.HasLen, 1)
			c.Assert(parsed.Indexes[0].Overrides, qt.DeepEquals, db.Indexes[0].Overrides)
			c.Assert(parsed.Indexes[0].Fields, qt.DeepEquals, test.fields)
			again, err := atlashclrender.Render(parsed)
			c.Assert(err, qt.IsNil)
			c.Assert(again.Data, qt.DeepEquals, rendered.Data)
		})
	}
}
