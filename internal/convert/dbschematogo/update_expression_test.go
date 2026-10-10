package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_TheUpdateExpressionSurvivesTheConversion is the second half of
// the carry.
//
// The reader can take the clause off EXTRA and it still reaches no renderer
// unless the conversion carries it, and the conversion lists its fields by
// hand. A field missing from that list is a field that arrives empty with
// nothing failing (stokaro/ptah#1215).
func TestConvert_TheUpdateExpressionSurvivesTheConversion(t *testing.T) {
	c := qt.New(t)

	converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
		Tables: []catalog.Table{{
			Name: "person", Schema: "sweep",
			Columns: []catalog.Column{{
				Name: "updated_at", DataType: "datetime", IsNullable: "YES",
				Facets: must.Must(mysqlschema.WithObservedColumnSettings(schemaext.Facets{},
					mysqlschema.ColumnSettings{OnUpdate: "CURRENT_TIMESTAMP"})),
			}},
		}},
	}, "mysql", must.Must(builtin.New())))

	c.Assert(converted.Fields, qt.HasLen, 1)
	c.Assert(converted.Fields[0].Name, qt.Equals, "updated_at")
	declared, found, err := schemaext.FacetAs[*mysqlschema.DesiredColumnSettings](converted.Fields[0].Facets, mysqlschema.ColumnSettingsKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(*declared, qt.Equals, mysqlschema.DesiredColumnSettings{OnUpdate: "CURRENT_TIMESTAMP"})
}

// TestConvert_AColumnWithoutTheClauseCarriesNothing is the control: a field
// that always reported one would put `ON UPDATE` on every column a renderer
// wrote.
func TestConvert_AColumnWithoutTheClauseCarriesNothing(t *testing.T) {
	c := qt.New(t)

	converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
		Tables: []catalog.Table{{
			Name: "person", Schema: "sweep",
			Columns: []catalog.Column{{
				Name: "created_at", DataType: "timestamp", IsNullable: "NO",
			}},
		}},
	}, "mysql", must.Must(builtin.New())))

	c.Assert(converted.Fields, qt.HasLen, 1)
	c.Assert(converted.Fields[0].Facets.IsZero(), qt.IsTrue)
}
