package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
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

	converted := dbschematogo.ConvertDBSchemaToGoSchema(&dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{
			Name: "person", Schema: "sweep",
			Columns: []dbschematypes.DBColumn{{
				Name: "updated_at", DataType: "datetime", IsNullable: "YES",
				UpdateExpression: "CURRENT_TIMESTAMP",
			}},
		}},
	})

	c.Assert(converted.Fields, qt.HasLen, 1)
	c.Assert(converted.Fields[0].Name, qt.Equals, "updated_at")
	c.Assert(converted.Fields[0].UpdateExpression, qt.Equals, "CURRENT_TIMESTAMP")
}

// TestConvert_AColumnWithoutTheClauseCarriesNothing is the control: a field
// that always reported one would put `ON UPDATE` on every column a renderer
// wrote.
func TestConvert_AColumnWithoutTheClauseCarriesNothing(t *testing.T) {
	c := qt.New(t)

	converted := dbschematogo.ConvertDBSchemaToGoSchema(&dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{
			Name: "person", Schema: "sweep",
			Columns: []dbschematypes.DBColumn{{
				Name: "created_at", DataType: "timestamp", IsNullable: "NO",
			}},
		}},
	})

	c.Assert(converted.Fields, qt.HasLen, 1)
	c.Assert(converted.Fields[0].UpdateExpression, qt.Equals, "")
}
