package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_CarriesTheWidthIntoTheDescription is the second half of
// stokaro/ptah#2034, and the half that reached a user.
//
// A width the read carries has to reach the DOCUMENT, which is a different
// path from the one a comparison takes: `ptah schema inspect` renders from the
// IR this conversion builds. The bit families were missing from the size list
// here as well as from the read's own, so the document said `bit` for a
// `bit(4)` column and `bit varying` for a `bit varying(8)` one. Replayed into a
// fresh database that is `bit(1)` and an unlimited `bit varying`, measured on
// PostgreSQL 17.11.
func TestConvert_CarriesTheWidthIntoTheDescription(t *testing.T) {
	width := 8
	precision, scale := 12, 4
	tests := []struct {
		name     string
		column   catalog.Column
		wantType string
	}{
		{
			name:     "varchar",
			column:   catalog.Column{Name: "c", DataType: "character varying", CharacterMaxLength: &width},
			wantType: "VARCHAR(8)",
		},
		{
			name:     "char",
			column:   catalog.Column{Name: "c", DataType: "character", CharacterMaxLength: &width},
			wantType: "CHAR(8)",
		},
		{
			name:     "bit",
			column:   catalog.Column{Name: "c", DataType: "bit", CharacterMaxLength: &width},
			wantType: "BIT(8)",
		},
		{
			// Lower case, because this one is not a modeled HCL type name: it
			// reaches the document through sql() carrying the case it has
			// here, and that binary's type names are case sensitive.
			name:     "bit varying",
			column:   catalog.Column{Name: "c", DataType: "bit varying", CharacterMaxLength: &width},
			wantType: "bit varying(8)",
		},
		{
			name: "numeric",
			column: catalog.Column{
				Name: "c", DataType: "numeric",
				NumericPrecision: &precision, NumericScale: &scale,
			},
			wantType: "NUMERIC(12,4)",
		},
		{
			// The control: a type that keeps no width must not grow one.
			name:     "a type with no width",
			column:   catalog.Column{Name: "c", DataType: "integer"},
			wantType: "integer",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted := dbschematogo.ConvertDBSchemaToGoSchema(&catalog.Database{
				Tables: []catalog.Table{{
					Name: "probe", Schema: "public", Columns: []catalog.Column{test.column},
				}},
			}, "")

			c.Assert(convertedFieldTypes(converted.Fields), qt.DeepEquals, []string{test.wantType})
		})
	}
}

// convertedFieldTypes names the types the conversion produced, so the
// assertion reads as one value rather than as an index into a slice that may
// be empty.
func convertedFieldTypes(fields []schemamodel.Field) []string {
	types := make([]string, 0, len(fields))
	for _, field := range fields {
		types = append(types, field.Type)
	}
	return types
}
