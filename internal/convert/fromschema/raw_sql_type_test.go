package fromschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// The AST has to carry the sql() marker, not just the reduced type.
//
// ColumnNode.Type stays the reduced SQL text so every dialect renderer keeps
// emitting valid DDL -- putting `sql("USER_DEFINED")` in Type instead was
// measured and produces `CREATE TABLE "main"."t" ("c" sql("USER_DEFINED"))`,
// which sqlite rejects and which makes `ptah-compat schema apply` exit 1 on a
// file the pinned Atlas community binary v1.3.0 applies at exit 0. The marker is
// how a consumer that writes Atlas HCL back out (issue #1138) tells the two
// spellings apart without the IR carrying HCL syntax.
//
// Without the fix, wantRawSQL is false on every row.
func TestFromFieldCarriesTheSQLRawExpressionTypeMarker(t *testing.T) {
	tests := []struct {
		name       string
		field      goschema.Field
		platform   string
		wantType   string
		wantRawSQL bool
	}{
		{
			name:       "marked type",
			field:      goschema.Field{Name: "c", Type: "USER_DEFINED", TypeRawSQL: true},
			wantType:   "USER_DEFINED",
			wantRawSQL: true,
		},
		{
			// Negative control: an ordinary type must not acquire the marker.
			name:       "unmarked type",
			field:      goschema.Field{Name: "c", Type: "USER_DEFINED"},
			wantType:   "USER_DEFINED",
			wantRawSQL: false,
		},
		{
			// A platform override replaces the type with a spelling the sql()
			// call never carried, so the marker must not survive: writing it
			// back would attribute the escape hatch to the override's text.
			name: "platform override replaces the type",
			field: goschema.Field{
				Name:       "c",
				Type:       "USER_DEFINED",
				TypeRawSQL: true,
				Overrides:  map[string]map[string]string{"mysql": {"type": "JSON"}},
			},
			platform:   "mysql",
			wantType:   "JSON",
			wantRawSQL: false,
		},
		{
			// An override that does not touch the type leaves the marker
			// alone -- otherwise any override at all would silently drop it.
			name: "platform override leaves the type alone",
			field: goschema.Field{
				Name:       "c",
				Type:       "USER_DEFINED",
				TypeRawSQL: true,
				Overrides:  map[string]map[string]string{"mysql": {"comment": "hello"}},
			},
			platform:   "mysql",
			wantType:   "USER_DEFINED",
			wantRawSQL: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := fromschema.FromField(test.field, nil, test.platform)
			c.Assert(column.Type, qt.Equals, test.wantType)
			c.Assert(column.TypeRawSQL, qt.Equals, test.wantRawSQL)

			withoutFK := fromschema.FromFieldWithoutForeignKeys(test.field, nil, test.platform)
			c.Assert(withoutFK.Type, qt.Equals, test.wantType)
			c.Assert(withoutFK.TypeRawSQL, qt.Equals, test.wantRawSQL)
		})
	}
}
