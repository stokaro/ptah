package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
)

// TestTheExtensionAnIndexNeedsIsNotCompared pins the half of the fact that is
// about NOT acting on it.
//
// A read resolves the extension an index needs against pg_depend; a description
// that was written by hand names none. They describe the same index, so
// comparing the two answers would plan a rebuild on every run for an index
// nobody changed -- and a rebuild drops the index first.
//
// The second row is the control: the same pair with a real difference still
// plans one change, so the first row is not passing because the fixture cannot
// produce a change at all.
func TestTheExtensionAnIndexNeedsIsNotCompared(t *testing.T) {
	tests := []struct {
		name           string
		declaredFields []string
		wantChanges    int
	}{
		{
			name:           "only the extension differs",
			declaredFields: []string{"n"},
			wantChanges:    0,
		},
		{
			name:           "the key differs as well",
			declaredFields: []string{"n", "code"},
			wantChanges:    1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description := describedTable(
				goschema.Field{StructName: "Widget", Name: "n", Type: "int", Nullable: true},
				goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
			)
			description.Indexes = append(description.Indexes, goschema.Index{
				StructName: "Widget", Name: "widget_gin", Fields: test.declaredFields,
			})
			currentCatalog := catalogTable(
				catalog.Column{Name: "n", DataType: "integer", IsNullable: "YES"},
				catalog.Column{Name: "code", DataType: "text", IsNullable: "YES"},
			)
			currentCatalog.Indexes = []catalog.Index{{
				Name: "widget_gin", TableName: "widget", Schema: "public",
				Columns: []string{"n"}, RequiresExtensions: []string{"btree_gin"},
			}}

			changes := changesFor(c, description, currentCatalog)

			c.Assert(changes, qt.HasLen, test.wantChanges)
		})
	}
}
