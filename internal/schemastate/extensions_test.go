package schemastate_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
)

// The extension an index cannot be built without is the one fact about it that
// its own text does not carry. Measured on PostgreSQL 17.10,
// `CREATE INDEX t_gin ON t USING gin (n int4_ops)` over an integer column needs
// btree_gin and is stored, and rendered, as `USING gin (n)`, because PostgreSQL
// prints an operator class exactly when it is NOT the default. The reader
// resolves it against pg_depend, and a state that dropped the answer leaves
// every later stage to guess it from `gin` -- which pins btree_gin to indexes
// that do not need it, since tsvector, jsonb and array columns have core GIN
// classes (stokaro/ptah#1286, stokaro/ptah#1663).
//
// Both adapters are asked, and both payloads, because the fact rides on the
// object and the adapters are hand-written field lists: an index's own, and the
// one a constraint's backing index needs, which is a separate field precisely
// because the backing index is not an object of the entity model.

func TestTheExtensionAnIndexNeedsSurvivesBothAdapters(t *testing.T) {
	tests := []struct {
		name  string
		build func() (*schemastate.State, error)
	}{
		{
			name: "a read resolved it",
			build: func() (*schemastate.State, error) {
				current := catalogWidget(
					[]catalog.Column{{Name: "n", DataType: "integer", IsNullable: "YES"}},
					nil,
				)
				current.Indexes = []catalog.Index{{
					Name: "widget_gin", TableName: "widget", Schema: "public",
					Columns: []string{"n"}, RequiresExtensions: []string{"btree_gin"},
				}}
				return schemastate.FromCatalog(
					current, "postgres", identifier.ForDialect("postgres"))
			},
		},
		{
			name: "a description carries it",
			build: func() (*schemastate.State, error) {
				description := withIndex(
					describedWidget([]schemamodel.Field{
						{StructName: "Widget", Name: "n", Type: "int", Nullable: true},
					}),
					schemamodel.Index{
						StructName: "Widget", Name: "widget_gin", Fields: []string{"n"},
						RequiresExtensions: []string{"btree_gin"},
					},
				)
				return schemastate.FromDescription(
					description, "postgres", identifier.ForDialect("postgres"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			state, err := test.build()

			c.Assert(err, qt.IsNil)
			c.Assert(indexExtensions(state), qt.DeepEquals, [][]string{{"btree_gin"}})
		})
	}
}

func TestTheExtensionAConstraintsIndexNeedsSurvivesBothAdapters(t *testing.T) {
	tests := []struct {
		name  string
		build func() (*schemastate.State, error)
	}{
		{
			name: "a read resolved it",
			build: func() (*schemastate.State, error) {
				return schemastate.FromCatalog(
					catalogWidget(
						[]catalog.Column{{Name: "room", DataType: "integer", IsNullable: "YES"}},
						[]catalog.Constraint{{
							Name: "ex_widget_room", TableName: "widget", Schema: "public",
							Type: "EXCLUDE", UsingMethod: new("gist"),
							ExcludeElements:    new("room WITH ="),
							RequiresExtensions: []string{"btree_gist"},
						}},
					),
					"postgres", identifier.ForDialect("postgres"))
			},
		},
		{
			name: "a description carries it",
			build: func() (*schemastate.State, error) {
				return schemastate.FromDescription(
					withConstraint(
						describedWidget([]schemamodel.Field{
							{StructName: "Widget", Name: "room", Type: "int", Nullable: true},
						}),
						schemamodel.Constraint{
							StructName: "Widget", Name: "ex_widget_room", Type: "EXCLUDE",
							UsingMethod: "gist", ExcludeElements: "room WITH =",
							RequiresExtensions: []string{"btree_gist"},
						},
					),
					"postgres", identifier.ForDialect("postgres"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			state, err := test.build()

			c.Assert(err, qt.IsNil)
			c.Assert(constraintExtensions(state), qt.DeepEquals, [][]string{{"btree_gist"}})
		})
	}
}

// indexExtensions is the extension list of every index a state carries.
func indexExtensions(state *schemastate.State) [][]string {
	extensions := make([][]string, 0)
	for _, object := range state.OfKind(objectidentity.KindIndex) {
		extensions = appendIndexExtensions(extensions, object)
	}
	return extensions
}

// constraintExtensions is the same for the clause constraints, whose backing
// index is not an object of its own and so carries its requirement here.
func constraintExtensions(state *schemastate.State) [][]string {
	extensions := make([][]string, 0)
	for _, object := range state.OfKind(objectidentity.KindConstraint) {
		extensions = appendConstraintExtensions(extensions, object)
	}
	return extensions
}

// appendIndexExtensions keeps the loop above free of the conditional the
// repository's test style refuses inside a test, with thunks rather than values
// because a map literal evaluates both branches.
func appendIndexExtensions(extensions [][]string, object schemastate.Object) [][]string {
	return map[bool]func() [][]string{
		false: func() [][]string { return extensions },
		true:  func() [][]string { return append(extensions, object.Index.RequiresExtensions) },
	}[object.Index != nil && len(object.Index.RequiresExtensions) > 0]()
}

func appendConstraintExtensions(extensions [][]string, object schemastate.Object) [][]string {
	return map[bool]func() [][]string{
		false: func() [][]string { return extensions },
		true: func() [][]string {
			return append(extensions, object.Constraint.RequiresExtensions)
		},
	}[object.Constraint != nil && len(object.Constraint.RequiresExtensions) > 0]()
}
