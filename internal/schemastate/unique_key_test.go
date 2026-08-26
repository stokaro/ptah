package schemastate_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
)

// A uniqueness guarantee reaches Ptah under four spellings, and a reader that
// understood one of them described a table whose key it could not see. The
// adapters are tested directly rather than through a comparison, because the
// comparison consults the DESIRED side only: a catalog's guarantees would
// otherwise be read by nothing and a gate over them would report a pass while
// examining an empty list (stokaro/ptah#1662).

func TestFromDescriptionReadsEverySpellingOfAUniquenessGuarantee(t *testing.T) {
	tests := []struct {
		name        string
		description *goschema.Database
		wantColumns []string
	}{
		{
			name: "a field's own flag",
			description: describedWidget(
				[]goschema.Field{{StructName: "Widget", Name: "code", Type: "text", Unique: true}}),
			wantColumns: []string{"code"},
		},
		{
			name: "a field's primary key",
			description: describedWidget(
				[]goschema.Field{{StructName: "Widget", Name: "id", Type: "int", Primary: true}}),
			wantColumns: []string{"id"},
		},
		{
			name: "a table-level composite key",
			description: withTableKey(describedWidget([]goschema.Field{
				{StructName: "Widget", Name: "tenant", Type: "int"},
				{StructName: "Widget", Name: "id", Type: "int"},
			}), []string{"tenant", "id"}),
			wantColumns: []string{"tenant", "id"},
		},
		{
			name: "a named UNIQUE constraint",
			description: withConstraint(describedWidget([]goschema.Field{
				{StructName: "Widget", Name: "tenant", Type: "int"},
				{StructName: "Widget", Name: "code", Type: "text"},
			}), goschema.Constraint{
				StructName: "Widget", Name: "uq_widget_scope",
				Type: "UNIQUE", Columns: []string{"tenant", "code"},
			}),
			wantColumns: []string{"tenant", "code"},
		},
		{
			name: "a unique index",
			description: withIndex(describedWidget([]goschema.Field{
				{StructName: "Widget", Name: "tenant", Type: "int"},
				{StructName: "Widget", Name: "code", Type: "text"},
			}), goschema.Index{
				StructName: "Widget", Name: "idx_widget_scope",
				Fields: []string{"tenant", "code"}, Unique: true,
			}),
			wantColumns: []string{"tenant", "code"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			state, err := schemastate.FromDescription(
				test.description, "postgres", identifier.ForDialect("postgres"))

			c.Assert(err, qt.IsNil)
			c.Assert(uniqueKeyColumns(state), qt.DeepEquals, [][]string{test.wantColumns})
		})
	}
}

// TestFromDescriptionRecordsNoGuaranteeWhereNoneIsDeclared is the control the
// rows above need. An adapter that produced a key for every table would satisfy
// all five and would tell the foreign-key gate that every reference is legal.
func TestFromDescriptionRecordsNoGuaranteeWhereNoneIsDeclared(t *testing.T) {
	c := qt.New(t)

	state, err := schemastate.FromDescription(
		describedWidget([]goschema.Field{{StructName: "Widget", Name: "code", Type: "text"}}),
		"postgres", identifier.ForDialect("postgres"))

	c.Assert(err, qt.IsNil)
	c.Assert(uniqueKeyColumns(state), qt.HasLen, 0)
}

// TestFromCatalogReadsBothSpellingsAServerUses pins the other adapter. A server
// reports a single-column key on the column row and a composite one as a
// constraint row, and a reader that took only one of them described a table
// whose key it could not see.
func TestFromCatalogReadsBothSpellingsAServerUses(t *testing.T) {
	tests := []struct {
		name        string
		catalog     *catalog.Database
		wantColumns [][]string
	}{
		{
			name: "the column's own flag",
			catalog: catalogWidget([]catalog.Column{
				{Name: "code", DataType: "text", IsNullable: "NO", IsUnique: true},
			}, nil),
			wantColumns: [][]string{{"code"}},
		},
		{
			name: "a constraint row",
			catalog: catalogWidget([]catalog.Column{
				{Name: "tenant", DataType: "integer", IsNullable: "NO"},
				{Name: "code", DataType: "text", IsNullable: "NO"},
			}, []catalog.Constraint{{
				Name: "uq_widget_scope", TableName: "widget", Schema: "public",
				Type: "UNIQUE", ColumnNames: []string{"tenant", "code"},
			}}),
			wantColumns: [][]string{{"tenant", "code"}},
		},
		{
			name: "no guarantee at all",
			catalog: catalogWidget([]catalog.Column{
				{Name: "code", DataType: "text", IsNullable: "YES"},
			}, nil),
			wantColumns: make([][]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			state, err := schemastate.FromCatalog(
				test.catalog, "postgres", identifier.ForDialect("postgres"))

			c.Assert(err, qt.IsNil)
			c.Assert(uniqueKeyColumns(state), qt.DeepEquals, test.wantColumns)
		})
	}
}

// uniqueKeyColumns is every uniqueness guarantee a state carries, by the columns
// it covers, in the order the state holds them.
func uniqueKeyColumns(state *schemastate.State) [][]string {
	columns := make([][]string, 0)
	for _, object := range state.OfKind(objectidentity.KindConstraint) {
		columns = appendUniqueKey(columns, object)
	}
	return columns
}

// appendUniqueKey keeps the loop above free of the conditional the repository's
// test style refuses inside a test.
//
// The branches are thunks, not values. A map literal evaluates BOTH, so a guard
// written as a value dereferences the nil it is guarding against -- which is
// what happened the moment an object of another family started appearing in the
// same list.
func appendUniqueKey(columns [][]string, object schemastate.Object) [][]string {
	return map[bool]func() [][]string{
		false: func() [][]string { return columns },
		true:  func() [][]string { return append(columns, object.UniqueKey.Columns) },
	}[object.UniqueKey != nil]()
}

func describedWidget(fields []goschema.Field) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Widget", Name: "widget"}},
		Fields: fields,
	}
}

func withTableKey(description *goschema.Database, key []string) *goschema.Database {
	description.Tables[0].PrimaryKey = key
	return description
}

func withConstraint(description *goschema.Database, constraint goschema.Constraint) *goschema.Database {
	description.Constraints = append(description.Constraints, constraint)
	return description
}

func withIndex(description *goschema.Database, index goschema.Index) *goschema.Database {
	description.Indexes = append(description.Indexes, index)
	return description
}

func catalogWidget(
	columns []catalog.Column,
	constraints []catalog.Constraint,
) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "widget", Schema: "public", Columns: columns},
		},
		Constraints: constraints,
	}
}
