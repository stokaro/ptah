package catalogfield_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/catalogfield"
)

// TestField_LeavesTheGoSourceToItsCaller pins the split this package exists for.
//
// A column the database reported was not parsed from Go source, so it has no
// struct and no Go field name. The converter that writes a document fills those
// in afterwards; the schema comparison, which has no Go source at all, does not
// (stokaro/ptah#2315).
func TestField_LeavesTheGoSourceToItsCaller(t *testing.T) {
	c := qt.New(t)

	field := catalogfield.Field(catalog.Column{Name: "email", DataType: "text"}, catalogfield.Options{})

	c.Assert(field.Name, qt.Equals, "email")
	c.Assert(field.StructName, qt.Equals, "",
		qt.Commentf("the struct a field belongs to is a parser artifact"))
	c.Assert(field.FieldName, qt.Equals, "",
		qt.Commentf("so is the Go field name"))
}

// TestField_PrimaryIsTheColumnsOwnKeyOnly covers the one option that decides a
// field's value rather than adding to it.
//
// A column carrying IsPrimaryKey under a TABLE-level primary key would render
// the key twice: once inline and once as the constraint.
func TestField_PrimaryIsTheColumnsOwnKeyOnly(t *testing.T) {
	tests := []struct {
		name    string
		covered bool
		want    bool
		why     string
	}{
		{
			name:    "a column-level key stands",
			covered: false,
			want:    true,
			why:     "no table-level constraint names it, so the column carries the key",
		},
		{
			name:    "a table-level key wins",
			covered: true,
			want:    false,
			why:     "the constraint renders the key, and rendering it inline too would state it twice",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			field := catalogfield.Field(
				catalog.Column{Name: "id", DataType: "integer", IsPrimaryKey: true},
				catalogfield.Options{CoveredByTablePrimaryKey: test.covered},
			)

			c.Assert(field.Primary, qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestField_TheDefaultKeepsItsKind covers the rule a catalog string is routed by.
//
// The catalog reports one string where the model has a literal value and an
// expression, and which slot it lands in decides whether the renderer quotes
// it. A default of now() in the value slot renders as the literal text.
func TestField_TheDefaultKeepsItsKind(t *testing.T) {
	tests := []struct {
		name      string
		reported  string
		wantValue string
		wantExpr  string
	}{
		{name: "an expression stays an expression", reported: "now()", wantExpr: "now()"},
		{name: "a quoted literal stays a value", reported: "'draft'", wantValue: "'draft'"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reported := test.reported
			field := catalogfield.Field(
				catalog.Column{Name: "state", DataType: "text", ColumnDefault: &reported},
				catalogfield.Options{},
			)

			c.Assert(field.Default, qt.Equals, test.wantValue)
			c.Assert(field.DefaultExpr, qt.Equals, test.wantExpr)
		})
	}
}

// TestType_PrefersTheServersOwnSpelling covers the ordering #1138 measured.
//
// An array column's DataType is the bare category ARRAY, a word no engine
// accepts as a type, and a domain over a user-defined base reports
// USER-DEFINED with udt_name naming the BASE rather than the domain. Reading
// FormattedType first is what keeps both from being flattened.
func TestType_PrefersTheServersOwnSpelling(t *testing.T) {
	tests := []struct {
		name   string
		column catalog.Column
		want   string
		why    string
	}{
		{
			name:   "the server's spelling wins over the category",
			column: catalog.Column{DataType: "ARRAY", FormattedType: "text[]"},
			want:   "text[]",
			why:    "ARRAY is a category, and no engine accepts it as a type",
		},
		{
			name:   "a domain over a user-defined base is not flattened to the base",
			column: catalog.Column{DataType: "USER-DEFINED", UDTName: "cube", FormattedType: "point3d"},
			want:   "point3d",
			why:    "udt_name names the base, and the base drops the domain's CHECK",
		},
		{
			name:   "a user-defined type with no formatted spelling falls back to udt_name",
			column: catalog.Column{DataType: "USER-DEFINED", UDTName: "mood"},
			want:   "mood",
			why:    "an enum column reaches the model by its type name",
		},
		{
			name:   "a sized type is rebuilt from its precision",
			column: catalog.Column{DataType: "numeric", NumericPrecision: new(10), NumericScale: new(2)},
			want:   "NUMERIC(10,2)",
			why:    "the catalog reports the parts, and the declaration writes them together",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(catalogfield.Type(test.column), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}
