package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableref"
)

// schemaWidgetOwner declares //ptah:schema:widget with a schema attribute and
// records, in received, the schema each declaration it decodes carries.
func schemaWidgetOwner(c *qt.C, received *[]string) annotation.Set {
	c.Helper()
	set, err := annotation.NewSet(annotation.Extension{
		Owner: "example.org/widget",
		Directives: []annotation.Directive{{Name: "ptah:schema:widget", Scopes: []annotation.Scope{annotation.ScopeStruct},
			Attributes: []annotation.Attribute{
				{Name: "table", Value: "string", Required: true}, {Name: "schema", Value: "string"}, {Name: "level", Value: "string"},
			}}},
		Kinds: []schemaext.Kind{widgetKind},
		Decode: func(declaration annotation.Declaration) ([]annotation.Contribution, error) {
			*received = append(*received, declaration.Attributes["schema"])
			return decodeWidget(declaration)
		},
		Coverage: annotation.Unlimited(widgetCoverage),
	})
	c.Assert(err, qt.IsNil)
	return set
}

// A provider's directive receives its schema attribute read as Ptah's own
// directives do: the space around it removed and one quoted identifier
// unquoted, as the executable provider page says.
func TestParseSource_AnOwnerDirectiveReceivesItsSchemaRead(t *testing.T) {
	tests := []struct {
		name, schema, want string
	}{
		{name: "a quoted schema with a capital", schema: `\"App\"`, want: "App"},
		{name: "a bracketed schema", schema: "[ app]", want: " app"},
		{name: "a bare schema", schema: "app", want: "app"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var received []string

			_, err := goschema.ParseSource(schemaWidgetOwner(c, &received), "items.go",
				"package models\n\n//ptah:schema:widget table=\"items\" schema=\""+test.schema+"\" level=\"high\"\n"+
					"type W struct{}\n\n//ptah:schema:table name=\"items\"\ntype Item struct{}\n")

			c.Assert(err, qt.IsNil)
			c.Assert(received, qt.DeepEquals, []string{test.want})
		})
	}
}

// A bare schema that ends with a space is refused before the provider runs.
func TestParseSource_AnOwnerDirectiveWithASpacedBareSchema_FailurePath(t *testing.T) {
	c := qt.New(t)
	var received []string

	_, err := goschema.ParseSource(schemaWidgetOwner(c, &received), "items.go",
		"package models\n\n//ptah:schema:widget table=\"items\" schema=\"app \" level=\"high\"\n"+
			"type W struct{}\n\n//ptah:schema:table name=\"items\"\ntype Item struct{}\n")

	c.Assert(err, qt.ErrorIs, tableref.ErrSpacedBareName)
	c.Assert(received, qt.IsNil)
}
