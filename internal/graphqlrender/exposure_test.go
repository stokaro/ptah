package graphqlrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/graphqlrender"
	"go.5x5.cz/ptah/internal/schemaexport"
)

func exposureSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true, APIExpose: "read"},
			{StructName: "User", Name: "email", Type: "TEXT", APIExpose: "read-write"},
			{StructName: "User", Name: "password_hash", Type: "TEXT", APIExpose: "write"},
			{StructName: "User", Name: "internal_state", Type: "TEXT", APIExpose: "none"},
		},
	}
}

func renderExposureSDL(c *qt.C, policy schemaexport.FieldPolicy) string {
	c.Helper()
	rendered, err := graphqlrender.Render(exposureSchema(), graphqlrender.Options{
		FieldPolicy: policy,
		Operations:  graphqlrender.Operations{CreateInput: true},
	})
	c.Assert(err, qt.IsNil)
	return string(rendered.Data)
}

// TestExposureSplitsObjectAndInput pins the read/write split onto the two
// declarations GraphQL already has for it.
//
// The object type is what a caller reads and the input type is what it sends,
// so a credential declared write reaches the input and never the object, and a
// server-owned key declared read reaches the object and never the input. This
// is the criterion "read and write projections can differ" with no conditional
// test logic: two separate declarations, each asserted directly.
func TestExposureSplitsObjectAndInput(t *testing.T) {
	tests := []struct {
		name       string
		column     string
		inObject   bool
		inputShape bool
	}{
		{name: "a read-only key", column: "id", inObject: true},
		{name: "a read-write column", column: "email", inObject: true, inputShape: true},
		{name: "a write-only credential", column: "password_hash", inputShape: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sdl := renderExposureSDL(c, schemaexport.FieldPolicyAll)

			object := section(sdl, "type User {")
			input := section(sdl, "input UserCreateInput {")

			c.Assert(containsField(object, test.column), qt.Equals, test.inObject,
				qt.Commentf("object:\n%s", object))
			c.Assert(containsField(input, test.column), qt.Equals, test.inputShape,
				qt.Commentf("input:\n%s", input))
		})
	}
}

// TestExposureNoneReachesNeitherDeclaration is the control: a column declared
// none is absent from the whole document, not merely from one shape.
func TestExposureNoneReachesNeitherDeclaration(t *testing.T) {
	c := qt.New(t)

	sdl := renderExposureSDL(c, schemaexport.FieldPolicyAll)

	c.Assert(sdl, qt.Not(qt.Contains), "internal_state")
}

// TestExposureUnchangedWithoutDeclarations is the non-interference control: a
// schema declaring no exposure renders exactly as it did before this existed.
func TestExposureUnchangedWithoutDeclarations(t *testing.T) {
	c := qt.New(t)
	plain := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
	}

	before, err := graphqlrender.Render(plain, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)
	after, err := graphqlrender.Render(plain, graphqlrender.Options{FieldPolicy: schemaexport.FieldPolicyAll})
	c.Assert(err, qt.IsNil)

	c.Assert(string(after.Data), qt.Equals, string(before.Data))
}

// containsField reports whether an SDL block declares a field with this name,
// matching the "  name:" shape so a substring of another identifier does not
// count.
func containsField(block, name string) bool {
	return strings.Contains(block, "\n  "+name+":")
}
