package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

// TestParseSourceMisplacedDirective_FailurePath refuses a directive written
// at a scope it does not take. Skipped, it declared nothing: a field directive
// in a struct's doc comment added no column, and a table directive on a field
// added no table, while the render that followed succeeded. The refusal names
// the directive, where it sits, its line, and where it belongs.
func TestParseSourceMisplacedDirective_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		source    string
		directive string
		line      int
		wantErr   string
	}{
		{
			name: "a field directive in a struct's doc comment",
			source: "package test\n\n//ptah:schema:field name=\"ghost\" type=\"INTEGER\"\n//ptah:schema:table name=\"users\"\n" +
				"type User struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n}\n",
			directive: "ptah:schema:field",
			line:      3,
			wantErr: `annotation directive "//ptah:schema:field" on User at schema\.go:3 is not read in a struct's doc comment; ` +
				`write it on a struct field`,
		},
		{
			name: "a table directive on a field",
			source: "package test\n\n//ptah:schema:table name=\"users\"\ntype User struct {\n" +
				"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n" +
				"\t//ptah:schema:table name=\"phantom\"\n\tName string\n}\n",
			directive: "ptah:schema:table",
			line:      7,
			wantErr: `annotation directive "//ptah:schema:table" on User\.Name at schema\.go:7 is not read on a struct field; ` +
				`write it in a struct's doc comment`,
		},
		{
			name: "an embedding directive in a struct's doc comment",
			source: "package test\n\n//ptah:embedded mode=\"inline\"\n//ptah:schema:table name=\"users\"\n" +
				"type User struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n}\n",
			directive: "ptah:embedded",
			line:      3,
			wantErr: `annotation directive "//ptah:embedded" on User at schema\.go:3 is not read in a struct's doc comment; ` +
				`write it on a struct field`,
		},
		{
			name: "an enum directive on a field",
			source: "package test\n\n//ptah:schema:table name=\"users\"\ntype User struct {\n" +
				"\t//ptah:schema:enum name=\"status\" values=\"on,off\"\n\tStatus string\n}\n",
			directive: "ptah:schema:enum",
			line:      5,
			wantErr: `annotation directive "//ptah:schema:enum" on User\.Status at schema\.go:5 is not read on a struct field; ` +
				`write it in a struct's doc comment`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := goschema.ParseSource(annotation.None(), "schema.go", test.source)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrMisplacedDirective)
			var parseErr *ptaherr.ParseError
			c.Assert(err, qt.ErrorAs, &parseErr)
			c.Assert(parseErr.Directive, qt.Equals, test.directive)
			c.Assert(parseErr.Line, qt.Equals, test.line)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// TestParseSourceMisplacedDirective_HappyPath is the control: a directive
// that takes both scopes parses in either, and a comment that names no
// directive is not a misplaced one.
func TestParseSourceMisplacedDirective_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		source string
	}{
		{
			name: "an index on a field",
			source: "package test\n\n//ptah:schema:table name=\"users\"\ntype User struct {\n" +
				"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n" +
				"\t//ptah:schema:index name=\"users_id_idx\" fields=\"id\"\n\tID int\n}\n",
		},
		{
			name: "an index in a struct's doc comment",
			source: "package test\n\n//ptah:schema:table name=\"users\"\n//ptah:schema:index name=\"users_id_idx\" fields=\"id\"\n" +
				"type User struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n}\n",
		},
		{
			name: "prose that names a directive",
			source: "package test\n\n// User keeps the table //ptah:schema:field describes.\n//ptah:schema:table name=\"users\"\n" +
				"type User struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n}\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := goschema.ParseSource(annotation.None(), "schema.go", test.source)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
		})
	}
}
