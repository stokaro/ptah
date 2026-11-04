package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
)

// TestParseSourceUnknownDirective_FailurePath holds an exhaustive parse to
// the directives it knows. A //ptah: comment naming no directive of the
// frontend or of a selected owner is a misspelling: read as documentation,
// `//ptah:schema:column` left the table without its column and
// `//ptah:schema:tabel` left out the table, and the render succeeded. The
// refusal names the directive, where it sits and its line.
func TestParseSourceUnknownDirective_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		owners    annotation.Set
		source    string
		directive string
		line      int
		wantErr   string
	}{
		{
			name:   "a misspelled field directive",
			owners: annotation.None().Exhaustive(),
			source: "package test\n\n//ptah:schema:table name=\"users\"\ntype User struct {\n" +
				"\t//ptah:schema:column name=\"id\" type=\"INTEGER\"\n\tID int\n}\n",
			directive: "ptah:schema:column",
			line:      5,
			wantErr: `unknown annotation directive "//ptah:schema:column" on User\.ID at schema\.go:5: ` +
				`neither Ptah nor a selected provider declares it`,
		},
		{
			name:      "a misspelled table directive",
			owners:    annotation.None().Exhaustive(),
			source:    "package test\n\n//ptah:schema:tabel name=\"users\"\ntype User struct{}\n",
			directive: "ptah:schema:tabel",
			line:      3,
			wantErr: `unknown annotation directive "//ptah:schema:tabel" on User at schema\.go:3: ` +
				`neither Ptah nor a selected provider declares it`,
		},
		{
			name:      "a name that extends a known one",
			owners:    annotation.None().Exhaustive(),
			source:    "package test\n\n//ptah:schema:tableau name=\"users\"\ntype User struct{}\n",
			directive: "ptah:schema:tableau",
			line:      3,
			wantErr: `unknown annotation directive "//ptah:schema:tableau" on User at schema\.go:3: ` +
				`neither Ptah nor a selected provider declares it`,
		},
		{
			name:      "a misspelling the runtime's own parse meets",
			owners:    must.Must(builtin.New()).Annotations(),
			source:    "package test\n\n//ptah:schema:hypertabel table=\"users\" column=\"id\"\ntype User struct{}\n",
			directive: "ptah:schema:hypertabel",
			line:      3,
			wantErr: `unknown annotation directive "//ptah:schema:hypertabel" on User at schema\.go:3: ` +
				`neither Ptah nor a selected provider declares it`,
		},
		{
			name:   "a spaced comment with a tab after the name",
			owners: annotation.None().Exhaustive(),
			source: "package test\n\n//ptah:schema:table name=\"users\"\ntype User struct {\n" +
				"\t// ptah:schema:colum\tname=\"id\"\n\tID int\n}\n",
			directive: "ptah:schema:colum",
			line:      5,
			wantErr: `unknown annotation directive "//ptah:schema:colum" on User\.ID at schema\.go:5: ` +
				`neither Ptah nor a selected provider declares it`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := goschema.ParseSource(test.owners, "schema.go", test.source)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownDirective)
			var parseErr *ptaherr.ParseError
			c.Assert(err, qt.ErrorAs, &parseErr)
			c.Assert(parseErr.Directive, qt.Equals, test.directive)
			c.Assert(parseErr.Line, qt.Equals, test.line)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// TestParseSourceUnknownDirective_HappyPath is the other half: what is not a
// misspelled directive parses. Prose is documentation even when it mentions
// Ptah, a block comment is not a directive, a parse that selected some owners
// leaves the others' directives to them, and an exhaustive set from the
// built-in runtime knows its owners' directives.
func TestParseSourceUnknownDirective_HappyPath(t *testing.T) {
	const table = "//ptah:schema:table name=\"users\"\ntype User struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n}\n"
	tests := []struct {
		name   string
		owners annotation.Set
		source string
	}{
		{
			name:   "prose that names Ptah",
			owners: annotation.None().Exhaustive(),
			source: "package test\n\n// ptah is the tool that renders this table.\n// ptah: keep it short.\n" + table,
		},
		{
			name:   "a block comment",
			owners: annotation.None().Exhaustive(),
			source: "package test\n\n/* ptah:schema:column is not read */\n" + table,
		},
		{
			name:   "an owner's directive in a parse that did not select it",
			owners: annotation.None(),
			source: "package test\n\n//ptah:schema:hypertable table=\"users\" column=\"id\"\n" + table,
		},
		{
			name:   "a misspelling in a parse that did not claim every owner",
			owners: annotation.None(),
			source: "package test\n\n//ptah:schema:tabel name=\"ghost\"\n" + table,
		},
		{
			name:   "an owner's directive in the runtime's own parse",
			owners: must.Must(builtin.New()).Annotations(),
			source: "package test\n\n//ptah:schema:hypertable table=\"users\" column=\"id\"\n" + table,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := goschema.ParseSource(test.owners, "schema.go", test.source)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
			c.Assert(database.Tables[0].Name, qt.Equals, "users")
		})
	}
}
