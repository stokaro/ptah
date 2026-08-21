package docsrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/docsrender"
)

func usersSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users", Comment: "People who can sign in."},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT", Unique: true, Comment: "Login identity."},
			{StructName: "User", Name: "status", Type: "TEXT", Default: "'active'", Nullable: true},
			{StructName: "Post", Name: "author_id", Type: "BIGINT", Foreign: "users(id)"},
		},
		Indexes: []goschema.Index{
			{StructName: "User", Name: "idx_users_email", Fields: []string{"email"}, Unique: true},
		},
		Enums: []goschema.Enum{{Name: "status", Values: []string{"active", "banned"}}},
	}
}

func TestRenderDocumentsEveryColumnOfEveryTable(t *testing.T) {
	c := qt.New(t)

	result, err := docsrender.Render(usersSchema(), docsrender.Options{})

	c.Assert(err, qt.IsNil)
	doc := string(result.Data)
	// Every column appears, unlike the API targets which project a public
	// shape: documentation that hid a column would describe a schema the
	// reader does not have.
	for _, want := range []string{"| id |", "| email |", "| status |", "| author_id |"} {
		c.Assert(doc, qt.Contains, want)
	}
	c.Assert(doc, qt.Contains, "People who can sign in.")
	c.Assert(doc, qt.Contains, "Login identity.")
	c.Assert(doc, qt.Contains, "FK → users(id)")
	c.Assert(doc, qt.Contains, "`idx_users_email` — unique index on email")
	c.Assert(doc, qt.Contains, "`status` — active, banned")
}

// TestRenderReportsAYAMLDeclaredDefault pins the asymmetry that would otherwise
// document every YAML schema as having no defaults at all.
//
// The Go annotation parser sets DefaultSet beside Default; the YAML loader sets
// only Default. A renderer that required the flag would be correct on one
// source and silently wrong on the other.
func TestRenderReportsADefaultFromEitherSource(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "from_yaml", Type: "TEXT", Default: "'a'"},
			{StructName: "T", Name: "from_go", Type: "TEXT", Default: "'b'", DefaultSet: true},
			{StructName: "T", Name: "empty_on_purpose", Type: "TEXT", DefaultSet: true},
			{StructName: "T", Name: "expression", Type: "TIMESTAMP", DefaultExpr: "now()"},
			{StructName: "T", Name: "none", Type: "TEXT"},
		},
	}

	result, err := docsrender.Render(db, docsrender.Options{})

	c.Assert(err, qt.IsNil)
	doc := string(result.Data)
	c.Assert(doc, qt.Contains, "| from_yaml | TEXT | no | 'a' |")
	c.Assert(doc, qt.Contains, "| from_go | TEXT | no | 'b' |")
	c.Assert(doc, qt.Contains, "| expression | TIMESTAMP | no | now() |")
	// No default and a deliberately empty one both render as the placeholder,
	// because a blank table cell is indistinguishable from a broken row.
	c.Assert(doc, qt.Contains, "| none | TEXT | no | — |")
}

func TestRenderSaysSoWhenTheSelectionMatchesNothing(t *testing.T) {
	c := qt.New(t)

	result, err := docsrender.Render(usersSchema(), docsrender.Options{IncludeTables: []string{"absent"}})

	c.Assert(err, qt.IsNil)
	// An empty document cannot be told apart from "the filter matched none",
	// so it says which one it is and warns.
	c.Assert(string(result.Data), qt.Contains, "No tables are selected.")
	c.Assert(result.Diagnostics, qt.DeepEquals, []string{"no tables matched the selection"})
}

func TestRenderEscapesAPipeRatherThanBreakingTheRow(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{{StructName: "T", Name: "c", Type: "TEXT", Comment: "a | b"}},
	}

	result, err := docsrender.Render(db, docsrender.Options{})

	c.Assert(err, qt.IsNil)
	line := rowFor(string(result.Data), "| c |")
	// An unescaped pipe would split the comment into two cells and shift every
	// column after it.
	c.Assert(line, qt.Contains, `a \| b`)
	c.Assert(strings.Count(line, "|")-strings.Count(line, `\|`), qt.Equals, 7)
}

// rowFor keeps the search out of the test body, which teststyle governs.
func rowFor(document, prefix string) string {
	for line := range strings.SplitSeq(document, "\n") {
		if strings.HasPrefix(line, prefix) {
			return line
		}
	}
	return ""
}

func TestRenderRefusesANilSchema(t *testing.T) {
	c := qt.New(t)

	_, err := docsrender.Render(nil, docsrender.Options{})

	c.Assert(err, qt.ErrorMatches, "schema database is nil")
}
