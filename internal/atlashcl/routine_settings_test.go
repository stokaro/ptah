package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// routineDocument renders a routine of the given block type carrying the given
// attribute lines.
func routineDocument(blockType, attrs string) []byte {
	returns := "\n  return = int"
	if blockType == "procedure" {
		returns = ""
	}
	return []byte(`
schema "public" {}

` + blockType + ` "r" {
  schema = schema.public
  lang   = SQL` + returns + `
  as     = "SELECT 1"
  ` + attrs + `
}
`)
}

// TestParseRoutineSetReachesTheRenderedClause_HappyPath pins that a routine's
// `set` becomes the SET clause on the rendered statement.
//
// The attribute was accepted and never read. A routine pinning its search_path
// rendered without the clause, so a SECURITY DEFINER routine resolved
// unqualified names through whatever the caller had set -- the hazard the
// clause exists to close -- and the document reported exit 0 with nothing on
// stderr (stokaro/ptah#3121).
func TestParseRoutineSetReachesTheRenderedClause_HappyPath(t *testing.T) {
	rows := []struct {
		name      string
		blockType string
	}{
		{name: "function", blockType: "function"},
		{name: "procedure", blockType: "procedure"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(routineDocument(row.blockType, `
  security = DEFINER
  set = {
    search_path = "public"
  }`), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Functions, qt.HasLen, 1)
			c.Assert(db.Functions[0].Settings, qt.DeepEquals, []string{"search_path=public"})
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, "SET search_path = public")
		})
	}
}

// TestParseRoutineSetOrdersItsPairs_HappyPath pins that one declaration renders
// one way.
//
// A map has no order of its own, so without sorting the same document produced
// a different statement between runs and every comparison against a live
// routine could report a change nobody made.
func TestParseRoutineSetOrdersItsPairs_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(routineDocument("function", `
  set = {
    work_mem    = "64MB"
    search_path = "public"
    row_security = "off"
  }`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(db.Functions[0].Settings, qt.DeepEquals,
		[]string{"row_security=off", "search_path=public", "work_mem=64MB"})
}

// TestParseRoutineWithoutSetCarriesNoSettings_HappyPath is the control for the
// reader above: a routine that declares no settings must render as it did.
func TestParseRoutineWithoutSetCarriesNoSettings_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(routineDocument("function", `security = DEFINER`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(db.Functions[0].Settings, qt.HasLen, 0)
	sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
	c.Assert(sql, qt.Not(qt.Contains), "SET ")
}

// TestParseRoutineSetRefusesWhatItCannotRead_FailurePath pins that a `set` the
// parser cannot turn into name=value pairs stops the parse.
//
// Reading it as no settings at all is the shape this change exists to remove:
// the routine would render without the clause its author wrote, and the
// document would be accepted.
func TestParseRoutineSetRefusesWhatItCannotRead_FailurePath(t *testing.T) {
	rows := []struct {
		name  string
		attrs string
		want  string
	}{
		{
			name:  "a list is not name = value pairs",
			attrs: `set = ["search_path"]`,
			want:  `function set must be an object of name = value pairs`,
		},
		{
			name:  "a string is not name = value pairs",
			attrs: `set = "search_path=public"`,
			want:  `function set must be an object of name = value pairs`,
		},
		{
			name:  "a null value names no setting",
			attrs: "set = {\n    search_path = null\n  }",
			want:  `function set "search_path" has no value`,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(routineDocument("function", row.attrs), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, `(?s).*`+row.want+`.*`)
			c.Assert(db, qt.IsNil)
		})
	}
}
