package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// triggerDocument renders a table, a trigger function, and a trigger carrying
// the given lines.
func triggerDocument(triggerLines string) []byte {
	return []byte(`
schema "public" {}

table "t" {
  schema = schema.public
  column "id" { type = int }
}

function "shared_touch" {
  schema = schema.public
  lang   = PLpgSQL
  return = trigger
  as     = "BEGIN RETURN NEW; END;"
}

trigger "t_touch" {
  on = table.t
  before {
    update = true
  }
  foreach = ROW
  ` + triggerLines + `
}
`)
}

// TestParseTriggerExecuteBindsADeclaredFunction_HappyPath pins that an execute
// block binds the trigger to a function the schema declares, by its qualified
// name.
//
// A body makes Ptah generate a private function per trigger and drop it with
// the trigger; an execute block binds one several triggers may share. The model
// has carried both alternatives all along -- a SQL schema file reaches the
// second, and the reader reports it -- and this format could reach only the
// first (stokaro/ptah#3113).
//
// The reference names the function's label and not its schema, so it is
// resolved against the functions the document declares. Emitted unqualified,
// the statement is resolved by the server through search_path, which does not
// hold a schema the document just created.
func TestParseTriggerExecuteBindsADeclaredFunction_HappyPath(t *testing.T) {
	rows := []struct {
		name  string
		lines string
	}{
		{
			name:  "a function reference",
			lines: "execute {\n    function = function.shared_touch\n  }",
		},
		{
			name:  "a function named as a string",
			lines: "execute {\n    function = \"shared_touch\"\n  }",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(triggerDocument(row.lines), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Triggers, qt.HasLen, 1)
			c.Assert(db.Triggers[0].ExecuteFunction, qt.Equals, "public.shared_touch")
			c.Assert(db.Triggers[0].Body, qt.Equals, "")
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, `EXECUTE FUNCTION "public"."shared_touch"()`)
			// The private function a body would have produced is not rendered,
			// which is what makes the two alternatives rather than spellings.
			c.Assert(sql, qt.Not(qt.Contains), "ptah_trigger_")
		})
	}
}

// TestParseTriggerBodyStillOwnsItsFunction_HappyPath is the control.
//
// The body spelling has to keep generating the private function it owns, or
// reading the execute block would have been bought by breaking every trigger
// that does not use it.
func TestParseTriggerBodyStillOwnsItsFunction_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(triggerDocument(`as = "BEGIN RETURN NEW; END;"`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Triggers, qt.HasLen, 1)
	c.Assert(db.Triggers[0].ExecuteFunction, qt.Equals, "")
	sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
	c.Assert(sql, qt.Contains, "ptah_trigger_")
}

// TestParseTriggerExecute_FailurePath pins the declarations that describe a
// trigger no server creates.
func TestParseTriggerExecute_FailurePath(t *testing.T) {
	rows := []struct {
		name  string
		lines string
		want  string
	}{
		{
			name:  "neither a body nor an execute block",
			lines: ``,
			want:  `trigger "t_touch" requires as or an execute block`,
		},
		{
			name:  "both",
			lines: "as = \"BEGIN RETURN NEW; END;\"\n  execute {\n    function = function.shared_touch\n  }",
			want:  `trigger "t_touch" cannot declare both as and an execute block`,
		},
		{
			name:  "two execute blocks",
			lines: "execute {\n    function = function.shared_touch\n  }\n  execute {\n    function = function.shared_touch\n  }",
			want:  `trigger "t_touch" contains multiple execute blocks`,
		},
		{
			name:  "an execute block naming nothing",
			lines: "execute {\n  }",
			want:  `trigger "t_touch" execute requires function`,
		},
		{
			name:  "an unknown attribute inside execute",
			lines: "execute {\n    function = function.shared_touch\n    zzz_nonsense = true\n  }",
			want:  `unsupported trigger execute attribute "zzz_nonsense"`,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(triggerDocument(row.lines), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, `(?s).*`+row.want+`.*`)
			c.Assert(db, qt.IsNil)
		})
	}
}
