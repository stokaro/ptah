package atlashcl_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// plainSchemaHCL and envSchemaHCL differ by exactly one thing: the top-level
// env block. That is what makes them a discriminating pair for issue #1073 --
// any difference in outcome between the two is attributable to the marker
// block alone.
const plainSchemaHCL = `schema "main" {
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`

const envSchemaHCL = `schema "main" {
}

env "local" {
  url = "sqlite://local.db"
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`

// TestParseProjectFileIsNotSilentlyErased pins the exact damage issue #1073
// reports. Before the fix, a schema file carrying a project-file env block
// parsed successfully into an IR indistinguishable from the same file with the
// block deleted: the project-level content was erased with no schema object and
// no diagnostic to show for it.
//
// The discriminating assertion is deliberately not "some error happened". The
// comment carries the IR equality, so a red run states that the two parses
// agreed -- which is the erasure itself -- rather than merely that an expected
// error was missing.
func TestParseProjectFileIsNotSilentlyErased(t *testing.T) {
	c := qt.New(t)

	withoutEnv, err := atlashcl.Parse([]byte(plainSchemaHCL), "schema.hcl")
	c.Assert(err, qt.IsNil)

	withEnv, errEnv := atlashcl.Parse([]byte(envSchemaHCL), "schema.hcl")

	c.Assert(errEnv, qt.IsNotNil, qt.Commentf(
		"env block was erased silently: parse succeeded and the IR is identical to the same file with the block deleted (IRs equal = %v)",
		reflect.DeepEqual(withoutEnv, withEnv)))
	c.Assert(withEnv, qt.IsNil)
}

// projectFileCase drives one file shape through Parse. Each row carries its own
// assert func so the per-row expectations stay in the row instead of turning the
// loop body into a decision tree.
type projectFileCase struct {
	name   string
	hcl    string
	assert func(c *qt.C, db *goschema.Database, err error)
}

// refusesAsProjectFile asserts the classification refusal: the file is rejected,
// the message keeps the sentence the Atlas community binary emits, and it goes
// past that binary by naming the offending construct and where it sits.
func refusesAsProjectFile() func(c *qt.C, db *goschema.Database, err error) {
	return func(c *qt.C, db *goschema.Database, err error) {
		c.Assert(err, qt.IsNotNil)
		c.Assert(db, qt.IsNil)
		c.Assert(err.Error(), qt.Contains, `cannot parse project file "schema.hcl" as a schema file`)
		c.Assert(err.Error(), qt.Contains, `"env"`)
		c.Assert(err.Error(), qt.Contains, "schema.hcl:")
	}
}

// parsesWithUsersTable asserts the file is accepted and really produced the
// table, so a row cannot pass by returning an empty IR.
func parsesWithUsersTable() func(c *qt.C, db *goschema.Database, err error) {
	return func(c *qt.C, db *goschema.Database, err error) {
		c.Assert(err, qt.IsNil)
		c.Assert(db, qt.IsNotNil)
		c.Assert(db.Tables, qt.HasLen, 1)
		c.Assert(db.Tables[0].Name, qt.Equals, "users")
	}
}

// failsWith asserts an unrelated pre-existing error survives the change
// untouched.
func failsWith(want string) func(c *qt.C, db *goschema.Database, err error) {
	return func(c *qt.C, db *goschema.Database, err error) {
		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Contains, want)
	}
}

// TestParseProjectFileClassification fixes the trigger boundary measured against
// the pinned Atlas community binary: a top-level env block marks the file
// regardless of label, body or surrounding content, while a nested env block and
// an env attribute are different constructs and must keep their own behavior.
//
// The negative rows share the table with the positive ones on purpose. A fix
// that classified too eagerly -- on any occurrence of the name rather than on a
// top-level block -- would pass every refusal row and fail here.
func TestParseProjectFileClassification(t *testing.T) {
	cases := []projectFileCase{
		{
			name:   "labeled env block",
			hcl:    envSchemaHCL,
			assert: refusesAsProjectFile(),
		},
		{
			name: "unlabeled env block",
			hcl: `schema "main" {
}

env {
  url = "sqlite://local.db"
}
`,
			assert: refusesAsProjectFile(),
		},
		{
			name: "env block with empty body",
			hcl: `schema "main" {
}

env "local" {
}
`,
			assert: refusesAsProjectFile(),
		},
		{
			name: "env-only file with no schema objects",
			hcl: `env "local" {
  url = "sqlite://local.db"
  migration {
    dir = "file://migrations"
  }
}
`,
			assert: refusesAsProjectFile(),
		},
		{
			name: "env block with contents that are nonsense",
			hcl: `schema "main" {
}

env "local" {
  totally_not_a_project_attr = 1
  weird_block "x" {
    y = 2
  }
}
`,
			assert: refusesAsProjectFile(),
		},
		{
			// Classification must win over schema-body validation. The community
			// binary reports the project-file error on this shape, not the block
			// error, so the pre-pass has to run before the body walk.
			name: "env block alongside an invalid table block",
			hcl: `schema "main" {
}

env "local" {
  url = "sqlite://local.db"
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  bogus_block "x" {
    y = 2
  }
}
`,
			assert: refusesAsProjectFile(),
		},
		{
			name:   "plain schema file",
			hcl:    plainSchemaHCL,
			assert: parsesWithUsersTable(),
		},
		{
			// The community binary accepts and evaluates variable in a schema
			// file. Ptah accepts and ignores it. Refusing it here would be a new
			// stricter break, so this row guards the arm #1073 must not touch.
			name: "variable block with a default",
			hcl: `variable "status" {
  type    = string
  default = "active"
}

schema "main" {
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
			assert: parsesWithUsersTable(),
		},
		{
			name: "top-level env attribute is not a block",
			hcl: `env = "notablock"

schema "main" {
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
			assert: parsesWithUsersTable(),
		},
		{
			name: "env block nested inside a table",
			hcl: `schema "main" {
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  env "local" {
    url = "sqlite://x.db"
  }
}
`,
			assert: failsWith(`unsupported table block "env"`),
		},
		{
			name: "env attribute inside a table",
			hcl: `schema "main" {
}

table "users" {
  schema = schema.main
  env    = "notablock"
  column "id" {
    type = int
  }
}
`,
			assert: failsWith(`unsupported table attribute "env"`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse([]byte(tc.hcl), "schema.hcl")
			tc.assert(c, db, err)
		})
	}
}
