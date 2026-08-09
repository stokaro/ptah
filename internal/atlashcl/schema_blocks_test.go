package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// TestRecordSchemaBlockReportsBlocksNotSchemas pins the one property the
// schema-scope gate in internal/schemafile rests on: this recorder counts
// BLOCKS, and the parse result counts SCHEMAS.
//
// The two differ exactly when a document declares one schema twice.
// `goschema.Finalize` folds those into one entry in Database.Schemas, while the
// pinned Atlas community binary v1.3.0 counts two: measured with
// `schema inspect -u file://<schema "main" twice>.hcl --dev-url sqlite://dv?mode=memory`,
// that binary exits 1 with `cannot use HCL with more than 1 schema when dev-url
// is limited to schema "main"`, the same refusal it gives a document naming two
// different schemas.
//
// A recorder that deduplicated by name would leave the gate exiting 0 on that
// document, so the duplicate row asserts on both numbers rather than on the
// two-name row alone.
func TestRecordSchemaBlockReportsBlocksNotSchemas(t *testing.T) {
	tests := []struct {
		name   string
		source string
		assert func(c *qt.C, blocks []atlashcl.SchemaBlock, db *goschema.Database, err error)
	}{
		{
			name: "two names are two blocks",
			source: `schema "main" {}
schema "other" {}
`,
			assert: func(c *qt.C, blocks []atlashcl.SchemaBlock, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(blocks, qt.DeepEquals, []atlashcl.SchemaBlock{
					{Name: "main", Filename: "schema.hcl", Line: 1},
					{Name: "other", Filename: "schema.hcl", Line: 2},
				})
				c.Assert(db.Schemas, qt.HasLen, 2)
			},
		},
		{
			name: "one name declared twice is two blocks and one schema",
			source: `schema "main" {}
schema "main" {}
`,
			assert: func(c *qt.C, blocks []atlashcl.SchemaBlock, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(blocks, qt.DeepEquals, []atlashcl.SchemaBlock{
					{Name: "main", Filename: "schema.hcl", Line: 1},
					{Name: "main", Filename: "schema.hcl", Line: 2},
				})
				c.Assert(db.Schemas, qt.HasLen, 1)
			},
		},
		{
			name:   "a document with no schema block records nothing",
			source: "table \"users\" {\n  column \"id\" {\n    type = int\n  }\n}\n",
			assert: func(c *qt.C, blocks []atlashcl.SchemaBlock, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(blocks, qt.HasLen, 0)
				c.Assert(db.Schemas, qt.HasLen, 0)
			},
		},
		{
			name: "a project file records nothing, because it is not a schema file",
			source: `schema "main" {}
env "local" {
  url = "postgres://localhost/x"
}
`,
			assert: func(c *qt.C, blocks []atlashcl.SchemaBlock, _ *goschema.Database, err error) {
				c.Assert(err, qt.ErrorMatches, `cannot parse project file "schema.hcl" as a schema file: .*`)
				c.Assert(blocks, qt.HasLen, 0)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var recorded []atlashcl.SchemaBlock

			db, err := atlashcl.ParseWithOptions([]byte(test.source), "schema.hcl", atlashcl.Options{
				RecordSchemaBlock: func(block atlashcl.SchemaBlock) { recorded = append(recorded, block) },
			})

			test.assert(c, recorded, db, err)
		})
	}
}
