package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

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
		name        string
		source      string
		wantBlocks  []atlashcl.SchemaBlock
		wantSchemas int
	}{
		{
			name: "two names are two blocks",
			source: `schema "main" {}
schema "other" {}
`,
			wantBlocks: []atlashcl.SchemaBlock{
				{Name: "main", Filename: "schema.hcl", Line: 1},
				{Name: "other", Filename: "schema.hcl", Line: 2},
			},
			wantSchemas: 2,
		},
		{
			name: "one name declared twice is two blocks and one schema",
			source: `schema "main" {}
schema "main" {}
`,
			wantBlocks: []atlashcl.SchemaBlock{
				{Name: "main", Filename: "schema.hcl", Line: 1},
				{Name: "main", Filename: "schema.hcl", Line: 2},
			},
			wantSchemas: 1,
		},
		{
			name:        "a document with no schema block records nothing",
			source:      "table \"users\" {\n  column \"id\" {\n    type = int\n  }\n}\n",
			wantBlocks:  nil,
			wantSchemas: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var recorded []atlashcl.SchemaBlock

			db, err := atlashcl.ParseWithOptions([]byte(test.source), "schema.hcl", atlashcl.Options{
				RecordSchemaBlock: func(block atlashcl.SchemaBlock) { recorded = append(recorded, block) },
			})

			c.Assert(err, qt.IsNil)
			c.Assert(recorded, qt.DeepEquals, test.wantBlocks)
			c.Assert(db.Schemas, qt.HasLen, test.wantSchemas)
		})
	}
}

// TestRecordSchemaBlockRecordsNothingForAProjectFile is the refusing half of
// the recorder's contract, and it asserts about a document that never becomes a
// schema at all -- so there is no schema count to compare and nothing the table
// above could state as a row.
//
// A recorder that ran before the parse decided what the document is would hand
// the gate a block from a file the parse then refused.
func TestRecordSchemaBlockRecordsNothingForAProjectFile(t *testing.T) {
	c := qt.New(t)
	source := `schema "main" {}
env "local" {
  url = "postgres://localhost/x"
}
`
	var recorded []atlashcl.SchemaBlock

	_, err := atlashcl.ParseWithOptions([]byte(source), "schema.hcl", atlashcl.Options{
		RecordSchemaBlock: func(block atlashcl.SchemaBlock) { recorded = append(recorded, block) },
	})

	c.Assert(err, qt.ErrorMatches, `cannot parse project file "schema.hcl" as a schema file: .*`)
	c.Assert(recorded, qt.HasLen, 0)
}
