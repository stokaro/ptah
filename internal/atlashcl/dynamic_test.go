package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

// dynamicTableDocument wraps a table body so each row below carries only the
// construct it is about.
func dynamicTableDocument(body string) []byte {
	return []byte(`schema "main" {
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  column "email" {
    type = text
  }
  column "tenant" {
    type = text
  }
` + body + `
}
`)
}

// TestParseDynamicBlockGeneratesTheBlocksItStandsFor covers stokaro/ptah#1636.
//
// A `dynamic "index"` block was accepted at exit 0 and expanded to nothing:
// measured on the pinned community Atlas binary v1.3.0 and on ptah-compat, the
// marker index name appeared in neither output. Exit 0 with the operator's
// declared intent missing and no diagnostic is the silent-drop failure mode, so
// Ptah expands these now.
//
// The rows separate every operand of the expansion:
//
//   - the count comes from for_each, so a two-element list yields two indexes;
//   - the label comes from `labels`, evaluated per iteration, which is what
//     lets two generated blocks of a labeled type differ;
//   - the body reads the iteration through the iterator root, whose default
//     name is the block's label and which `iterator` overrides;
//   - a map for_each carries a key as well as a value;
//   - a content block with a literal label keeps it, which is the shape for a
//     generated block whose label does not vary.
func TestParseDynamicBlockGeneratesTheBlocksItStandsFor(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantIndexes []string
		wantColumns []string
	}{
		{
			name: "a two-element list yields two indexes",
			body: `  dynamic "index" {
    for_each = ["email", "tenant"]
    labels   = ["idx_${index.value}"]
    content {
      columns = [index.value]
    }
  }`,
			wantIndexes: []string{"idx_email", "idx_tenant"},
			wantColumns: []string{"email", "tenant"},
		},
		{
			name: "an empty for_each yields nothing",
			body: `  dynamic "index" {
    for_each = []
    labels   = ["idx_none"]
    content {
      columns = ["email"]
    }
  }`,
			wantIndexes: make([]string, 0),
			wantColumns: make([]string, 0),
		},
		{
			name: "the iterator name is overridable",
			body: `  dynamic "index" {
    for_each = ["email"]
    iterator = col
    labels   = ["idx_${col.value}"]
    content {
      columns = [col.value]
    }
  }`,
			wantIndexes: []string{"idx_email"},
			wantColumns: []string{"email"},
		},
		{
			name: "a map for_each carries a key beside the value",
			body: `  dynamic "index" {
    for_each = {
      by_email = "email"
    }
    labels = ["idx_${index.key}"]
    content {
      columns = [index.value]
    }
  }`,
			wantIndexes: []string{"idx_by_email"},
			wantColumns: []string{"email"},
		},
		{
			name: "a literal content label is kept",
			body: `  dynamic "index" {
    for_each = ["email"]
    content "idx_literal" {
      columns = [index.value]
    }
  }`,
			wantIndexes: []string{"idx_literal"},
			wantColumns: []string{"email"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(dynamicTableDocument(test.body), "schema.hcl")

			c.Assert(err, qt.IsNil)
			names := make([]string, 0, len(db.Indexes))
			columns := make([]string, 0, len(db.Indexes))
			for _, index := range db.Indexes {
				names = append(names, index.Name)
				columns = append(columns, strings.Join(index.Fields, ","))
			}
			c.Assert(names, qt.ContentEquals, test.wantIndexes)
			c.Assert(columns, qt.ContentEquals, test.wantColumns)
		})
	}
}

// TestParseDynamicBlockRefusesWhatItCannotExpand pins the refusals. A dynamic
// block that cannot be expanded must not fall back to the old behavior of
// contributing nothing at exit 0 — that silence is what stokaro/ptah#1636 is
// about, and a malformed block is the case where it would be least noticed.
func TestParseDynamicBlockRefusesWhatItCannotExpand(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantError string
	}{
		{
			name: "no label names no block type",
			body: `  dynamic {
    for_each = ["email"]
    content {
      columns = ["email"]
    }
  }`,
			wantError: "exactly one label",
		},
		{
			name: "no for_each iterates nothing",
			body: `  dynamic "index" {
    labels = ["idx_email"]
    content {
      columns = ["email"]
    }
  }`,
			wantError: "requires a for_each attribute",
		},
		{
			name: "no content repeats nothing",
			body: `  dynamic "index" {
    for_each = ["email"]
  }`,
			wantError: "requires a content block",
		},
		{
			name: "a scalar for_each is not a collection",
			body: `  dynamic "index" {
    for_each = "email"
    labels   = ["idx_email"]
    content {
      columns = ["email"]
    }
  }`,
			wantError: "must be a list, set, map or object",
		},
		{
			name: "a dynamic block cannot generate a dynamic block",
			body: `  dynamic "dynamic" {
    for_each = ["email"]
    content {
      columns = ["email"]
    }
  }`,
			wantError: "cannot generate another dynamic block",
		},
		{
			name: "a body block other than content is refused by name",
			body: `  dynamic "index" {
    for_each = ["email"]
    nonsense {
      columns = ["email"]
    }
  }`,
			wantError: `unsupported dynamic block "nonsense"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlashcl.Parse(dynamicTableDocument(test.body), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, `.*`+test.wantError+`.*`)
		})
	}
}

// TestParseWithoutDynamicBlockIsUnchanged is the non-interference control. The
// expansion is reached from the block walk, so a document that writes no
// dynamic block must produce exactly what it did before — including a two-step
// traversal in a column reference, which the expansion had to learn to tell
// apart from an iteration.
func TestParseWithoutDynamicBlockIsUnchanged(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(dynamicTableDocument(`  index "idx_email" {
    columns = [column.email]
  }`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "idx_email")
	c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{"email"})
}
