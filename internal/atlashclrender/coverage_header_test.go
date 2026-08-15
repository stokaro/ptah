package atlashclrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// The compatibility surface omits three block types the pinned Atlas community
// binary v1.3.0 refuses to read. That omission is a PRESENTATION decision, and
// until this header existed the comparator read it as deletion intent:
// `ptah-compat schema inspect` followed by `schema apply` of its own output
// planned `DROP EXTENSION`, `DROP SEQUENCE` and `DROP POLICY` against the
// database the document came from (stokaro/ptah#1276).
//
// The record has to travel in the DOCUMENT, not on the diagnostics stream, for
// the reason the whole design turns on: the command that reads the document is
// a different process, and it reads the file rather than the terminal.

// TestCompatibilityRenderDeclaresWhatItDoesNotDescribe pins the header the
// compatibility surface writes.
//
// The claim is about the RULE, not about the contents: a block is omitted
// whenever nothing else in the document names it, so for any name absent from
// the document a reader cannot tell "the database does not have it" from
// "nothing named it". The row for a database with none of these objects at all
// is in the table for that reason -- recording only what this render happened
// to omit would leave a document that asserts authority it does not have.
func TestCompatibilityRenderDeclaresWhatItDoesNotDescribe(t *testing.T) {

	tests := []struct {
		name string
		db   func() *goschema.Database
	}{
		{name: "a database holding all three block types", db: inspectedRichDatabase},
		{name: "a database holding none of them", db: coverageBareDatabase},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := atlashclrender.RenderInspectedForAtlasCLI(
				test.db(), platform.Postgres, "public",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(result.NotDescribed, qt.DeepEquals, coverage.Set{}.WithKind(
				coverage.Extension, coverage.Policy, coverage.Sequence,
			))
			c.Assert(coverageHeaderLines(string(result.Data)), qt.DeepEquals, []string{
				"// ptah:not-described extension",
				"// ptah:not-described policy",
				"// ptah:not-described sequence",
			})
		})
	}
}

// TestNativeRenderDeclaresNoLimits is the control that keeps the header from
// becoming an unconditional decoration. The native surface omits nothing, so it
// claims everything, and a removal a native document asks for is still a
// removal.
func TestNativeRenderDeclaresNoLimits(t *testing.T) {

	tests := []struct {
		name   string
		render func() (atlashclrender.Result, error)
	}{
		{
			name: "inspected",
			render: func() (atlashclrender.Result, error) {
				return atlashclrender.RenderInspected(inspectedRichDatabase(), platform.Postgres, "public")
			},
		},
		{
			name: "parse and re-render",
			render: func() (atlashclrender.Result, error) {
				return atlashclrender.RenderForDialect(inspectedRichDatabase(), platform.Postgres)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := test.render()

			c.Assert(err, qt.IsNil)
			c.Assert(result.NotDescribed.IsZero(), qt.IsTrue)
			c.Assert(coverageHeaderLines(string(result.Data)), qt.HasLen, 0)
		})
	}
}

// TestCompatibilityRenderDeclaresNothingOnSQLite pins that the header follows
// the refusal it reports. SQLite's Atlas HCL accepts all three block types, so
// nothing is omitted there and nothing may be claimed.
func TestCompatibilityRenderDeclaresNothingOnSQLite(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspectedForAtlasCLI(
		inspectedRichDatabase(), platform.SQLite, "main",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(result.NotDescribed.IsZero(), qt.IsTrue)
	c.Assert(coverageHeaderLines(string(result.Data)), qt.HasLen, 0)
}

// TestCompatibilityDocumentCarriesItsLimitsBackThroughTheParser is the
// serialization half. A record the renderer keeps only in its own Result is
// worth nothing to `schema apply --to file://...`, which is a new process
// reading bytes.
func TestCompatibilityDocumentCarriesItsLimitsBackThroughTheParser(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspectedForAtlasCLI(
		inspectedRichDatabase(), platform.Postgres, "public",
	)
	c.Assert(err, qt.IsNil)

	parsed, err := atlashcl.ParseWithOptions(result.Data, "schema.hcl", atlashcl.Options{
		IgnoreUnknownNames: true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.NotDescribed, qt.DeepEquals, result.NotDescribed)
}

// TestNativeDocumentCarriesNoLimitsBackThroughTheParser is that round trip's
// control: a document with no header parses to full authority, which is what
// every hand-written schema file is.
func TestNativeDocumentCarriesNoLimitsBackThroughTheParser(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspected(
		inspectedRichDatabase(), platform.Postgres, "public",
	)
	c.Assert(err, qt.IsNil)

	parsed, err := atlashcl.ParseWithOptions(result.Data, "schema.hcl", atlashcl.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.NotDescribed.IsZero(), qt.IsTrue)
}

// coverageBareDatabase is a database holding none of the block types the
// compatibility surface omits, so the header it produces cannot have come from
// counting what was left out.
func coverageBareDatabase() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{Name: "accounts", StructName: "Accounts", Schema: "public"}},
		Fields: []goschema.Field{{Name: "id", StructName: "Accounts", Type: "integer"}},
	}
}

// coverageHeaderLines is every coverage directive line a rendered document
// carries, read out of the bytes rather than out of the renderer's own answer.
func coverageHeaderLines(document string) []string {
	var lines []string
	for line := range strings.SplitSeq(document, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, coverage.DirectiveMarker) {
			lines = append(lines, trimmed)
		}
	}
	return lines
}
