package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/internal/atlashcl"
)

// securityDocument is a view whose block holds security, as Atlas documents it
// for a PostgreSQL view.
func securityDocument(security string) []byte {
	return []byte(`
schema "public" {}

view "report" {
  schema = schema.public
  as     = "SELECT 1 AS v"
  ` + security + `
}
`)
}

// TestParseViewSecurity_HappyPath pins that a view's security block declares
// PostgreSQL's security_invoker and security_barrier: invoker and barrier are
// the two attributes the Atlas PostgreSQL HCL reference documents for it, each
// a bool, at least one of them set (stokaro/ptah#5017).
func TestParseViewSecurity_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		security string
		want     []string
	}{
		{name: "invoker", security: "security {\n invoker = true\n}", want: []string{"security_invoker=true"}},
		{name: "barrier", security: "security {\n barrier = true\n}", want: []string{"security_barrier=true"}},
		{name: "both, one of them false", security: "security {\n barrier = true\n invoker = false\n}",
			want: []string{"security_invoker=false", "security_barrier=true"}},
		{name: "no block", security: "", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse(t.Context(), securityDocument(test.security), "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(db.Views, qt.HasLen, 1)
			c.Assert(db.Views[0].Options, qt.DeepEquals, test.want)
		})
	}
}

// TestParseViewSecurity_FailurePath pins the security blocks the reader
// refuses rather than reading as a view without options.
func TestParseViewSecurity_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		security string
		wantErr  string
	}{
		{name: "neither attribute", security: "security {}", wantErr: `(?s).*view "report" security block sets neither invoker nor barrier.*`},
		{name: "two blocks", security: "security {\n invoker = true\n}\nsecurity {\n barrier = true\n}",
			wantErr: `(?s).*view "report" declares more than one security block.*`},
		{name: "a label", security: "security \"x\" {\n invoker = true\n}", wantErr: `(?s).*view "report" security block takes no label.*`},
		{name: "a value that is not a bool", security: "security {\n invoker = \"yes\"\n}",
			wantErr: `(?s).*view security attribute "invoker" must be a bool.*`},
		{name: "an attribute the block does not have", security: "security {\n invoker = true\n definer = true\n}",
			wantErr: `(?s).*unsupported view security attribute "definer".*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse(t.Context(), securityDocument(test.security), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseMaterializedView_StorageParametersAreNotDescribed pins that an HCL
// document says it does not describe a materialized view's storage
// parameters: the block has no attribute for them, so a comparison takes the
// ones the database holds rather than planning their removal. A plain view's
// options are described, by its security block or by its absence.
func TestParseMaterializedView_StorageParametersAreNotDescribed(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(t.Context(), []byte(`
schema "public" {}

materialized "totals" {
  schema = schema.public
  as     = "SELECT 1 AS v"
}

view "report" {
  schema = schema.public
  as     = "SELECT 1 AS v"
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.NotDescribed.Describes(coverage.MaterializedViewOptions, "public.totals"), qt.IsFalse)
	c.Assert(db.NotDescribed.Describes(coverage.MaterializedViewOptions, "public.other"), qt.IsTrue)
	c.Assert(db.NotDescribed.Describes(coverage.View, "public.report"), qt.IsTrue)
	c.Assert(db.NotDescribed.Describes(coverage.Extension), qt.IsTrue)
}
