package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/schemamodel"
)

// TestParseSource_ADeclaredSchemaReachesTheObjectName is the gap this closes:
// the same object placed in a named schema is expressible in atlas.hcl and was
// not expressible in a Go annotation at all.
//
// `internal/atlashcl` accepts a `schema` attribute on `function`, `view` and
// `materialized` and folds it into the name with tableref.Canonical. The Go
// annotation registry had `schema` on sequence, domain, composite and range and
// on none of these, and an unregistered attribute is a hard ParseError rather
// than a silent drop -- so `schema="app"` on a function FAILED THE BUILD while
// the identical HCL project worked (stokaro/ptah#1270).
//
// The expected values are literal rather than computed from tableref.Canonical:
// asserting a fold against the function that performs it would pass for any
// spelling it happened to produce.
func TestParseSource_ADeclaredSchemaReachesTheObjectName(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		want       string
		why        string
	}{
		{
			name:       "function",
			annotation: `//ptah:schema:function name="fn" schema="app" body="BEGIN END;"`,
			want:       "app.fn",
			why:        "the qualified spelling atlas.hcl already produces for the same declaration",
		},
		{
			name:       "procedure",
			annotation: `//ptah:schema:procedure name="pr" schema="app" body="BEGIN END;"`,
			want:       "app.pr",
			why:        "a procedure is validated against its own directive, so the attribute has to be registered twice",
		},
		{
			name:       "view",
			annotation: `//ptah:schema:view name="v" schema="app" body="SELECT 1"`,
			want:       "app.v",
			why:        "",
		},
		{
			name:       "matview",
			annotation: `//ptah:schema:matview name="m" schema="app" body="SELECT 1"`,
			want:       "app.m",
			why:        "",
		},
		{
			// The controls. Without them every row above would pass an
			// implementation that qualified unconditionally, and an optional
			// attribute that re-spells the declarations not using it is worse
			// than the gap it closes.
			name:       "function, no schema",
			annotation: `//ptah:schema:function name="fn" body="BEGIN END;"`,
			want:       "fn",
			why:        "a declaration naming no schema keeps its name byte for byte",
		},
		{
			name:       "view, no schema",
			annotation: `//ptah:schema:view name="v" body="SELECT 1"`,
			want:       "v",
			why:        "as above",
		},
		{
			name:       "matview, no schema",
			annotation: `//ptah:schema:matview name="m" body="SELECT 1"`,
			want:       "m",
			why:        "as above",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := goschema.ParseSource("fixture.go",
				"package m\n\n"+test.annotation+"\ntype S struct{}\n")

			c.Assert(err, qt.IsNil)
			c.Assert(objectNames(database), qt.DeepEquals, []string{test.want},
				qt.Commentf("%s", test.why))
		})
	}
}

// objectNames is every routine, view and materialized-view name the parse
// produced, so one helper serves all four declaration kinds.
func objectNames(database schemamodel.Database) []string {
	names := make([]string, 0, 1)
	for _, function := range database.Functions {
		names = append(names, function.Name)
	}
	for _, view := range database.Views {
		names = append(names, view.Name)
	}
	for _, view := range database.MaterializedViews {
		names = append(names, view.Name)
	}
	return names
}
