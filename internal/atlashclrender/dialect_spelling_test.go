package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

// TestRenderInspectedForAtlasCLI_FoldsTheDialectSpelling pins that every
// spelling of a dialect renders the document its canonical name renders. A
// caller that names PostgreSQL as `pgx`, or with padding and capitals, gets the
// blocks the pinned community binary refuses left out, the same diagnostics
// and the same not-described record, rather than a document that binary
// cannot read.
func TestRenderInspectedForAtlasCLI_FoldsTheDialectSpelling(t *testing.T) {
	c := qt.New(t)
	canonical, err := atlashclrender.RenderInspectedForAtlasCLI(inspectedRichDatabase(), "public",
		atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	c.Assert(string(canonical.Data), qt.Not(qt.Contains), `extension "`)

	for _, spelling := range []string{"pgx", " POSTGRES ", "PostgreSQL", "postgresql"} {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlashclrender.RenderInspectedForAtlasCLI(inspectedRichDatabase(), "public",
				atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: spelling})

			c.Assert(err, qt.IsNil)
			c.Assert(string(got.Data), qt.Equals, string(canonical.Data))
			c.Assert(got.Diagnostics, qt.DeepEquals, canonical.Diagnostics)
			c.Assert(got.NotDescribed, qt.DeepEquals, canonical.NotDescribed)
		})
	}
}
