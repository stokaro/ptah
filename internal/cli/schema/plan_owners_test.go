package schema_test

import (
	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
)

// withDirectives is plan with a directive header, its statements judged by
// the bundled runtime's owners.
func withDirectives(c *qt.C, plan atlasschema.PlanFile, directives []atlasschema.PlanDirective) atlasschema.PlanFile {
	c.Helper()
	decorated, err := plan.WithDirectiveHeader(c.Context(), must.Must(builtin.New()), directives)
	c.Assert(err, qt.IsNil)
	return decorated
}
