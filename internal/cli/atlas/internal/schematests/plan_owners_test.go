package schematests_test

import (
	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
)

// readPlanDocument reads a plan file, its statements judged by the bundled
// runtime's owners.
func readPlanDocument(c *qt.C, path string) (atlasschema.PlanFile, atlasschema.PlanFormat, error) {
	c.Helper()
	return atlasschema.ReadPlanDocument(c.Context(), must.Must(builtin.New()), path)
}

// withStatements is plan with the statements of sqlText, judged by the
// bundled runtime's owners.
func withStatements(c *qt.C, plan atlasschema.PlanFile, sqlText string) atlasschema.PlanFile {
	c.Helper()
	edited, err := plan.WithStatementsFromSQL(c.Context(), must.Must(builtin.New()), sqlText)
	c.Assert(err, qt.IsNil)
	return edited
}
