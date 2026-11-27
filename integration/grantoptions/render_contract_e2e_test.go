//go:build integration

package grantoptions_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// SQL Server needs CASCADE in the execution script, but desired-state input
// must keep refusing a cascade whose onward grants it cannot inspect. The
// original declaration remains the source for another comparison or render.
func TestSchemaRenderSQLServerKeepsCascadeOutOfDesiredStateE2E(t *testing.T) {
	c := qt.New(t)
	document := "GRANT SELECT ON t1 TO reader WITH GRANT OPTION;\n" +
		"REVOKE GRANT OPTION FOR SELECT ON t1 FROM reader;\n"
	rendered := renderOptionDocument(c, "sqlserver", document)
	c.Assert(rendered, qt.Contains, "REVOKE GRANT OPTION FOR SELECT ON [t1] FROM [reader] CASCADE;")

	reread := ptah(c, "schema", "render", "--dialect", "sqlserver", "--schema-file", schemaFile(c, rendered))

	c.Assert(reread.ExitCode, qt.Equals, 2, outcome(reread))
	c.Assert(reread.Stderr, qt.Contains, "REVOKE ... CASCADE is not supported")
	c.Assert(reread.Stdout, qt.Equals, "")
}
