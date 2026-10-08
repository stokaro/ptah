package schema_test

import (
	"bytes"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/cli/schema"
)

func TestSchemaValidateCancellationIsNotASchemaProblem(t *testing.T) {
	c := qt.New(t)
	source := writeSchemaSQLFile(c, t.TempDir(), "schema.sql", "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n")
	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"validate", "--schema-file", source, "--dialect", "postgres"})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := cmd.ExecuteContext(ctx)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(out.String(), qt.Not(qt.Contains), "structural problem")
}
