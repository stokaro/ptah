package root_test

import (
	"bytes"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/engine"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/cli/root"
)

func TestSchemaInspectHonorsTheSelectedRuntime(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New()
	c.Assert(err, qt.IsNil)
	ctx, err := engineruntime.With(c.Context(), runtime)
	c.Assert(err, qt.IsNil)
	dbURL := atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "target.db"))
	cmd := root.NewRootCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	code := root.RunContext(ctx, cmd, "schema", "inspect", "--db-url", dbURL)
	c.Assert(code, qt.Equals, 2)
	c.Assert(out.String(), qt.Equals, "")
	c.Assert(diagnostics.String(), qt.Contains, ptaherr.ErrUnsupportedDialect.Error())
	_, _, code = runRoot(c, "schema", "inspect", "--db-url", dbURL)
	c.Assert(code, qt.Equals, 0)
}
