package introspect_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/introspect"
)

func TestCanceledIntrospectionPublishesNoGoFiles(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	dir := c.TempDir()
	output := filepath.Join(dir, "generated")
	cmd := introspect.NewIntrospectCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--db-url", atlasurl.SQLiteURLFromPath(filepath.Join(dir, "db.sqlite")), "--out", output})
	err := cmd.ExecuteContext(ctx)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(stdout.String(), qt.Equals, "")
	_, err = os.Stat(output)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}
