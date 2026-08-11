package schema_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashclrender"
)

func TestSchemaInspectNativeHCLFraming_EmptySQLiteExactBytes(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "empty.db")

	out, err := runSchema("", "inspect", "--db-url", "sqlite://"+dbPath, "--format", "hcl")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals,
		atlashclrender.GeneratedCodeMarker+"\n\nschema \"main\" {\n}\n\n")
}
