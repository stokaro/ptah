package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

func TestSchemaInspectCompatibilityHCLFraming_EmptySQLiteExactBytes(t *testing.T) {
	envbooltest.Unset(atlashclrender.KeepAtlasRefusedBlocksEnvVar)(t)
	tests := []struct {
		name string
		args []string
	}{
		{name: "default"},
		{name: "hcl name", args: []string{"--format", "hcl"}},
		{name: "hcl helper", args: []string{"--format", `{{ hcl . }}`}},
		{name: "MarshalHCL method", args: []string{"--format", `{{ $.MarshalHCL }}`}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "empty.db")

			out, err := runSchemaInspectOutput(c, dbPath, test.args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Equals, "schema \"main\" {\n}\n")
		})
	}
}

func TestSchemaInspectCompatibilityHCLFraming_AllBlocksOptInKeepsFraming(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "1")(t)
	dbPath := filepath.Join(c.TempDir(), "empty.db")

	out, err := runSchemaInspectOutput(c, dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "schema \"main\" {\n}\n")
}

func TestSchemaInspectCompatibilityHCLFraming_OutputFileUsesExactBytes(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(atlashclrender.KeepAtlasRefusedBlocksEnvVar)(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "schema.hcl")

	out, err := runSchemaInspectOutput(c, filepath.Join(dir, "empty.db"), "--output", target)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "")
	written, err := os.ReadFile(target)
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Equals, "schema \"main\" {\n}\n")
}
