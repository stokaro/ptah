package atlas

// White-box testing required: this test verifies the unexported Atlas argument
// mapper's config snapshot boundary between project evaluation and native
// consumption; no exported API exposes a deterministic hook at that point.

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdadapter"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
)

func TestAtlasArgMapperPreservesProjectConfigSnapshot(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	c.Assert(os.WriteFile("ptah.yaml", []byte(`env:
  local:
    migration:
      pre_down_hook: "generation-one-hook"
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://generation-one.db"
  migration {
    dir = "file://generation-one-migrations"
  }
}
`), 0o600), qt.IsNil)

	mapperCommand := &cobra.Command{Use: "down"}
	mapperCommand.SetContext(t.Context())
	mapper := atlasArgMapper("migrate", atlasMigrateDownVerb())
	cleanup := &cmdadapter.CleanupScope{}
	_, snapshotContext, err := mapper(mapperCommand, []string{
		"--config", "file://atlas.hcl",
		"--env", "local",
	}, cleanup)
	c.Assert(err, qt.IsNil)

	c.Assert(os.WriteFile("ptah.yaml", []byte(`env:
  local:
    migration:
      pre_down_hook: "generation-two-hook"
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://generation-two.db"
  migration {
    dir = "file://generation-two-migrations"
  }
}
`), 0o600), qt.IsNil)

	nativeCommand := &cobra.Command{Use: "down"}
	nativeCommand.SetContext(snapshotContext)
	loaded, err := dbcli.LoadProjectConfig(nativeCommand, "ptah.yaml")
	c.Assert(err, qt.IsNil)
	c.Assert(loaded.DatabaseURL, qt.Equals, "sqlite://generation-one.db")
	c.Assert(loaded.Migration.Dir, qt.Equals, "generation-one-migrations")
	c.Assert(loaded.Migration.PreDownHook, qt.Equals, "generation-one-hook")
	c.Assert(cleanup.Close(), qt.IsNil)
}
