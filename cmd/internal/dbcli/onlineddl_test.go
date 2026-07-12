package dbcli_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
)

func TestLoadOnlineDDLConfig(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "ptah.yaml")
	c.Assert(os.WriteFile(path, []byte("online_ddl:\n  tool: ghost\n  threshold_rows: 500\n"), 0o600), qt.IsNil)

	cfg, err := dbcli.LoadOnlineDDLConfig(path)
	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Tool, qt.Equals, "ghost")
	c.Assert(cfg.ThresholdRows, qt.Equals, int64(500))
}

func TestLoadOnlineDDLConfig_ExplicitMissingPathIsAnError(t *testing.T) {
	c := qt.New(t)

	// An explicit --config path that does not exist is a usage error — unlike
	// the conventional ./ptah.yaml, whose absence is fine (covered by
	// onlineddl.TestLoadConfig_MissingFileYieldsDisabledConfig).
	_, err := dbcli.LoadOnlineDDLConfig(filepath.Join(t.TempDir(), "absent.yaml"))
	c.Assert(err, qt.ErrorMatches, "ptah config .*absent.yaml.*")
}
