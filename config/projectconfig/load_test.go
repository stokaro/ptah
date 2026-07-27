package projectconfig_test

import (
	"io/fs"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config/projectconfig"
)

func TestLoadExplicitMissingPtahConfigFails(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "missing.yaml")

	_, err := projectconfig.Load(projectconfig.LoadOptions{PtahPath: path})

	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	c.Assert(err, qt.ErrorMatches, `failed to read ptah config .*missing\.yaml: .*no such file or directory`)
}

func TestLoadConventionalMissingProjectConfigsAreOptional(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())

	cfg, err := projectconfig.Load(projectconfig.LoadOptions{})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "")
	c.Assert(cfg.OnlineDDL, qt.DeepEquals, projectconfig.OnlineDDLConfig{})
}

func TestLoadPtahFileMissingRemainsOptional(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "missing.yaml")

	cfg, err := projectconfig.LoadPtahFile(path, "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "")
	c.Assert(cfg.OnlineDDL, qt.DeepEquals, projectconfig.OnlineDDLConfig{})
}
