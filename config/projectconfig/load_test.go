package projectconfig_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
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

func TestLoadPreservesIgnoredAtlasConstructsAfterMerge(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	ptahPath := filepath.Join(dir, projectconfig.PtahFileName)
	atlasPath := filepath.Join(dir, projectconfig.AtlasFileName)
	c.Assert(os.WriteFile(ptahPath, []byte("url: sqlite://ptah.db\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(atlasPath, []byte(`env "local" {
  url     = "sqlite://atlas.db"
  project = "ignored"
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.Load(projectconfig.LoadOptions{
		PtahPath:  ptahPath,
		AtlasPath: atlasPath,
		EnvName:   "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://atlas.db")
	c.Assert(cfg.IgnoredConstructs, qt.DeepEquals, []projectconfig.IgnoredAtlasConstruct{
		{Name: "project", Kind: "attribute", Filename: atlasPath, Line: 3},
	})
}
