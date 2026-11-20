package skills

// White-box testing required: Ptah ships one skill of one file, so the order
// that makes a refusal leave no partial skill behind -- every file compared
// before any is written -- is observable within one skill only on a skill of
// several files, which only the unexported install can be given.

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
)

func TestInstall_RefusesBeforeWritingAnyFile(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"skills/multi/SKILL.md":          {Data: []byte("---\nname: multi\n---\n")},
		"skills/multi/references/a.md":   {Data: []byte("a\n")},
		"skills/multi/references/zzz.md": {Data: []byte("z\n")},
	}
	dir := t.TempDir()
	changed := filepath.Join(dir, "multi", "references", "zzz.md")
	c.Assert(os.MkdirAll(filepath.Dir(changed), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(changed, []byte("edited\n"), 0o600), qt.IsNil)

	installed, err := install(source, []Target{{Name: "multi", Dir: dir}}, InstallOptions{})

	c.Assert(err, qt.ErrorIs, ErrFileDiffers)
	c.Assert(installed, qt.IsNil)
	_, statErr := os.Stat(filepath.Join(dir, "multi", "SKILL.md"))
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
	_, statErr = os.Stat(filepath.Join(dir, "multi", "references", "a.md"))
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}

func TestInstall_WritesEveryFileOfTheSkill(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"skills/multi/SKILL.md":        {Data: []byte("---\nname: multi\n---\n")},
		"skills/multi/references/a.md": {Data: []byte("a\n")},
		"skills/other/SKILL.md":        {Data: []byte("---\nname: other\n---\n")},
	}
	dir := t.TempDir()

	installed, err := install(source, []Target{{Name: "multi", Dir: dir}}, InstallOptions{})

	c.Assert(err, qt.IsNil)
	c.Assert(installed, qt.DeepEquals, []InstalledFile{
		{Path: filepath.Join(dir, "multi", "SKILL.md"), Status: Written},
		{Path: filepath.Join(dir, "multi", "references", "a.md"), Status: Written},
	})
	reference, err := os.ReadFile(filepath.Join(dir, "multi", "references", "a.md"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(reference), qt.Equals, "a\n")
	_, statErr := os.Stat(filepath.Join(dir, "other"))
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}
