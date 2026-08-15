//go:build !windows

package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/atlasschema"
)

func TestSchemaPlanExplicitOutputReplacesSymlinkInsteadOfFollowingIt(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	victimPath := filepath.Join(dir, "victim")
	outputPath := filepath.Join(dir, "explicit.plan.json")
	c.Assert(os.WriteFile(victimPath, []byte("protected\n"), 0o600), qt.IsNil)
	c.Assert(os.Symlink(victimPath, outputPath), qt.IsNil)
	fixture := newPlanFixture(c, "atomic-explicit", "", `CREATE TABLE explicit_users (id INTEGER PRIMARY KEY);`)

	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", outputPath)...)

	c.Assert(err, qt.IsNil)
	victim, err := os.ReadFile(victimPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(victim), qt.Equals, "protected\n")
	info, err := os.Lstat(outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().IsRegular(), qt.IsTrue)
	_, err = atlasschema.ReadPlanFile(outputPath)
	c.Assert(err, qt.IsNil)
}

func TestSchemaPlanDefaultOutputRefusesSymlinkWithoutFollowingIt(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	victimPath := filepath.Join(dir, "victim")
	outputPath := filepath.Join(dir, "guarded.plan.hcl")
	c.Assert(os.WriteFile(victimPath, []byte("protected\n"), 0o600), qt.IsNil)
	c.Assert(os.Symlink(victimPath, outputPath), qt.IsNil)
	fixture := newPlanFixture(c, "atomic-default", "", `CREATE TABLE guarded_users (id INTEGER PRIMARY KEY);`)

	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--save", "--name", "guarded")...)

	c.Assert(err, qt.ErrorMatches, `plan file guarded\.plan\.hcl already exists; .*`)
	victim, err := os.ReadFile(victimPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(victim), qt.Equals, "protected\n")
	info, err := os.Lstat(outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode()&os.ModeSymlink, qt.Equals, os.ModeSymlink)
}
