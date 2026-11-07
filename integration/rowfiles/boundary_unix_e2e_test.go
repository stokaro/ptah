//go:build integration && !windows

package rowfiles_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestReadingVerbs_RefuseASymlinkLeavingTheProject is the shape a checkout or an
// unpacked archive can carry and no spelling reveals: the name stays inside and
// the bytes do not.
func TestReadingVerbs_RefuseASymlinkLeavingTheProject(t *testing.T) {
	for _, verb := range readingVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			p := newProject(c, "link.yaml")
			c.Assert(os.Symlink(p.secret, filepath.Join(p.dir, "pulled", "link.yaml")), qt.IsNil)
			c.Assert(os.Symlink(p.secret, filepath.Join(p.dir, "goent", "link.yaml")), qt.IsNil)

			run := p.run(c, verb.target, verb.args...)

			c.Assert(run.ExitCode, qt.Equals, verb.refusedExit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, "outside allowed root")
			c.Assert(run.Stdout+run.Stderr, qt.Not(qt.Contains), secretValue)
			c.Assert(p.registry.requests.Load(), qt.Equals, int64(0))
			c.Assert(p.rowCount(c), qt.Equals, 0)
		})
	}
}
