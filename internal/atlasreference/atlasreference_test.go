package atlasreference_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreference"
)

// Binary reports configured only when a path is actually there. The three cases
// are separate subtests rather than a table because "the variable is absent" is
// not a value a row can carry: expressing it alongside the others would take a
// conditional inside the test.
//
// The empty case is the one worth having. `PTAH_ATLAS_REFERENCE=` and an absent
// variable are different configuration states everywhere else in Ptah, and here
// they deliberately answer the same thing -- a conformance run has no reference
// binary either way, and reporting one that cannot be executed would fail later
// and further from the cause.
func TestBinary_HappyPath(t *testing.T) {
	t.Run("a configured path", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(atlasreference.EnvVar, "/opt/atlas/bin/atlas")

		path, ok := atlasreference.Binary()

		c.Assert(path, qt.Equals, "/opt/atlas/bin/atlas")
		c.Assert(ok, qt.IsTrue)
	})

	t.Run("an absent variable", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(atlasreference.EnvVar, "restored-by-cleanup")
		c.Assert(os.Unsetenv(atlasreference.EnvVar), qt.IsNil)

		path, ok := atlasreference.Binary()

		c.Assert(path, qt.Equals, "")
		c.Assert(ok, qt.IsFalse)
	})

	t.Run("a present but empty variable", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(atlasreference.EnvVar, "")

		path, ok := atlasreference.Binary()

		c.Assert(path, qt.Equals, "")
		c.Assert(ok, qt.IsFalse)
	})
}
