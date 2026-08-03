package fsdurable

// White-box testing required: the recovery artifact is produced inside the
// commit primitive on a branch that only a failed swap-back reaches, which no
// public API can drive deterministically.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// Test_preserveDisplacedFile_HappyPath_UsesAStableVisibleName pins the naming
// contract, not just the fact that something survived. The only recovery
// artifact this package produced before was a hidden random dotfile, which is
// durable and useless: nobody can find it without knowing the error text.
func Test_preserveDisplacedFile_HappyPath_UsesAStableVisibleName(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	displacedPath := filepath.Join(dir, ".schema.hcl.tmp-XYZ")
	c.Assert(os.WriteFile(displacedPath, []byte("rival"), 0o600), qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	recovered, err := preserveDisplacedFile(root, ".schema.hcl.tmp-XYZ", "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(filepath.IsAbs(recovered), qt.IsTrue)
	name := filepath.Base(recovered)
	c.Assert(strings.HasPrefix(name, "schema.hcl.ptah-recovery-"), qt.IsTrue)
	c.Assert(strings.HasPrefix(name, "."), qt.IsFalse)
	c.Assert(strings.ContainsAny(name, `:\/`), qt.IsFalse)
	contents, err := os.ReadFile(recovered)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "rival")
	_, err = os.Stat(displacedPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}
