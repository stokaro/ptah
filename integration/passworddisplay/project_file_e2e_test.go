//go:build integration

package passworddisplay_test

import (
	"os"
	"path/filepath"

	qt "github.com/frankban/quicktest"
)

// writeProjectFile writes an atlas.hcl naming address as the target and the dev
// database of the environment `local`.
func writeProjectFile(c *qt.C, dir, address string) {
	c.Helper()
	content := "env \"local\" {\n  url = \"" + address + "\"\n  dev = \"" + address + "\"\n}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(content), 0o600), qt.IsNil)
}
