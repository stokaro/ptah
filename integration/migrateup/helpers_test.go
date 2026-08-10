//go:build integration

package migrateup_test

import (
	"os"
	"path/filepath"

	qt "github.com/frankban/quicktest"
)

func writeLintGateMigration(c *qt.C, dir, name, contents string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600), qt.IsNil)
}
