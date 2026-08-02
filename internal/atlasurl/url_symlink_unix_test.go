//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package atlasurl_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasurl"
)

func TestSameDatabase_SQLiteSymlinkAlias(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "dev.db")
	aliasPath := filepath.Join(dir, "dev-alias.db")
	c.Assert(os.WriteFile(databasePath, nil, 0o600), qt.IsNil)
	c.Assert(os.Symlink(databasePath, aliasPath), qt.IsNil)

	same, err := atlasurl.SameDatabase(
		"sqlite://"+databasePath,
		"sqlite://"+aliasPath,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(same, qt.IsTrue)
}
