//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

func TestSchemaPlanValidateRefusesDevURLSymlinkToTarget(t *testing.T) {
	c := qt.New(t)
	fixture := newValidateFixture(c.TB, "validate-devurl-symlink",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")
	execOnTarget(c.TB, fixture.dbURL, `INSERT INTO keep_me (id) VALUES (1), (2), (3);`)
	beforeFingerprint := targetSchemaFingerprint(c.TB, fixture.dbURL)
	aliasPath := filepath.Join(filepath.Dir(fixture.dbPath), "target-symlink.db")
	c.Assert(os.Symlink(fixture.dbPath, aliasPath), qt.IsNil)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
		fixture.validateArgs("--dev-url", sqliteURLFromPath(aliasPath))...)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "--dev-url must not point at the target database")
	c.Assert(targetSchemaFingerprint(c.TB, fixture.dbURL), qt.Equals, beforeFingerprint)
	c.Assert(countRows(c.TB, fixture.dbPath, "keep_me"), qt.Equals, 3)
}
