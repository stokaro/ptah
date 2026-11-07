//go:build integration

package passworddisplay_test

import (
	"os"
	"path/filepath"
	"strings"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// password holds every character that ends a password early for some parser.
// Each part between them is one a reader could recognize, and "%Ty" is what
// net/url quotes when it refuses the "%".
//
// #nosec G101 -- a made-up password these tests look for, not a credential
const password = "Zq7Kp@Xk9Lm/Wm3Nt#Vp8Qr%Ty4Hs:Rn6Jd?Lh2Fb"

// passwordParts are what must not be in anything a run writes.
var passwordParts = []string{"Zq7Kp", "Xk9Lm", "Wm3Nt", "Vp8Qr", "%Ty", "Ty4Hs", "Rn6Jd", "Lh2Fb"}

// fixtureVersion is the version of the one migration of the fixture's native
// directory.
const fixtureVersion = "1794028789"

// newWorkspace writes what the verbs read: models, a schema file, a migration
// directory in each layout, tests, seeds and a script. It returns the directory
// they run in.
func newWorkspace(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"models/model.go": `package models

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`,
		"schema.sql": "CREATE TABLE products (id INTEGER PRIMARY KEY);\n",
		"native/" + fixtureVersion + "_init.up.sql":   "CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n",
		"native/" + fixtureVersion + "_init.down.sql": "DROP TABLE gadgets;\n",
		"migrations/20260101000000_init.sql":          "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		"tests/t.test.hcl": `test "migrate" "smoke" {
  exec {
    sql = "SELECT 1"
  }
}
test "schema" "smoke" {
  exec {
    sql = "SELECT 1"
  }
}
`,
		"checks/c.sql":           "-- +ptah check name=\"ready\" assert=\"SELECT 1\"\nSELECT 1;\n",
		"seeds/001_seed.dev.sql": "SELECT 1;\n",
		"script.hcl": `script "query" "one" {
  sql = "SELECT 1"
}
`,
	}
	for name, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o750), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	}
	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir}, "migrate", "hash", "--dir", "file://migrations")
	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s", got.Stderr))
	got = clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, "migrations", "hash", "--dir", "native")
	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s", got.Stderr))
	return dir
}

// withAddress is args with the address where ADDRESS stands.
func withAddress(args []string, address string) []string {
	replaced := make([]string, 0, len(args))
	for _, arg := range args {
		replaced = append(replaced, strings.ReplaceAll(arg, "ADDRESS", address))
	}
	return replaced
}

// assertNoPassword holds text to having no part of the password.
func assertNoPassword(c *qt.C, text string) {
	c.Helper()
	for _, part := range passwordParts {
		c.Assert(text, qt.Not(qt.Contains), part)
	}
}

// assertNoPasswordInFiles holds every file under dir to having no part of the
// password.
func assertNoPasswordInFiles(c *qt.C, dir string) {
	c.Helper()
	var paths []string
	c.Assert(filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr == nil && !entry.IsDir() {
			paths = append(paths, path)
		}
		return walkErr
	}), qt.IsNil)
	for _, path := range paths {
		content, err := os.ReadFile(path) // #nosec G304 -- a file of this test's own temporary directory
		c.Assert(err, qt.IsNil)
		for _, part := range passwordParts {
			c.Assert(string(content), qt.Not(qt.Contains), part, qt.Commentf("file %s", path))
		}
	}
}
