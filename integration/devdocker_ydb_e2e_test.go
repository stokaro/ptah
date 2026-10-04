//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
)

// TestDevDockerStartsYDBE2E replays a migration directory on a
// `docker://ydb/<tag>[/local]` dev database, on each certified YDB line, and
// finds no container left. The server is the run's own, so the replay runs a
// statement whose effect reaches the whole database, a user, which a dev
// realm in a database the operator named refuses, as
// TestYDBBinary_DevDatabaseIsARealm pins. The run waits out the image's
// readiness: local-ydb refuses a CREATE TABLE for about a second after it
// answers a query.
func TestDevDockerStartsYDBE2E(t *testing.T) {
	tests := []struct {
		name   string
		devURL string
	}{
		{name: "26.2", devURL: "docker://ydb/26.2.1.14/local"},
		{name: "25.1 with no database", devURL: "docker://ydb/25.1.4.7"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(devdocker.DockerCLI{}.Available(t.Context()), qt.IsNil)
			before := devDockerCensus(c)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			files := map[string]string{
				"0000000001_users.up.sql": "CREATE TABLE `app/users` (`id` Int64 NOT NULL, `name` Utf8, PRIMARY KEY (`id`));\n" +
					"CREATE USER ptahdevreader PASSWORD 'secret';\n",
				"0000000001_users.down.sql": "DROP TABLE `app/users`;\nDROP USER ptahdevreader;\n",
			}
			for name, body := range files {
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
			}
			hashed, hashErr := runPtahNativeWithError("migrations", "hash", "--dir", dir)
			c.Assert(hashErr, qt.IsNil, qt.Commentf("%s", hashed))

			out, err := runPtahNativeWithError("migrations", "validate", "--dir", dir, "--dev-url", test.devURL)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "OK: migration SQL validated on dev database")
			c.Assert(devDockerCensus(c), qt.DeepEquals, before)
		})
	}
}

// devDockerYDBDesired is a desired schema in HCL with one table more than the
// migration directory below creates.
const devDockerYDBDesired = `schema "app" {
}

table "users" {
  schema = schema.app
  column "id" {
    type = Int64
  }
  primary_key {
    columns = [column.id]
  }
}

table "orders" {
  schema = schema.app
  column "id" {
    type = Uint64
  }
  primary_key {
    columns = [column.id]
  }
}
`

// On the compatibility surface docker://ydb is a Ptah extension. The default
// mode starts it as the native binary does, and migrate diff replays the
// directory there and plans only the table the directory lacks; strict mode
// refuses the URL before anything starts, as
// TestStrictCompatRefusesDockerYDBAsTheBinaryDoes pins.
func TestDevDockerStartsYDBForTheCompatibilitySurfaceE2E(t *testing.T) {
	c := qt.New(t)
	c.Assert(devdocker.DockerCLI{}.Available(t.Context()), qt.IsNil)
	before := devDockerCensus(c)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_users.sql"),
		[]byte("CREATE TABLE `app/users` (`id` Int64 NOT NULL, PRIMARY KEY (`id`));\n"), 0o600), qt.IsNil)
	desired := filepath.Join(root, "desired.hcl")
	c.Assert(os.WriteFile(desired, []byte(devDockerYDBDesired), 0o600), qt.IsNil)
	dirURL := "file://" + filepath.ToSlash(dir)
	hashed, hashErr := runAtlasCompat("migrate", "hash", "--dir", dirURL)
	c.Assert(hashErr, qt.IsNil, qt.Commentf("%s", hashed))

	out, err := runAtlasCompat("migrate", "diff", "orders", "--dir", dirURL,
		"--to", "file://"+filepath.ToSlash(desired), "--dev-url", "docker://ydb/26.2.1.14/local")
	written, globErr := filepath.Glob(filepath.Join(dir, "*_orders.sql"))

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(globErr, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
	body, readErr := os.ReadFile(written[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(body), qt.Contains, "CREATE TABLE `app/orders`")
	c.Assert(string(body), qt.Not(qt.Contains), "`app/users`")
	c.Assert(devDockerCensus(c), qt.DeepEquals, before)
}
