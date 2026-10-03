//go:build integration

package integration_test

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
)

// A PostgreSQL image may name its own POSTGRES_USER and run its init scripts as
// that user. The Supabase image declares supabase_admin, and with the variable
// overridden to postgres its init failed and the container exited, while the
// run waited two minutes and reported a dial timeout (stokaro/ptah#4053). The
// images below are that shape in miniature.

// imageUserInit runs as the image's own user and creates the role Ptah
// connects as. It fails when POSTGRES_USER was overridden, because then
// image_admin does not exist.
const imageUserInit = `psql -v ON_ERROR_STOP=1 -U image_admin -d postgres -c "CREATE ROLE postgres LOGIN SUPERUSER PASSWORD '$POSTGRES_PASSWORD'"
`

// imageUserMigration and imageUserSchema are a one-table project.
const (
	imageUserMigration = "CREATE TABLE t (id int NOT NULL, PRIMARY KEY (id));\n"
	imageUserSchema    = "CREATE TABLE t (id int NOT NULL, PRIMARY KEY (id));\n"
)

// dockerBlockProject writes an atlas.hcl whose docker block builds an image
// from dockerfile and initScript, a one-file migration directory and the
// schema it reaches, and returns the image tag the block builds. The working
// directory becomes the project's.
func dockerBlockProject(c *qt.C, dockerfile, initName, initScript, timeout string) string {
	c.Helper()
	c.Assert(devdocker.DockerCLI{}.Available(c.Context()), qt.IsNil)
	suffix := make([]byte, 6)
	_, err := rand.Read(suffix)
	c.Assert(err, qt.IsNil)
	image := "ptah-image-user-" + hex.EncodeToString(suffix) + ":1"
	c.Cleanup(func() {
		// #nosec G204 -- the tag is generated above.
		_ = exec.Command("docker", "rmi", "--force", image).Run()
	})
	dir := c.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(dir, "db"), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "db", "Dockerfile"), []byte(dockerfile), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "db", initName), []byte(initScript), 0o600), qt.IsNil)
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "20240101000000_t.sql"), []byte(imageUserMigration), 0o600), qt.IsNil)
	_, err = runCompatVerb("migrate", "hash", "--dir", "file://"+migrations)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(imageUserSchema), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`docker "postgres" "dev" {
  image    = "`+image+`"
  database = "dev"
  schema   = "public"
  timeout  = "`+timeout+`"
  build {
    context = "db"
  }
}

env "dev" {
  dev = docker.postgres.dev.url
  migration {
    dir = "file://migrations"
  }
  schema {
    src = "file://schema.sql"
  }
}
`), 0o600), qt.IsNil)
	c.Chdir(dir)
	return image
}

// TestCompatMigrateDiffStartsAnImageThatNamesItsOwnUserE2E builds an image
// whose POSTGRES_USER is image_admin and whose init script runs as that user.
// The container starts, and the replay reaches the desired state.
func TestCompatMigrateDiffStartsAnImageThatNamesItsOwnUserE2E(t *testing.T) {
	c := qt.New(t)
	image := dockerBlockProject(c,
		"FROM postgres:16-alpine\nENV POSTGRES_USER=image_admin\nCOPY --chmod=644 10-roles.sh /docker-entrypoint-initdb.d/\n",
		"10-roles.sh", imageUserInit, "2m")

	out, err := runCompatVerb("migrate", "diff", "--env", "dev", "next")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state, no changes to be made")
	c.Assert(imageExists(image), qt.IsFalse)
}

// TestCompatMigrateDiffReportsAnImageThatExitsDuringInitE2E builds an image
// whose init script exits. The block allows five minutes, and the run ends
// with the exit code and the script's own line instead of waiting them out.
// The container and the image are removed.
func TestCompatMigrateDiffReportsAnImageThatExitsDuringInitE2E(t *testing.T) {
	c := qt.New(t)
	image := dockerBlockProject(c,
		"FROM postgres:16-alpine\nCOPY --chmod=644 99-fail.sh /docker-entrypoint-initdb.d/\n",
		"99-fail.sh", "echo ptah-init-fails-here >&2\nexit 3\n", "5m")

	out, err := runCompatVerb("migrate", "diff", "--env", "dev", "next")

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, "the container stopped before the server answered: it exited with code 3")
	c.Assert(err.Error(), qt.Contains, "ptah-init-fails-here")
	c.Assert(err.Error(), qt.Not(qt.Contains), "timed out")
	c.Assert(imageExists(image), qt.IsFalse)
}
