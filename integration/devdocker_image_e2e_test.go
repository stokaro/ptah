//go:build integration

package integration_test

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/devdocker"
)

// These tests prove what parsing a `docker+<driver>://` dev URL cannot
// (stokaro/ptah#4040): the container runs the image the URL names, the database
// the URL names exists in it even when the image ignores the variable that
// names one, and the container is gone afterwards.
//
// The images are built here, so the proof does not depend on a registry. Each
// differs from the official image it starts from in two observable ways: it
// creates the account ptah_image_marker when it initializes, and its entrypoint
// unsets the variable naming the database, so the database the URL names is
// not created by the image. Measured on 2026-10-03, the pinned community binary
// v1.3.0 exits 1 with `database "dev" does not exist` on such a PostgreSQL
// image, and creates the database on such a MySQL image.

// devDockerImage is an image built from an engine's official one.
type devDockerImage struct {
	// from is the official image the build starts from. The MySQL tag is the
	// one go-integration-tests.yml already pulls for its service container.
	from string
	// unset is the variable the entrypoint removes.
	unset string
	// marker creates ptah_image_marker when the server initializes.
	marker string
	// cmd is the official image's own command.
	cmd string
}

// devDockerImages are the engines a `docker+` URL starts, keyed by driver.
var devDockerImages = map[string]devDockerImage{
	"postgres": {
		from:   "postgres:16-alpine",
		unset:  "POSTGRES_DB",
		marker: "CREATE ROLE ptah_image_marker;\n",
		cmd:    "postgres",
	},
	"mysql": {
		from:   "mysql:26.7",
		unset:  "MYSQL_DATABASE",
		marker: "CREATE USER 'ptah_image_marker'@'%';\n",
		cmd:    "mysqld",
	},
}

// buildDevDockerImage builds the image for driver and returns its tag. The tag
// is unique to the run, so a census by image finds this run's containers and no
// one else's, and the image is removed when the test ends.
//
// The files are written owner-only, so the image grants the init script to the
// server's own user, which runs it.
func buildDevDockerImage(c *qt.C, driver string) string {
	c.Helper()
	spec := devDockerImages[driver]
	dir := c.TempDir()
	dockerfile := "FROM " + spec.from + "\n" +
		"COPY marker.sql /docker-entrypoint-initdb.d/marker.sql\n" +
		"COPY entrypoint.sh /ptah-entrypoint.sh\n" +
		"RUN chmod 0644 /docker-entrypoint-initdb.d/marker.sql && chmod 0755 /ptah-entrypoint.sh\n" +
		"ENTRYPOINT [\"/ptah-entrypoint.sh\"]\n" +
		"CMD [\"" + spec.cmd + "\"]\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "marker.sql"), []byte(spec.marker), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "entrypoint.sh"),
		[]byte("#!/bin/sh\nunset "+spec.unset+"\nexec docker-entrypoint.sh \"$@\"\n"), 0o600), qt.IsNil)
	suffix := make([]byte, 6)
	_, err := rand.Read(suffix)
	c.Assert(err, qt.IsNil)
	tag := "ptah-devdocker-image-" + driver + "-" + hex.EncodeToString(suffix) + ":1"
	// #nosec G204 -- the tag is generated above and the context is a temporary directory.
	out, err := exec.Command("docker", "build", "--quiet", "--tag", tag, dir).CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Cleanup(func() {
		// #nosec G204 -- the tag is generated above.
		_ = exec.Command("docker", "rmi", "--force", tag).Run()
	})
	return tag
}

// containersFromImage lists every container, running or not, created from
// image, as the runtime reports them.
func containersFromImage(c *qt.C, image string) []string {
	c.Helper()
	// #nosec G204 -- the image tag is the one buildDevDockerImage generated.
	out, err := exec.Command("docker", "ps", "--all", "--quiet", "--filter", "ancestor="+image).Output()
	c.Assert(err, qt.IsNil)
	return strings.Fields(string(out))
}

func TestDevDockerProvisionsTheImageAnImageURLNamesE2E(t *testing.T) {
	tests := []struct {
		driver string
		// database names the database the session is in.
		database string
		// markers counts the accounts named ptah_image_marker.
		markers string
	}{
		{
			driver:   "postgres",
			database: "SELECT current_database()",
			markers:  "SELECT count(*) FROM pg_roles WHERE rolname = 'ptah_image_marker'",
		},
		{
			driver:   "mysql",
			database: "SELECT DATABASE()",
			markers:  "SELECT count(*) FROM mysql.user WHERE user = 'ptah_image_marker'",
		},
	}
	for _, test := range tests {
		t.Run(test.driver, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(devdocker.DockerCLI{}.Available(t.Context()), qt.IsNil)
			image := buildDevDockerImage(c, test.driver)

			resolved, release, err := devdocker.Resolve(t.Context(),
				"docker+"+test.driver+"://_/"+image+"/ptahimage", devdocker.Options{})
			c.Assert(err, qt.IsNil)
			c.Cleanup(release)
			c.Assert(containersFromImage(c, image), qt.HasLen, 1)

			conn, err := dbschema.ConnectToDatabase(t.Context(), resolved)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = conn.Close() })
			var database string
			c.Assert(conn.QueryRowContext(t.Context(), test.database).Scan(&database), qt.IsNil)
			c.Assert(database, qt.Equals, "ptahimage")
			var markers int
			c.Assert(conn.QueryRowContext(t.Context(), test.markers).Scan(&markers), qt.IsNil)
			c.Assert(markers, qt.Equals, 1)

			c.Assert(conn.Close(), qt.IsNil)
			release()
			c.Assert(containersFromImage(c, image), qt.HasLen, 0)
		})
	}
}

// TestDevDockerImageURLReachesEveryReplayingVerbE2E runs a replaying verb of
// each binary with the image as its dev database. The migration grants on the
// role the image creates, so a run on any other image fails the replay.
func TestDevDockerImageURLReachesEveryReplayingVerbE2E(t *testing.T) {
	tests := []struct {
		name string
		// run is the binary the row drives, in process.
		run  func(args ...string) (string, error)
		args []string
	}{
		{
			name: "ptah-compat migrate diff",
			run:  runCompatVerb,
			args: []string{"migrate", "diff", "--dir", "file://{dir}", "--to", "file://{schema}", "--dev-url", "{dev}"},
		},
		{
			name: "ptah migrations validate",
			run:  runPtahNativeWithError,
			args: []string{"migrations", "validate", "--dir", "{dir}", "--dir-format", "atlas", "--dev-url", "{dev}"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(devdocker.DockerCLI{}.Available(t.Context()), qt.IsNil)
			image := buildDevDockerImage(c, "postgres")
			root := c.TempDir()
			dir := filepath.Join(root, "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			const body = "CREATE TABLE widgets (id integer NOT NULL, PRIMARY KEY (id));\n" +
				"GRANT SELECT ON widgets TO ptah_image_marker;\n"
			c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_init.sql"), []byte(body), 0o600), qt.IsNil)
			_, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
			c.Assert(err, qt.IsNil)
			schema := filepath.Join(root, "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(body), 0o600), qt.IsNil)
			replacer := strings.NewReplacer(
				"{dir}", dir, "{schema}", schema, "{dev}", "docker+postgres://_/"+image+"/ptahimage",
			)
			args := make([]string, len(test.args))
			for i, arg := range test.args {
				args[i] = replacer.Replace(arg)
			}

			out, err := test.run(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(containersFromImage(c, image), qt.HasLen, 0)
		})
	}
}
