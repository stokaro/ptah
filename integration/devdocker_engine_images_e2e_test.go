//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
)

// TestDevDockerStartsThePostGISAndPgvectorImagesE2E replays a migration that
// creates each image's extension on `docker://postgis/<tag>/dev` and
// `docker://pgvector/<tag>/dev`, which the pinned community binary v1.3.0
// starts as PostgreSQL (stokaro/ptah#4066).
//
// The migration says CREATE EXTENSION without IF NOT EXISTS, so it fails where
// the image already created the extension in the dev database. The PostGIS
// image does that, with the tiger and topology schemas, in the database
// POSTGRES_DB names, which is why the binary leaves that variable unset and
// creates the database itself; a dev database the image filled would also be
// refused as not clean before the replay. The run exits 0 only on a database
// created empty.
func TestDevDockerStartsThePostGISAndPgvectorImagesE2E(t *testing.T) {
	tests := []struct {
		name   string
		devURL string
		// image is the image the URL starts, for the census.
		image     string
		migration string
	}{
		{
			name:      "postgis",
			devURL:    "docker://postgis/16-3.4/dev",
			image:     "postgis/postgis:16-3.4",
			migration: "CREATE EXTENSION postgis;\nCREATE TABLE places (id integer PRIMARY KEY, location geometry(Point, 4326));\n",
		},
		{
			name:      "pgvector",
			devURL:    "docker://pgvector/pg16/dev",
			image:     "pgvector/pgvector:pg16",
			migration: "CREATE EXTENSION vector;\nCREATE TABLE items (id integer PRIMARY KEY, embedding vector(3));\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(devdocker.DockerCLI{}.Available(t.Context()), qt.IsNil)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_extension.sql"), []byte(test.migration), 0o600), qt.IsNil)
			_, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
			c.Assert(err, qt.IsNil)

			out, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", test.devURL)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(containersFromImage(c, test.image), qt.HasLen, 0)
		})
	}
}
