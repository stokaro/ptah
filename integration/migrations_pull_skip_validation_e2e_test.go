//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/migrationartifact"
	"ptah.run/internal/ociartifact"
)

const (
	skipValidationUp   = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"
	skipValidationDown = "DROP TABLE widgets;\n"
	skipValidationSeed = "INSERT INTO widgets VALUES (1);\n"
)

// skipValidationMigration is one migration in the Ptah format.
func skipValidationMigration() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_create_widgets.up.sql":   {Data: []byte(skipValidationUp)},
		"0000000001_create_widgets.down.sql": {Data: []byte(skipValidationDown)},
	}
}

// publishMigrationLayers publishes exactly the files it is given under a
// migration artifact's manifest, with the format annotation it is given, and
// returns the manifest digest. A real push reads the directory and refuses a
// file the format does not read, so this is how such an artifact reaches a
// registry, as one an older Ptah pushed does.
func publishMigrationLayers(c *qt.C, reference, artifactType, format string, files fstest.MapFS) digest.Digest {
	c.Helper()
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	pushed, err := client.Push(c.Context(), reference, files, ociartifact.PushOptions{
		ArtifactType:   artifactType,
		LayerMediaType: migrationartifact.LayerMediaType,
		Annotations:    map[string]string{"io.stokaro.ptah.migration-format": format},
	})
	c.Assert(err, qt.IsNil)
	return pushed.Descriptor.Digest
}

// skipValidationReference names a repository nothing else in this run writes to.
func skipValidationReference(registry, name string) string {
	return fmt.Sprintf("oci://%s/ptah/skip-validation-%s-%d:v1", registry, name, time.Now().UnixNano())
}

// TestMigrationsPullSkipValidationMovesAnUnreadableDirectoryToTheNextCommandE2E
// is the measurement the flag exists for, through the shipped binary against a
// real registry: the step holding registry credentials writes the directory,
// and the step that reads it is the one that says why it cannot.
func TestMigrationsPullSkipValidationMovesAnUnreadableDirectoryToTheNextCommandE2E(t *testing.T) {
	c := qt.New(t)
	registry := requiredOCIRegistry(t)
	reference := skipValidationReference(registry, "stray")
	files := skipValidationMigration()
	files["seed.sql"] = &fstest.MapFile{Data: []byte(skipValidationSeed)}
	published := publishMigrationLayers(c, reference, ociartifact.MigrationArtifactType, "ptah", files)
	dir := c.TempDir()

	// The default still reads the directory, and writes nothing.
	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "pull", reference, "--out", "default", "--plain-http", "--json")
	c.Assert(refused.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, "validate migration artifact: migration file the Ptah format does not read: seed.sql")
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)

	// The flag writes the published bytes and says it did not read them.
	pulled := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "pull", reference, "--out", "migrations", "--plain-http", "--skip-validation", "--json")
	c.Assert(pulled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", pulled.Stdout, pulled.Stderr))
	var report struct {
		Digest      string `json:"digest"`
		Format      string `json:"format"`
		Unvalidated bool   `json:"unvalidated"`
	}
	c.Assert(json.Unmarshal([]byte(pulled.Stdout), &report), qt.IsNil, qt.Commentf("%s", pulled.Stdout))
	c.Assert(report.Digest, qt.Equals, published.String())
	c.Assert(report.Format, qt.Equals, "ptah")
	c.Assert(report.Unvalidated, qt.IsTrue)
	c.Assert(readSkipParseFile(c, filepath.Join(dir, "migrations", "0000000001_create_widgets.up.sql")), qt.Equals, skipValidationUp)
	c.Assert(readSkipParseFile(c, filepath.Join(dir, "migrations", "0000000001_create_widgets.down.sql")), qt.Equals, skipValidationDown)
	c.Assert(readSkipParseFile(c, filepath.Join(dir, "migrations", "seed.sql")), qt.Equals, skipValidationSeed)

	// The next command is the one that says why it cannot read the directory.
	status := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "status", "--migrations-dir", "migrations", "--dir-format", "ptah", "--json",
		"--db-url", atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db")))
	c.Assert(status.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", status.Stdout, status.Stderr))
	var statusReport struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	c.Assert(json.Unmarshal([]byte(status.Stdout), &statusReport), qt.IsNil, qt.Commentf("%s", status.Stdout))
	c.Assert(statusReport.Error.Message, qt.Contains, "migration file the Ptah format does not read: seed.sql")
}

// TestMigrationsPullSkipValidationStillChecksTheArtifactE2E holds the flag to
// what it skips: what the artifact says about itself is checked as on a
// default pull, and a refusal writes nothing.
func TestMigrationsPullSkipValidationStillChecksTheArtifactE2E(t *testing.T) {
	tests := []struct {
		name         string
		artifactType string
		format       string
		want         string
	}{
		{name: "an unknown migration format", artifactType: ociartifact.MigrationArtifactType, format: "bogus", want: "invalid migration artifact format"},
		{name: "a desired-schema artifact", artifactType: ociartifact.SchemaArtifactType, format: "ptah", want: ociartifact.SchemaArtifactType},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry := requiredOCIRegistry(t)
			reference := skipValidationReference(registry, "checked")
			publishMigrationLayers(c, reference, test.artifactType, test.format, skipValidationMigration())
			dir := c.TempDir()

			pulled := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				"migrations", "pull", reference, "--out", "migrations", "--plain-http", "--skip-validation")

			c.Assert(pulled.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", pulled.Stdout, pulled.Stderr))
			c.Assert(pulled.Stderr, qt.Contains, test.want)
			entries, err := os.ReadDir(dir)
			c.Assert(err, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0)
		})
	}
}
