package atlas_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
)

const unknownFixtureDriver = "sql/sqlclient: unknown driver \"fixture\". See: https://atlasgo.io/url"

// missingProviderManifest claims the engine "fixture" for an executable that
// does not exist. A run that reaches the provider fails at once, offline, with
// the executable named missing, which proves the surface handed the URL to the
// provider resolver and started nothing.
func missingProviderManifest(c *qt.C) string {
	c.Helper()
	executable := filepath.Join(c.TempDir(), "missing-provider")
	manifest := `{"format_version":2,"providers":[{"executable":` + strconv.Quote(executable) + `,"sha256":"` + strings.Repeat("a", 64) +
		`","provider_id":"example.org/fixture","engines":[{"id":"fixture"}]}]}`
	path := filepath.Join(c.TempDir(), "providers.json")
	c.Assert(os.WriteFile(path, []byte(manifest), 0o600), qt.IsNil)
	return path
}

func TestCompatProviderSession_HappyPath(t *testing.T) {
	t.Run("a manifest engine reaches the provider resolver instead of the unknown-driver refusal", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_PROVIDER_CONFIG", missingProviderManifest(c))

		_, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), "schema", "inspect", "--url", "fixture:///x.db")

		c.Assert(err, qt.IsNotNil)
		c.Assert(stderr, qt.Contains, "executable was not found")
		c.Assert(stderr, qt.Not(qt.Contains), "unknown driver")
	})
	t.Run("the connection URL gate of each verb lets a claimed scheme through", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_PROVIDER_CONFIG", missingProviderManifest(c))
		migrations := filepath.Join(c.TempDir(), "migrations")
		c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(migrations, "20240101000000_init.sql"), []byte("CREATE TABLE widgets (id integer NOT NULL);\n"), 0o600), qt.IsNil)
		dir := "file://" + filepath.ToSlash(migrations)
		_, _, err := runCompatWithPolicy(atlascompatpolicy.Full(), "migrate", "hash", "--dir", dir)
		c.Assert(err, qt.IsNil)
		for _, args := range [][]string{
			{"migrate", "status", "--url", "fixture:///x.db", "--dir", dir},
			{"migrate", "apply", "--url", "fixture:///x.db", "--dir", dir},
			{"schema", "clean", "--url", "fixture:///x.db", "--auto-approve"},
		} {
			_, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), args...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%v", args))
			c.Assert(stderr, qt.Contains, "executable was not found", qt.Commentf("%v", args))
			c.Assert(stderr, qt.Not(qt.Contains), "unknown driver", qt.Commentf("%v", args))
		}
	})
	t.Run("the dev URL gate lets a claimed scheme through", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_PROVIDER_CONFIG", missingProviderManifest(c))
		lite := "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), "lite.db"))

		_, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), "schema", "diff", "--from", lite, "--to", lite, "--dev-url", "fixture:///x.db")

		c.Assert(err, qt.IsNotNil)
		c.Assert(stderr, qt.Not(qt.Contains), "unknown driver")
	})
	t.Run("a built-in URL is untouched by a manifest", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_PROVIDER_CONFIG", missingProviderManifest(c))
		database := "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), "lite.db"))

		stdout, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), "schema", "inspect", "--url", database)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
		c.Assert(stdout, qt.Contains, `schema "main"`)
	})
	t.Run("commands that connect to nothing do not read the variable", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_PROVIDER_CONFIG", "")

		stdout, _, err := runCompatWithPolicy(atlascompatpolicy.Full(), "version")

		c.Assert(err, qt.IsNil)
		c.Assert(stdout, qt.Not(qt.Equals), "")
	})
	t.Run("the strict profile installs no session, so the variable is not read", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_PROVIDER_CONFIG", "")

		_, stderr, err := runCompatWithPolicy(atlascompatpolicy.StrictCE(), "schema", "inspect", "--url", "fixture:///x.db")

		c.Assert(err, qt.IsNotNil)
		c.Assert(stderr, qt.Equals, "Error: "+unknownFixtureDriver+"\n")
	})
}

func TestCompatProviderSession_FailurePath(t *testing.T) {
	t.Run("without a manifest the same URL is an unknown driver", func(t *testing.T) {
		c := qt.New(t)

		_, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), "schema", "inspect", "--url", "fixture:///x.db")

		c.Assert(err, qt.IsNotNil)
		c.Assert(stderr, qt.Equals, "Error: "+unknownFixtureDriver+"\n")
	})
	t.Run("a blank variable is refused on a connecting verb", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_PROVIDER_CONFIG", "  ")

		_, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), "migrate", "status", "--url", "sqlite://x.db", "--dir", "file://x")

		c.Assert(err, qt.IsNotNil)
		c.Assert(stderr, qt.Equals, "Error: PTAH_PROVIDER_CONFIG requires a manifest path\n")
	})
	t.Run("a manifest that does not load is refused before the verb runs", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_PROVIDER_CONFIG", filepath.Join(c.TempDir(), "absent.json"))

		_, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), "schema", "inspect", "--url", "sqlite://x.db")

		c.Assert(err, qt.IsNotNil)
		c.Assert(stderr, qt.Contains, "Error: read provider manifest: ")
	})
	t.Run("a scheme the manifest does not claim stays unknown", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_PROVIDER_CONFIG", missingProviderManifest(c))

		_, stderr, err := runCompatWithPolicy(atlascompatpolicy.Full(), "schema", "inspect", "--url", "other:///x.db")

		c.Assert(err, qt.IsNotNil)
		c.Assert(stderr, qt.Equals, "Error: sql/sqlclient: unknown driver \"other\". See: https://atlasgo.io/url\n")
	})
}
