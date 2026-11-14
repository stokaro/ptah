package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// The directory and file names a `migrate lint --format` template reads,
// measured on the pinned community binary v1.3.0 with SQLite and --latest 1
// (stokaro/ptah#6000). The verb captures the directory at its absolute path
// and converts a Flyway directory into numeric Atlas files; neither is what
// the operator's repository spells.

// writeEnvDirFixture lays down two hashed Atlas migrations in dir.
func writeEnvDirFixture(c *qt.C, dir string) {
	c.Helper()
	atlastest.WriteHashedAtlasDir(c, dir, "1.sql", "CREATE TABLE t1 (id int);\n")
	atlastest.WriteHashedAtlasDir(c, dir, "2.sql", "CREATE TABLE t2 (id int);\n")
}

func TestCompatMigrateLintFormatEnvDir_HappyPath(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	absolute := filepath.Join(root, "migrations")
	writeEnvDirFixture(c, absolute)
	tests := []struct {
		name string
		dir  string
		want string
	}{
		{name: "a relative directory", dir: "file://migrations", want: "migrations"},
		{name: "a relative directory spelled from the current one", dir: "file://./migrations", want: "migrations"},
		{name: "a trailing slash", dir: "file://migrations/", want: "migrations"},
		{name: "a query", dir: "file://migrations?format=atlas", want: "migrations"},
		{name: "an absolute directory", dir: "file://" + filepath.ToSlash(absolute), want: absolute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, err := atlastest.RunCompatStreams(c,
				"migrate", "lint",
				"--dir", test.dir,
				"--dev-url", "sqlite://file?mode=memory",
				"--latest", "1",
				"--format", "{{ .Env.Dir }}",
			)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, test.want)
		})
	}
}

// TestCompatMigrateLintFormatEnvDirFromProject: a directory atlas.hcl names is
// reported as a --dir naming it is, cleaned. The directory is not the --dir
// default, so a report that ignored the project value would print
// `migrations`. Measured on the pinned community binary v1.3.0, each spelling
// below prints `db/mig`.
func TestCompatMigrateLintFormatEnvDirFromProject(t *testing.T) {
	for _, dir := range []string{"file://db/mig", "file://./db/mig", "file://db/mig/", "file://db//mig"} {
		t.Run(dir, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			t.Chdir(root)
			writeEnvDirFixture(c, filepath.Join(root, "db", "mig"))
			project := "env \"dev\" {\n  dev = \"sqlite://file?mode=memory\"\n  migration {\n    dir = \"" + dir + "\"\n  }\n}\n"
			c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)

			stdout, stderr, err := atlastest.RunCompatStreams(c,
				"migrate", "lint", "--env", "dev", "--latest", "1", "--format", "{{ .Env.Dir }}",
			)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, filepath.FromSlash("db/mig"))
		})
	}
}

// writeFlywayFixture lays down a Flyway directory in root/migrations and
// hashes it for the Flyway layout.
func writeFlywayFixture(c *qt.C, root string, files map[string]string) {
	c.Helper()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, stderr, err := atlastest.RunCompatStreams(c, "migrate", "hash", "--dir", "file://migrations", "--dir-format", "flyway")
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
}

func runFlywayLintFormat(c *qt.C, latest, format string) (stdout, stderr string, err error) {
	c.Helper()
	return atlastest.RunCompatStreams(c,
		"migrate", "lint",
		"--dir", "file://migrations",
		"--dir-format", "flyway",
		"--dev-url", "sqlite://file?mode=memory",
		"--latest", latest,
		"--format", format,
	)
}

// flywayFractionalVersion has a fractional second version, so its converted
// Atlas name shares nothing with the source name.
func flywayFractionalVersion(second string) map[string]string {
	return map[string]string{"V1__a.sql": "CREATE TABLE a (id int);\n", "V1.5__b.sql": second}
}

// TestCompatMigrateLintFormatFlywayFileNames_HappyPath: each analyzed file and
// the step that analyzed it carry the source name. Conversion gives every
// file a numeric Atlas name, the baseline and the repeatable included.
func TestCompatMigrateLintFormatFlywayFileNames_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		files  map[string]string
		latest string
		want   string
	}{
		{
			name:   "a fractional version",
			files:  flywayFractionalVersion("CREATE TABLE b (id int);\n"),
			latest: "1",
			want:   "[V1.5__b.sql]|Migration Integrity Check;Detect New Migration Files;Replay Migration Files;Analyze V1.5__b.sql;",
		},
		{
			name: "a baseline, a version and a repeatable",
			files: map[string]string{
				"B1__base.sql": "CREATE TABLE base (id int);\n",
				"V2__b.sql":    "CREATE TABLE b (id int);\n",
				"R__v.sql":     "CREATE VIEW v AS SELECT 1 AS x;\n",
			},
			latest: "3",
			want: "[B1__base.sql][V2__b.sql][R__v.sql]|Migration Integrity Check;Detect New Migration Files;Replay Migration Files;" +
				"Analyze B1__base.sql;Analyze V2__b.sql;Analyze R__v.sql;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			t.Chdir(root)
			writeFlywayFixture(c, root, test.files)

			stdout, stderr, err := runFlywayLintFormat(c, test.latest, "{{ range .Files }}[{{ .Name }}]{{ end }}|{{ range .Steps }}{{ .Name }};{{ end }}")

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, test.want)
		})
	}
}

// TestCompatMigrateLintFormatFlywayFileNames_ReplayFailure: the file a failed
// replay stopped at carries the source name too. The run exits 1 with the
// report on stdout and nothing on stderr.
func TestCompatMigrateLintFormatFlywayFileNames_ReplayFailure(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeFlywayFixture(c, root, flywayFractionalVersion("CREATE TABLEX b (id int);\n"))

	stdout, stderr, err := runFlywayLintFormat(c, "1", "{{ range .Files }}{{ .Name }}{{ end }}")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "V1.5__b.sql")
	c.Assert(stderr, qt.Equals, "")
}
