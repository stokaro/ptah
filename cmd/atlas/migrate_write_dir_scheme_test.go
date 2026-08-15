package atlas_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests close the half of stokaro/ptah#1186 that WRITES.
//
// The pinned community binary v1.3.0 requires a scheme on --dir. Measured
// through ptah-atlas-conformance/bin/atlas on 2026-08-06:
//
//	$ atlas migrate new addcol --dir mig --dir-format goose
//	Error: missing scheme for dir url. Did you mean "file://mig"?
//	exit 1, nothing created
//
//	$ atlas migrate diff demo --dir mig2 --dev-url … --to …
//	Error: missing scheme for dir url. Did you mean "file://mig2"?
//	exit 1, nothing created
//
// Ptah accepted the bare path on every verb, which on the two verbs that create
// a directory means materializing one somewhere the operator did not point at:
// `migrate new addcol --dir /tmp/nosuchxyz --dir-format goose` exited 0 and
// wrote to /private/tmp/nosuchxyz. That change first covered the writers; the
// read consumers now require the scheme too and are covered below.
//
// The suggestion carries the URL's path component only — measured,
// `--dir 'sub/dir?format=goose&x=1'` and `--dir 'sub/dir#frag'` both suggest
// `"file://sub/dir"` there, while `--dir ./rel` suggests `"file://./rel"`.
// The message terminates with one ASCII space before the line feed. In hex,
// its final two bytes on stderr are `20 0a`.

// atlasWriteVerbSpelling is one writing verb invoked with the --dir value
// exactly as the caller spelled it. It is deliberately not
// [atlasWriteVerbs]: that table adds `file://` on the caller's behalf, which is
// the one thing these tests are about.
type atlasWriteVerbSpelling struct {
	name string
	run  func(c *qt.C, dirArg string) (stdout, stderr string, err error)
}

func atlasWriteVerbSpellings() []atlasWriteVerbSpelling {
	return []atlasWriteVerbSpelling{
		{
			name: "new",
			run: func(_ *qt.C, dirArg string) (string, string, error) {
				return runCompat("migrate", "new", "demo", "--dir", dirArg)
			},
		},
		{
			name: "diff",
			run: func(c *qt.C, dirArg string) (string, string, error) {
				target := filepath.Join(c.TempDir(), "target.sql")
				c.Assert(os.WriteFile(target, []byte(
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				), 0o600), qt.IsNil)
				return runCompat("migrate", "diff", "demo",
					"--dir", dirArg,
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--to", "file://"+target)
			},
		},
	}
}

// atlasMissingSchemeError renders the community binary's message for a
// directory URL naming no scheme.
func atlasMissingSchemeError(path string) string {
	return fmt.Sprintf("missing scheme for dir url. Did you mean %q? ", "file://"+path)
}

// TestCompatMigrateWrite_RefusesADirectoryNamingNoScheme is the headline test.
//
// Revert the gate — drop the `verb.writesDir` branch in
// resolveAtlasMigrateSource and the RequireDirScheme call in
// runAtlasMigrateDiff — and every row fails with `got nil error but want
// non-nil`, followed by a fingerprint holding a `20260806nnnnnn_demo.sql` and
// an `atlas.sum` in a directory that did not exist when the command started.
// The fingerprint is what makes the row about the write rather than the exit
// code: a refusal placed after the writer would still satisfy an exit-code
// assertion.
func TestCompatMigrateWrite_RefusesADirectoryNamingNoScheme(t *testing.T) {
	spellings := []struct {
		name string
		// dirArg renders the --dir value from a directory path that does not
		// exist yet.
		dirArg func(missing string) string
		// wantError renders the message the community binary prints after
		// `Error: ` for that value.
		wantError func(missing string) string
	}{
		{
			name:      "bare path",
			dirArg:    func(missing string) string { return missing },
			wantError: atlasMissingSchemeError,
		},
		{
			name:      "bare path carrying a format query",
			dirArg:    func(missing string) string { return missing + "?format=goose" },
			wantError: atlasMissingSchemeError,
		},
		{
			name:      "bare path carrying a fragment",
			dirArg:    func(missing string) string { return missing + "#frag" },
			wantError: atlasMissingSchemeError,
		},
		{
			name:      "empty",
			dirArg:    func(string) string { return "" },
			wantError: func(string) string { return atlasMissingSchemeError("") },
		},
	}

	for _, verb := range atlasWriteVerbSpellings() {
		for _, spelling := range spellings {
			t.Run(verb.name+"/"+spelling.name, func(t *testing.T) {
				c := qt.New(t)
				missing := filepath.Join(c.TempDir(), "mig")

				_, stderr, err := verb.run(c, spelling.dirArg(missing))

				c.Assert(err, qt.ErrorMatches, regexpQuote(spelling.wantError(missing)))
				// The stream split is the community binary's: one
				// `Error: <reason>` line on stderr and nothing else.
				c.Assert(stderr, qt.Equals, "Error: "+spelling.wantError(missing)+"\n")
				c.Assert(atlasWriteDirFingerprint(c.TB, missing), qt.Equals, "<absent>")
			})
		}
	}
}

// TestCompatMigrateNew_RequiresTheSchemeOnEveryAtlasSpelling enumerates the
// layers that can name this verb's directory with Atlas semantics, because a
// gate placed on the flag alone leaves the environment twin open — the shape
// stokaro/ptah#1086 already had to be fixed for once.
//
// Reverting the gate prints `got nil error but want non-nil` on both rows.
func TestCompatMigrateNew_RequiresTheSchemeOnEveryAtlasSpelling(t *testing.T) {
	layers := []struct {
		name string
		// run invokes `migrate new` with the directory named through this layer.
		run func(c *qt.C, dir string) (stdout, stderr string, err error)
	}{
		{
			name: "the --dir flag",
			run: func(_ *qt.C, dir string) (string, string, error) {
				return runCompat("migrate", "new", "demo", "--dir", dir)
			},
		},
		{
			name: "the PTAH_DIR environment twin",
			run: func(c *qt.C, dir string) (string, string, error) {
				c.Setenv("PTAH_DIR", dir)
				return runCompat("migrate", "new", "demo")
			},
		},
	}

	for _, layer := range layers {
		t.Run(layer.name, func(t *testing.T) {
			c := qt.New(t)
			missing := filepath.Join(c.TempDir(), "mig")

			_, stderr, err := layer.run(c, missing)

			c.Assert(err, qt.ErrorMatches, regexpQuote(atlasMissingSchemeError(missing)))
			c.Assert(stderr, qt.Equals, "Error: "+atlasMissingSchemeError(missing)+"\n")
			c.Assert(atlasWriteDirFingerprint(c.TB, missing), qt.Equals, "<absent>")
		})
	}
}

// TestCompatMigrateWrite_SpellingsTheCommunityBinaryAcceptsStillWrite is the
// non-interference control, and it is the half a revert cannot redden: a guard
// that never fires cannot be caught by removing it.
//
// It is written to be reddened by the INVERSE mutant — make the requirement
// unconditional by dropping `verb.writesDir && … atlasDirSchemeIsAnswerable(…)`
// in resolveAtlasMigrateSource and by checking the merged opts.dirURL instead
// of dirURLSpelled in runAtlasMigrateDiff. Every row then fails with `got
// non-nil error` carrying `missing scheme for dir url`, on invocations the
// community binary exits 0 on.
func TestCompatMigrateWrite_SpellingsTheCommunityBinaryAcceptsStillWrite(t *testing.T) {
	cases := []struct {
		name string
		// run prepares the invocation and returns the directory it must have
		// written into.
		run func(c *qt.C) (dir string, err error)
	}{
		{
			name: "new names the directory as a file:// URL",
			run: func(c *qt.C) (string, error) {
				dir := filepath.Join(c.TempDir(), "mig")
				_, _, err := runCompat("migrate", "new", "demo", "--dir", "file://"+dir)
				return dir, err
			},
		},
		{
			name: "new names an external layout through the format query",
			run: func(c *qt.C) (string, error) {
				dir := filepath.Join(c.TempDir(), "mig")
				_, _, err := runCompat("migrate", "new", "demo", "--dir", "file://"+dir+"?format=goose")
				return dir, err
			},
		},
		{
			name: "new names an external layout through --dir-format",
			run: func(c *qt.C) (string, error) {
				dir := filepath.Join(c.TempDir(), "mig")
				_, _, err := runCompat("migrate", "new", "demo",
					"--dir", "file://"+dir, "--dir-format", "golang-migrate")
				return dir, err
			},
		},
		{
			name: "PTAH_DIR carries the scheme",
			run: func(c *qt.C) (string, error) {
				dir := filepath.Join(c.TempDir(), "mig")
				c.Setenv("PTAH_DIR", "file://"+dir)
				_, _, err := runCompat("migrate", "new", "demo")
				return dir, err
			},
		},
		{
			// PTAH_MIGRATIONS_DIR is the NATIVE --migrations-dir under the
			// environment prefix cmdadapter installs on the forwarded target. It
			// takes a plain path and the community binary has no spelling for it
			// at all, so a rule read off that binary must not reach it.
			name: "PTAH_MIGRATIONS_DIR carries a bare path",
			run: func(c *qt.C) (string, error) {
				dir := filepath.Join(c.TempDir(), "mig")
				c.Setenv("PTAH_MIGRATIONS_DIR", dir)
				_, _, err := runCompat("migrate", "new", "demo")
				return dir, err
			},
		},
		{
			name: "atlas.hcl names the directory as a file:// URL",
			run: func(c *qt.C) (string, error) {
				root := c.TempDir()
				c.Setenv("HOME", root)
				writeAtlasMigrationDirProject(c.TB, root, "file://mig")
				_, _, err := runCompat("migrate", "new", "demo",
					"--config", "file://"+filepath.Join(root, "atlas.hcl"), "--env", "local")
				return filepath.Join(root, "mig"), err
			},
		},
		{
			name: "diff names the directory as a file:// URL",
			run: func(c *qt.C) (string, error) {
				dir := filepath.Join(c.TempDir(), "mig")
				target := filepath.Join(c.TempDir(), "target.sql")
				c.Assert(os.WriteFile(target, []byte(
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				), 0o600), qt.IsNil)
				_, _, err := runCompat("migrate", "diff", "demo",
					"--dir", "file://"+dir,
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--to", "file://"+target)
				return dir, err
			},
		},
		{
			// The --dir default on this verb is `file://migrations`, so an
			// omitted flag must still pass a requirement read off the flag layer.
			name: "diff omits --dir and takes the flag default",
			run: func(c *qt.C) (string, error) {
				root := c.TempDir()
				c.Setenv("HOME", root)
				target := filepath.Join(c.TempDir(), "target.sql")
				c.Assert(os.WriteFile(target, []byte(
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				), 0o600), qt.IsNil)
				c.Chdir(root)
				_, _, err := runCompat("migrate", "diff", "demo",
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--to", "file://"+target)
				return filepath.Join(root, "migrations"), err
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			dir, err := tt.run(c)

			c.Assert(err, qt.IsNil)
			c.Assert(atlasWriteDirFingerprint(c.TB, dir), qt.Contains, "atlas.sum")
		})
	}
}

// TestCompatMigrateRead_RequiresTheSchemeToo is the boundary this test used to
// pin from the other side. It read
// `TestCompatMigrateRead_StillAcceptsADirectoryNamingNoScheme`, and the
// requirement belonged to the verbs that can CREATE a directory, because
// widening it was "a separate decision with a measurable blast radius"
// (stokaro/ptah#1186).
//
// That decision was taken and the blast radius was measured, so the assertion
// is inverted rather than deleted: the reading verbs now refuse the same
// spelling, with the same message.
//
// Measured on the pinned community binary v1.3.0, `--dir mig --dir-format
// goose`, each verb in its own directory:
//
//	hash      binary 1, ptah 1, `missing scheme for dir url. Did you mean "file://mig"?`
//	validate  binary 1, ptah 1, same line
//	lint      binary 1, ptah 1, same line
//	status    binary 1, ptah 1, same line
//
// The control is below: the same directory named WITH the scheme still works on
// the two integrity verbs, which is what separates "requires the scheme" from
// "refuses this directory". The tagged black-box integration contour covers
// all six compatibility consumers and native bare-path controls.
func TestCompatMigrateRead_RequiresTheSchemeToo(t *testing.T) {
	verbs := []struct {
		name string
		args func(dir string) []string
	}{
		{
			name: "hash",
			args: func(dir string) []string { return []string{"migrate", "hash", "--dir", dir} },
		},
		{
			name: "validate",
			args: func(dir string) []string { return []string{"migrate", "validate", "--dir", dir} },
		},
	}

	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeAtlasWriteFixture(c, c.TempDir())
			hashAtlasWriteFixture(c.TB, dir)

			_, _, err := runCompat(verb.args(dir)...)

			c.Assert(err, qt.ErrorMatches, `missing scheme for dir url\. Did you mean ".*"\? `)
		})
	}

	for _, verb := range verbs {
		t.Run("control: "+verb.name+" with the scheme", func(t *testing.T) {
			c := qt.New(t)
			dir := writeAtlasWriteFixture(c, c.TempDir())
			hashAtlasWriteFixture(c.TB, dir)

			_, _, err := runCompat(verb.args("file://" + dir)...)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestCompatMigrateNew_LeavesTheProjectFileSpellingToIssue1186 records a
// divergence this change deliberately does not close, so that closing it later
// has to come here and say so.
//
// atlas.hcl `migration.dir` is normalized by
// config/projectconfig.normalizeAtlasMigrationDir, which strips `file://` at
// parse time. By the time any verb sees the value, `file://mig` and `mig` are
// the same string, so a requirement placed on it would refuse both — and the
// community binary refuses only the second (measured: `migrate new addcol
// --env local` against `dir = "mig"` exits 1 there with the scheme message,
// against `dir = "file://mig"` exits 0). Recovering the spelling means changing
// what the project loader keeps, which is a public-surface decision that
// belongs to #1186 rather than to a `migrate new` change.
func TestCompatMigrateNew_LeavesTheProjectFileSpellingToIssue1186(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	c.Setenv("HOME", root)
	writeAtlasMigrationDirProject(c.TB, root, "mig")

	_, _, err := runCompat("migrate", "new", "demo",
		"--config", "file://"+filepath.Join(root, "atlas.hcl"), "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(atlasWriteDirFingerprint(c.TB, filepath.Join(root, "mig")), qt.Contains, "atlas.sum")
}

// writeAtlasMigrationDirProject writes an atlas.hcl whose only env names the
// migration directory with the given spelling.
func writeAtlasMigrationDirProject(tb testing.TB, root, dir string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), fmt.Appendf(nil, `env "local" {
  migration {
    dir = %q
  }
}
`, dir), 0o600), qt.IsNil)
}
