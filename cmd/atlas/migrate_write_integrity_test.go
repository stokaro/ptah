package atlas_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests close stokaro/ptah#1086: `ptah-compat migrate new` and
// `ptah-compat migrate diff` wrote a migration file and a fresh atlas.sum over
// a directory nothing had verified.
//
// Every state below is measured against the pinned community binary v1.3.0
// through ptah-atlas-conformance/bin/atlas on 2026-08-03. The refusals are
// `atlas migrate new demo --dir file://d` and the matching `migrate diff`
// invocation printing
//
//	You have a checksum error in your migration directory.
//	...
//
// on stdout with `Error: checksum file not found` or `Error: checksum mismatch`
// on stderr at exit 1; the acceptances are the same commands exiting 0 and
// writing.

// atlasWriteVerb is one of the two verbs under test. Each row supplies its own
// invocation because they need different companion flags, which is wiring
// rather than a branch in a test body.
type atlasWriteVerb struct {
	name string
	run  func(c *qt.C, dir, query string) (stdout, stderr string, err error)
}

func atlasWriteVerbs() []atlasWriteVerb {
	return []atlasWriteVerb{
		{
			name: "new",
			run: func(_ *qt.C, dir, query string) (string, string, error) {
				return runCompat("migrate", "new", "demo", "--dir", "file://"+dir+query)
			},
		},
		{
			name: "diff",
			run: func(c *qt.C, dir, query string) (string, string, error) {
				target := filepath.Join(c.TempDir(), "target.sql")
				c.Assert(os.WriteFile(target, []byte(
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n"+
						"CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
				), 0o600), qt.IsNil)
				return runCompat("migrate", "diff", "demo",
					"--dir", "file://"+dir+query,
					"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
					"--to", "file://"+target)
			},
		},
	}
}

// atlasWriteDirState builds one migration directory in a named state and
// returns its path.
type atlasWriteDirState struct {
	name string
	// build populates root/migrations and returns the directory path, which is
	// not required to exist.
	build func(c *qt.C, root string) string
}

// writeAtlasWriteFixture writes one Atlas migration and returns the directory.
func writeAtlasWriteFixture(c *qt.C, root string) string {
	c.Helper()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "20240101000000_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	return dir
}

// hashAtlasWriteFixture writes the atlas.sum the community binary would write
// for the directory as it currently stands.
func hashAtlasWriteFixture(c *qt.C, dir string) {
	c.Helper()
	_, _, err := runCompat("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil)
}

// atlasWriteDirFingerprint records every file the directory holds and its
// content hash, so "the refusal wrote nothing" is asserted on bytes rather than
// on an exit code. An exit-code-only assertion passes just as well on a build
// that refuses AFTER creating the migration file.
func atlasWriteDirFingerprint(c *qt.C, dir string) string {
	c.Helper()
	fsys := os.DirFS(dir)
	var lines []string
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, readErr := fs.ReadFile(fsys, name)
		if readErr != nil {
			return readErr
		}
		sum := sha256.Sum256(data)
		lines = append(lines, name+" "+hex.EncodeToString(sum[:8]))
		return nil
	})
	if os.IsNotExist(err) {
		return "<absent>"
	}
	c.Assert(err, qt.IsNil)
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// TestCompatMigrateWrite_RefusesUnverifiedDirectoryBeforeWriting is the
// headline test for stokaro/ptah#1086.
//
// Revert the gate and every row prints the same failure: `got nil error but
// want non-nil`, followed by the fingerprint assertion showing a
// `20260803nnnnnn_demo.sql` and an `atlas.sum` that were not there before. The
// fingerprint is what makes the test about the write rather than about the exit
// code -- a gate placed after the writer would still fail the first assertion
// but pass the second, and this fails both.
func TestCompatMigrateWrite_RefusesUnverifiedDirectoryBeforeWriting(t *testing.T) {
	states := []struct {
		atlasWriteDirState
		// wantError is the message the root command prints after `Error: `,
		// matching the community binary's stderr byte for byte.
		wantError string
		// wantStdout is a line the guidance block on stdout must carry.
		wantStdout string
	}{
		{
			atlasWriteDirState: atlasWriteDirState{
				name:  "never hashed",
				build: writeAtlasWriteFixture,
			},
			wantError:  "checksum file not found",
			wantStdout: "Please check your migration files and run 'atlas migrate hash' to re-hash the contents",
		},
		{
			atlasWriteDirState: atlasWriteDirState{
				name: "hashed then edited",
				build: func(c *qt.C, root string) string {
					dir := writeAtlasWriteFixture(c, root)
					hashAtlasWriteFixture(c, dir)
					c.Assert(os.WriteFile(
						filepath.Join(dir, "20240101000000_init.sql"),
						[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE pwned (id INTEGER PRIMARY KEY);\n"),
						0o600,
					), qt.IsNil)
					return dir
				},
			},
			wantError:  "checksum mismatch",
			wantStdout: "L2: 20240101000000_init.sql was edited",
		},
		{
			atlasWriteDirState: atlasWriteDirState{
				name: "hashed then added",
				build: func(c *qt.C, root string) string {
					dir := writeAtlasWriteFixture(c, root)
					hashAtlasWriteFixture(c, dir)
					c.Assert(os.WriteFile(
						filepath.Join(dir, "20240102000000_more.sql"),
						[]byte("CREATE TABLE extra (id INTEGER PRIMARY KEY);\n"),
						0o600,
					), qt.IsNil)
					return dir
				},
			},
			wantError:  "checksum mismatch",
			wantStdout: "You have a checksum error in your migration directory.",
		},
		{
			atlasWriteDirState: atlasWriteDirState{
				name: "hashed then removed",
				build: func(c *qt.C, root string) string {
					dir := writeAtlasWriteFixture(c, root)
					hashAtlasWriteFixture(c, dir)
					c.Assert(os.Remove(filepath.Join(dir, "20240101000000_init.sql")), qt.IsNil)
					return dir
				},
			},
			wantError:  "checksum mismatch",
			wantStdout: "You have a checksum error in your migration directory.",
		},
		{
			// The gate fires on the presence of a top-level *.sql, not on a
			// parseable versioned migration: measured, the community binary
			// refuses an unhashed directory holding only `foo.sql`.
			atlasWriteDirState: atlasWriteDirState{
				name: "unhashed unversioned sql",
				build: func(c *qt.C, root string) string {
					dir := filepath.Join(root, "migrations")
					c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
					c.Assert(os.WriteFile(filepath.Join(dir, "foo.sql"),
						[]byte("CREATE TABLE foo (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
					return dir
				},
			},
			wantError:  "checksum file not found",
			wantStdout: "Please check your migration files and run 'atlas migrate hash' to re-hash the contents",
		},
	}

	for _, verb := range atlasWriteVerbs() {
		for _, state := range states {
			t.Run(verb.name+"/"+state.name, func(t *testing.T) {
				c := qt.New(t)
				dir := state.build(c, c.TempDir())
				before := atlasWriteDirFingerprint(c, dir)

				stdout, stderr, err := verb.run(c, dir, "")

				c.Assert(err, qt.ErrorMatches, state.wantError)
				c.Assert(stdout, qt.Contains, state.wantStdout)
				// The stream split is the community binary's: the guidance
				// block on stdout, one `Error: <reason>` line on stderr.
				c.Assert(stderr, qt.Equals, "Error: "+state.wantError+"\n")
				c.Assert(atlasWriteDirFingerprint(c, dir), qt.Equals, before)
			})
		}
	}
}

// TestCompatMigrateWrite_AcceptedDirectoriesStillWrite is the non-interference
// control, and it is the half a revert cannot redden.
//
// A guard that never fires cannot be caught by removing it, so this table is
// written to be reddened by the INVERSE mutant: make the gate refuse
// unconditionally (drop the exemptions in failUnhashedAtlasDir, or return the
// refusal from verifyAtlasWriteDirChecksum before it captures anything) and
// every row here fails with `got non-nil error` plus the checksum block, on
// directories the community binary exits 0 on.
func TestCompatMigrateWrite_AcceptedDirectoriesStillWrite(t *testing.T) {
	states := []atlasWriteDirState{
		{
			name: "hashed and clean",
			build: func(c *qt.C, root string) string {
				dir := writeAtlasWriteFixture(c, root)
				hashAtlasWriteFixture(c, dir)
				return dir
			},
		},
		{
			name: "empty",
			build: func(c *qt.C, root string) string {
				dir := filepath.Join(root, "migrations")
				c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
				return dir
			},
		},
		{
			name: "no sql files",
			build: func(c *qt.C, root string) string {
				dir := filepath.Join(root, "migrations")
				c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "README.md"), []byte("notes\n"), 0o600), qt.IsNil)
				return dir
			},
		},
		{
			// Nested SQL is not a migration on either tool since #976: the
			// covered set is top-level only, so the directory has nothing to
			// verify and the community binary exits 0.
			name: "sql only below the top level",
			build: func(c *qt.C, root string) string {
				dir := filepath.Join(root, "migrations")
				c.Assert(os.MkdirAll(filepath.Join(dir, "sub"), 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "sub", "1_a.sql"),
					[]byte("CREATE TABLE a (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
				return dir
			},
		},
		{
			// The covered set is case-sensitive, so `.SQL` is nothing to
			// execute rather than an unhashed history. Measured: the community
			// binary exits 0 here too.
			name: "uppercase sql suffix",
			build: func(c *qt.C, root string) string {
				dir := filepath.Join(root, "migrations")
				c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_init.SQL"),
					[]byte("CREATE TABLE a (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
				return dir
			},
		},
		{
			// Both verbs create their directory, and the community binary
			// exits 0 on a --dir that does not exist yet, so a gate that
			// treated an absent directory as an error would refuse the first
			// migration of every new project.
			name: "directory does not exist",
			build: func(_ *qt.C, root string) string {
				return filepath.Join(root, "migrations")
			},
		},
	}

	for _, verb := range atlasWriteVerbs() {
		for _, state := range states {
			t.Run(verb.name+"/"+state.name, func(t *testing.T) {
				c := qt.New(t)
				dir := state.build(c, c.TempDir())
				before := atlasWriteDirFingerprint(c, dir)

				stdout, _, err := verb.run(c, dir, "")

				c.Assert(err, qt.IsNil, qt.Commentf("%s", stdout))
				c.Assert(atlasWriteDirFingerprint(c, dir), qt.Not(qt.Equals), before)
				c.Assert(atlasWriteDirFingerprint(c, dir), qt.Contains, "atlas.sum")
			})
		}
	}
}

// TestCompatMigrateNew_GatesTheDirectoryTheForwardedCommandWillWrite closes the
// hole a gate placed on the Atlas-facing arguments alone would leave.
//
// `migrate new` forwards into `ptah migrations create`, whose directory flag is
// --migrations-dir, and cmdadapter installs PTAH_<FLAG> binding on every
// forwarded target. So PTAH_DIR and PTAH_MIGRATIONS_DIR both name this verb's
// directory, and only the first reaches atlasargs.Map. Reverting the native
// environment layer in resolveAtlasMigrateSourceDir prints, on the
// PTAH_MIGRATIONS_DIR row alone, `got nil error but want non-nil` and a
// fingerprint carrying the demo migration and a fresh atlas.sum -- exactly the
// #1086 defect, reachable through an environment variable while --dir looks
// gated.
//
// The hashed rows are the controls: they must keep writing, so the fix cannot
// be "refuse whenever the environment names the directory".
func TestCompatMigrateNew_GatesTheDirectoryTheForwardedCommandWillWrite(t *testing.T) {
	tests := []struct {
		name string
		// envName is the variable that carries the directory.
		envName string
		// envValue renders the variable's value from the directory path.
		envValue func(dir string) string
		// hashed selects the control half of the pair.
		hashed bool
	}{
		{
			name:     "native spelling unhashed",
			envName:  "PTAH_MIGRATIONS_DIR",
			envValue: func(dir string) string { return dir },
		},
		{
			name:     "native spelling hashed",
			envName:  "PTAH_MIGRATIONS_DIR",
			envValue: func(dir string) string { return dir },
			hashed:   true,
		},
		{
			name:     "atlas spelling unhashed",
			envName:  "PTAH_DIR",
			envValue: func(dir string) string { return "file://" + dir },
		},
		{
			name:     "atlas spelling hashed",
			envName:  "PTAH_DIR",
			envValue: func(dir string) string { return "file://" + dir },
			hashed:   true,
		},
	}

	outcomes := map[bool]func(c *qt.C, before, after string, err error){
		false: func(c *qt.C, before, after string, err error) {
			c.Assert(err, qt.ErrorMatches, `checksum file not found`)
			c.Assert(after, qt.Equals, before)
		},
		true: func(c *qt.C, before, after string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(after, qt.Not(qt.Equals), before)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeAtlasWriteFixture(c, c.TempDir())
			hashers := map[bool]func(){
				true:  func() { hashAtlasWriteFixture(c, dir) },
				false: func() {},
			}
			hashers[tt.hashed]()
			t.Setenv(tt.envName, tt.envValue(dir))
			before := atlasWriteDirFingerprint(c, dir)

			_, _, err := runCompat("migrate", "new", "demo")

			outcomes[tt.hashed](c, before, atlasWriteDirFingerprint(c, dir), err)
		})
	}
}

// TestCompatMigrateWrite_GatesTheDirectoryNamedByAtlasProjectConfig covers the
// third layer that can name the directory. Reverting the gate prints `got nil
// error but want non-nil` on both rows and leaves a demo migration plus a fresh
// atlas.sum in a directory the project file pointed at.
func TestCompatMigrateWrite_GatesTheDirectoryNamedByAtlasProjectConfig(t *testing.T) {
	verbs := map[string][]string{
		"new":  {"migrate", "new", "demo", "--env", "local"},
		"diff": {"migrate", "diff", "demo", "--env", "local"},
	}

	for name, args := range verbs {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			t.Chdir(root)
			dir := writeAtlasWriteFixture(c, root)
			c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"),
				[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
				0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), fmt.Appendf(nil, `env "local" {
  dev = "sqlite://%s"
  schema {
    src = "file://schema.sql"
  }
  migration {
    dir = "file://migrations"
  }
}
`, filepath.ToSlash(filepath.Join(root, "dev.db"))), 0o600), qt.IsNil)
			before := atlasWriteDirFingerprint(c, dir)

			stdout, _, err := runCompat(args...)

			c.Assert(err, qt.ErrorMatches, `checksum file not found`)
			c.Assert(stdout, qt.Contains, "You have a checksum error in your migration directory.")
			c.Assert(atlasWriteDirFingerprint(c, dir), qt.Equals, before)
		})
	}
}
