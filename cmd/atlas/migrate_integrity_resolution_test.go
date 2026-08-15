package atlas_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/migratesum"
)

// These tests cover where the source layout can come from besides an explicit
// flag — atlas.hcl and the PTAH_* environment — and the two behaviors the
// converted path inherits that Atlas CE does not share.

func writeIntegrityProject(c *qt.C, format string) (configPath, migrationsDir string) {
	c.Helper()
	projectDir := c.TempDir()
	migrationsDir = filepath.Join(projectDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	for name, content := range integrityFixture {
		path := filepath.Join(migrationsDir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	}
	configPath = filepath.Join(projectDir, "atlas.hcl")
	c.Assert(os.WriteFile(configPath, []byte(`env "local" {
  migration {
    dir    = "file://migrations"
    format = `+format+`
  }
}
`), 0o600), qt.IsNil)
	return configPath, migrationsDir
}

// TestCompatMigrateIntegrityProjectConfig_HappyPath covers atlas.hcl
// migration.format reaching both verbs, which is the path a user hits with a
// plain `ptah-compat migrate hash --env local` and no directory flags at all.
func TestCompatMigrateIntegrityProjectConfig_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   []string
	}{
		{name: "flyway", format: "flyway", want: flywayCoveredSet},
		{name: "goose", format: "goose", want: sqlSuffixCoveredSet},
		{name: "golang_migrate", format: "golang-migrate", want: golangMigrateCoveredSet},
		{name: "atlas", format: "atlas", want: sqlSuffixCoveredSet},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			configPath, migrationsDir := writeIntegrityProject(c, tt.format)

			hashOut, _, hashErr := runCompatExit(
				"migrate", "hash", "--config", "file://"+configPath, "--env", "local")

			c.Assert(hashErr, qt.IsNil, qt.Commentf("output:\n%s", hashOut))
			c.Assert(sumEntryNames(c, migrationsDir), qt.DeepEquals, tt.want)

			validateOut, validateErrOut, validateErr := runCompatExit(
				"migrate", "validate", "--config", "file://"+configPath, "--env", "local")

			c.Assert(validateErr, qt.IsNil)
			c.Assert(validateOut, qt.Equals, "")
			c.Assert(validateErrOut, qt.Equals, "")
		})
	}
}

// TestCompatMigrateIntegrityProjectConfigPrecedence_HappyPath pins that an
// explicit spelling outranks atlas.hcl, both ways round. The project format and
// the overriding value are always different formats so neither result can be
// produced by the other rule.
func TestCompatMigrateIntegrityProjectConfigPrecedence_HappyPath(t *testing.T) {
	t.Run("flag overrides project format", func(t *testing.T) {
		c := qt.New(t)
		configPath, migrationsDir := writeIntegrityProject(c, "flyway")

		stdout, _, err := runCompatExit(
			"migrate", "hash", "--config", "file://"+configPath, "--env", "local", "--dir-format", "goose")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(sumEntryNames(c, migrationsDir), qt.DeepEquals, sqlSuffixCoveredSet)
	})

	t.Run("query overrides project format", func(t *testing.T) {
		c := qt.New(t)
		configPath, migrationsDir := writeIntegrityProject(c, "flyway")

		stdout, _, err := runCompatExit(
			"migrate", "hash", "--config", "file://"+configPath, "--env", "local",
			"--dir", "file://"+migrationsDir+"?format=golang-migrate")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(sumEntryNames(c, migrationsDir), qt.DeepEquals, golangMigrateCoveredSet)
	})

	// An atlas query over a converted project format is the case that forwards
	// to the native command with a value it would refuse, so the neutralized
	// --dir-format has to reach it.
	//
	//	$ atlas migrate hash --env local --dir 'file://migrations?format=atlas'
	//	  -> 1_init.sql U1__undo.sql V1__x.sql   (atlas, with migration.format = flyway)
	t.Run("atlas query overrides a converted project format", func(t *testing.T) {
		c := qt.New(t)
		configPath, migrationsDir := writeIntegrityProject(c, "flyway")

		stdout, stderr, err := runCompatExit(
			"migrate", "hash", "--config", "file://"+configPath, "--env", "local",
			"--dir", "file://"+migrationsDir+"?format=atlas")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
		c.Assert(sumEntryNames(c, migrationsDir), qt.DeepEquals, sqlSuffixCoveredSet)
	})

	t.Run("project format applies when no spelling is given", func(t *testing.T) {
		c := qt.New(t)
		configPath, migrationsDir := writeIntegrityProject(c, "golang-migrate")

		stdout, _, err := runCompatExit(
			"migrate", "hash", "--config", "file://"+configPath, "--env", "local")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(sumEntryNames(c, migrationsDir), qt.DeepEquals, golangMigrateCoveredSet)
	})
}

// TestCompatMigrateIntegrityEnvironment_HappyPath covers PTAH_DIR and
// PTAH_DIR_FORMAT, the layer between the command line and atlas.hcl. A query
// arriving through the environment has no --dir token to rewrite, so the atlas
// row here is what exercises the resolver's append branch.
func TestCompatMigrateIntegrityEnvironment_HappyPath(t *testing.T) {
	t.Run("PTAH_DIR_FORMAT selects the layout", func(t *testing.T) {
		c := qt.New(t)
		dir := writeIntegrityFixture(c)
		c.Setenv("PTAH_DIR_FORMAT", "flyway")

		stdout, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir)

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(sumEntryNames(c, dir), qt.DeepEquals, flywayCoveredSet)
	})

	t.Run("PTAH_DIR carries a converted query", func(t *testing.T) {
		c := qt.New(t)
		dir := writeIntegrityFixture(c)
		c.Setenv("PTAH_DIR", "file://"+dir+"?format=golang-migrate")

		stdout, _, err := runCompatExit("migrate", "hash")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(sumEntryNames(c, dir), qt.DeepEquals, golangMigrateCoveredSet)
	})

	t.Run("an atlas query overrides PTAH_DIR_FORMAT", func(t *testing.T) {
		c := qt.New(t)
		dir := writeIntegrityFixture(c)
		c.Setenv("PTAH_DIR_FORMAT", "goose")

		stdout, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=atlas")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
		c.Assert(sumEntryNames(c, dir), qt.DeepEquals, sqlSuffixCoveredSet)
	})

	t.Run("PTAH_DIR carries an atlas query", func(t *testing.T) {
		c := qt.New(t)
		dir := writeIntegrityFixture(c)
		c.Setenv("PTAH_DIR", "file://"+dir+"?format=atlas")

		stdout, _, err := runCompatExit("migrate", "hash")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(stdout, qt.Equals, "")
		c.Assert(sumEntryNames(c, dir), qt.DeepEquals, sqlSuffixCoveredSet)
	})
}

// TestCompatMigrateIntegrityDefaultDirectory_HappyPath covers an omitted
// --dir. The Atlas flag registers no default on these verbs, so the converted
// path has to fall back to the same directory the forwarded native command
// would use rather than to an empty path.
func TestCompatMigrateIntegrityDefaultDirectory_HappyPath(t *testing.T) {
	c := qt.New(t)
	workDir := c.TempDir()
	migrationsDir := filepath.Join(workDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	for name, content := range integrityFixture {
		path := filepath.Join(migrationsDir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	}
	t.Chdir(workDir)

	stdout, stderr, err := runCompatExit("migrate", "hash", "--dir-format", "golang-migrate")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
	c.Assert(sumEntryNames(c, migrationsDir), qt.DeepEquals, golangMigrateCoveredSet)

	validateOut, validateErrOut, validateErr := runCompatExit("migrate", "validate", "--dir-format", "golang-migrate")

	c.Assert(validateErr, qt.IsNil)
	c.Assert(validateOut, qt.Equals, "")
	c.Assert(validateErrOut, qt.Equals, "")
}

// TestCompatMigrateValidateConvertedDevURL_HappyPath covers --dev-url on a
// converted directory. Atlas CE replays a converted directory on the dev
// database exactly as it replays a native one, so dropping the flag silently
// would turn a failing validate into a passing one.
func TestCompatMigrateValidateConvertedDevURL_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	devDBPath := filepath.Join(c.TempDir(), "dev.db")
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
		[]byte("-- +goose Up\nCREATE TABLE compat_converted_dev_url (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	_, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=goose")
	c.Assert(err, qt.IsNil)

	stdout, stderr, err := runCompatExit(
		"migrate", "validate", "--dir", "file://"+dir+"?format=goose", "--dev-url", "sqlite://"+devDBPath)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
	// The replay cleans up after itself, so the table exists only during it.
	assertSQLiteTableCount(c, devDBPath, "compat_converted_dev_url", 0)
}

// TestCompatMigrateValidateConvertedDevURL_FailurePath covers the two orders
// that matter: integrity is checked before the directory is parsed or replayed,
// and a clean directory whose SQL does not execute still fails.
func TestCompatMigrateValidateConvertedDevURL_FailurePath(t *testing.T) {
	t.Run("invalid sql on a clean directory", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		devDBPath := filepath.Join(c.TempDir(), "dev.db")
		c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
			[]byte("-- +goose Up\nTHIS IS NOT SQL;\n"), 0o600), qt.IsNil)
		_, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=goose")
		c.Assert(err, qt.IsNil)

		_, stderr, err := runCompatExit(
			"migrate", "validate", "--dir", "file://"+dir+"?format=goose", "--dev-url", "sqlite://"+devDBPath)

		c.Assert(err, qt.ErrorMatches, "(?s)error validating migration SQL on dev database: .*")
		c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
		c.Assert(stderr, qt.Contains, "error validating migration SQL on dev database")
	})

	t.Run("integrity is checked before the dev database is touched", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
			[]byte("-- +goose Up\nCREATE TABLE never_replayed (id int);\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runCompatExit(
			"migrate", "validate", "--dir", "file://"+dir+"?format=goose",
			"--dev-url", "unsupported://must-not-connect")

		c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n"+
			"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
		c.Assert(stderr, qt.Equals, "Error: checksum file not found\n")
	})
}

// atlasChecksumPreamble is the guidance block Atlas CE writes to STDOUT before
// refusing a directory over its integrity file, with no `L<n>:` line because
// nothing mismatched — the directory could not be hashed at all.
const atlasChecksumPreamble = "You have a checksum error in your migration directory.\n" +
	"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n"

// directoryNamedSQLRefusal is the message every verb reports for a covered
// entry that turns out to be a directory. Ptah's wording is deliberately not
// the community binary's: see migratesum's coveredDirectoryError.
const directoryNamedSQLRefusal = `read file "%s": is a directory, not a migration file; ` +
	`rename it or move it out of the migration directory`

// writeDirectoryNamedSQLEntries writes files and empty directories under dir.
// Directories are created before files so a case can nest a migration inside a
// directory whose own name ends in .sql.
func writeDirectoryNamedSQLEntries(c *qt.C, dir string, files map[string]string, dirs []string) {
	c.Helper()
	for _, name := range dirs {
		c.Assert(os.MkdirAll(filepath.Join(dir, filepath.FromSlash(name)), 0o755), qt.IsNil)
	}
	for name, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(full), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(full, []byte(content), 0o600), qt.IsNil)
	}
}

func writeDirectoryNamedSQLFixture(c *qt.C, files map[string]string, dirs []string) string {
	c.Helper()
	dir := c.TempDir()
	writeDirectoryNamedSQLEntries(c, dir, files, dirs)
	return dir
}

// wantDirectoryRefusal renders the refusal message for name as an anchored
// regexp, so the assertion pins the whole string rather than a substring.
func wantDirectoryRefusal(name string) string {
	return regexp.QuoteMeta(fmt.Sprintf(directoryNamedSQLRefusal, name))
}

func assertNoAtlasSum(c *qt.C, dir string) {
	c.Helper()
	_, statErr := os.Stat(filepath.Join(dir, migratesum.AtlasFileName))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue, qt.Commentf("an atlas.sum was written for a refused directory"))
}

// The two bodies every directory-named-SQL fixture is built from. They are
// package constants because the refusal and the acceptance are separate tests
// over the same layouts.
const (
	directoryNamedSQLGooseBody = "-- +goose Up\nCREATE TABLE w (id int);\n"
	directoryNamedSQLPlainBody = "CREATE TABLE w (id int);\n"
)

// TestCompatMigrateHashRefusesADirectoryItsGlobMatches pins the per-format
// membership rule for a DIRECTORY whose name matches the layout's glob,
// measured against the pinned community binary v1.3.0 on 2026-08-03
// (stokaro/ptah#991).
//
// Atlas CE reaches every non-Flyway layout through a per-format glob, and a
// glob matches on the name alone, so a directory called weird.sql is a member of
// the covered set. The read that follows fails and the oracle refuses the whole
// directory, writing no atlas.sum:
//
//	$ atlas migrate hash --dir 'file://w?format=goose'
//	Error: sql/migrate: read file "weird.sql": read w/weird.sql: is a directory
//	exit=1, no atlas.sum written
//
// Ptah used to skip the entry and write a sum over the remainder — a sum the
// community binary then refused to read, which is the trap #991 reports.
//
// The rows a directory does NOT stop are in
// TestCompatMigrateHashWalksPastADirectoryItsGlobMisses, and the pair is what
// makes each row evidence about the RULE rather than about the headline shape:
// without the accepting half, "always refuse" passes here.
func TestCompatMigrateHashRefusesADirectoryItsGlobMatches(t *testing.T) {
	tests := []struct {
		name   string
		format string
		files  map[string]string
		dirs   []string
		// refused is the entry the oracle names, and the whole refusal is
		// rendered from it.
		refused string
	}{
		{
			name:    "goose refuses a directory its glob matches",
			format:  "goose",
			files:   map[string]string{"1_init.sql": directoryNamedSQLGooseBody},
			dirs:    []string{"weird.sql"},
			refused: "weird.sql",
		},
		{
			// The oracle globs *.up.sql for this format, so the suffix decides
			// membership rather than the read. Its partner is the golang-migrate
			// row beside weird.sql, which is hashed.
			name:    "golang-migrate refuses a directory its glob matches",
			format:  "golang-migrate",
			files:   map[string]string{"1_init.up.sql": directoryNamedSQLPlainBody},
			dirs:    []string{"weird.up.sql"},
			refused: "weird.up.sql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeDirectoryNamedSQLFixture(c, tt.files, tt.dirs)

			stdout, stderr, err := runCompatExit(
				"migrate", "hash", "--dir", "file://"+dir+"?format="+tt.format)

			c.Assert(stdout, qt.Equals, "", qt.Commentf("stderr:\n%s", stderr))
			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(err, qt.ErrorMatches, wantDirectoryRefusal(tt.refused))
			assertNoAtlasSum(c, dir)
		})
	}
}

// TestCompatMigrateHashWalksPastADirectoryItsGlobMisses is the accepting half
// of the same rule, and every row here is what stops "reject any .sql
// directory" from passing its sibling:
//
//   - goose without the directory at all, the plain control.
//   - golang-migrate beside weird.sql, which its *.up.sql glob does not match.
//   - flyway beside weird.sql, and with a migration nested inside it. The
//     oracle WALKS a Flyway tree instead of globbing it, so a directory is a
//     node it descends into and never reads: both tools exit 0 and produce
//     byte-identical sums. These rows fail if the fix is applied to treeNames.
//
// The sum's entry list is the assertion rather than the exit code, because a
// hash that quietly covered the wrong file set also exits 0.
func TestCompatMigrateHashWalksPastADirectoryItsGlobMisses(t *testing.T) {
	tests := []struct {
		name   string
		format string
		files  map[string]string
		dirs   []string
		// wantEntries is the covered set the written atlas.sum records.
		wantEntries []string
	}{
		{
			name:        "goose hashes the same layout without the directory",
			format:      "goose",
			files:       map[string]string{"1_init.sql": directoryNamedSQLGooseBody},
			wantEntries: []string{"1_init.sql"},
		},
		{
			name:        "golang-migrate ignores a directory outside its glob",
			format:      "golang-migrate",
			files:       map[string]string{"1_init.up.sql": directoryNamedSQLPlainBody},
			dirs:        []string{"weird.sql"},
			wantEntries: []string{"1_init.up.sql"},
		},
		{
			name:        "flyway walks past a directory named sql",
			format:      "flyway",
			files:       map[string]string{"V1__init.sql": directoryNamedSQLPlainBody},
			dirs:        []string{"weird.sql"},
			wantEntries: []string{"V1__init.sql"},
		},
		{
			name:   "flyway still covers a migration nested inside one",
			format: "flyway",
			files: map[string]string{
				"V1__init.sql":             directoryNamedSQLPlainBody,
				"weird.sql/V2__nested.sql": "CREATE TABLE n (id int);\n",
			},
			wantEntries: []string{"V1__init.sql", "weird.sql/V2__nested.sql"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeDirectoryNamedSQLFixture(c, tt.files, tt.dirs)

			stdout, stderr, err := runCompatExit(
				"migrate", "hash", "--dir", "file://"+dir+"?format="+tt.format)

			c.Assert(stdout, qt.Equals, "", qt.Commentf("stderr:\n%s", stderr))
			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(sumEntryNames(c, dir), qt.DeepEquals, tt.wantEntries)
		})
	}
}

// directoryNamedSQLShape is one way a directory named like a migration can
// appear beside a snapshot that was captured while the tree was clean.
//
// The second shape is not decoration: the capture predicate admits *.sql and
// the metadata names, so a note.txt inside is filtered out and, with nothing
// left underneath, the directory disappears from the snapshot again.
type directoryNamedSQLShape struct {
	name  string
	files map[string]string
	dirs  []string
}

func directoryNamedSQLShapes() []directoryNamedSQLShape {
	return []directoryNamedSQLShape{{
		name: "empty directory",
		dirs: []string{"2_evil.sql"},
	}, {
		name:  "directory holding an uncaptured file",
		files: map[string]string{"2_evil.sql/note.txt": "hello\n"},
	}}
}

// seedDirectoryNamedSQLSnapshot hashes a clean directory and only then lets the
// shape appear, so the sum under test is one Atlas CE itself would have
// written and the directory arrives after the capture.
func seedDirectoryNamedSQLSnapshot(c *qt.C, shape directoryNamedSQLShape) string {
	c.Helper()
	dir := writeDirectoryNamedSQLFixture(c,
		map[string]string{"1_init.sql": "CREATE TABLE w (id INTEGER PRIMARY KEY);\n"}, nil)
	hashOut, _, hashErr := runCompatExit("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(hashErr, qt.IsNil, qt.Commentf("seed:\n%s", hashOut))
	writeDirectoryNamedSQLEntries(c, dir, shape.files, shape.dirs)
	return dir
}

// TestCompatMigrateNativeDirectoryNamedSQL pins the three NATIVE verbs that
// verify a captured snapshot rather than the live directory and reproduce the
// community binary's stream layout exactly. `migrate lint` reaches the same
// state through its own integrity surface and is
// TestCompatMigrateLintDirectoryNamedSQL.
//
// This is the half a fix confined to the file-set selection does not reach, and
// it was measured to be exactly that: with the selection corrected and the
// snapshot untouched, `migrate apply` still exited 0 here. Those verbs verify an
// fsnapshot.Snapshot, which recorded only files, so a directory holding no
// captured file vanished between the capture and the check and the recomputed
// sum still matched.
//
// Measured against the pinned binary v1.3.0 on 2026-08-03, that binary exits 1
// on all four verbs, printing the checksum preamble on stdout and the read
// failure on stderr. Nothing unverified executes — a directory holds no SQL —
// so this is a loss of tamper DETECTION rather than of execution safety, and it
// is still exit 0 where the community binary exits 1.
func TestCompatMigrateNativeDirectoryNamedSQL(t *testing.T) {
	verbs := []struct {
		name string
		args func(dir, dbPath string) []string
	}{{
		name: "apply",
		args: func(dir, dbPath string) []string {
			return []string{"migrate", "apply", "--dir", "file://" + dir, "--url", "sqlite://" + dbPath}
		},
	}, {
		name: "status",
		args: func(dir, dbPath string) []string {
			return []string{"migrate", "status", "--dir", "file://" + dir, "--url", "sqlite://" + dbPath}
		},
	}, {
		name: "set",
		args: func(dir, dbPath string) []string {
			return []string{"migrate", "set", "1", "--dir", "file://" + dir, "--url", "sqlite://" + dbPath}
		},
	}}

	for _, shape := range directoryNamedSQLShapes() {
		for _, verb := range verbs {
			t.Run(shape.name+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				dir := seedDirectoryNamedSQLSnapshot(c, shape)

				stdout, stderr, err := runCompatExit(
					verb.args(dir, filepath.Join(c.TempDir(), "evil.db"))...)

				c.Assert(exitcode.Code(err, 0), qt.Equals, 1,
					qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
				assertAtlasChecksumStreams(c, stdout, stderr)
			})
		}
	}
}

// TestCompatMigrateLintDirectoryNamedSQL is the fourth verb of the same
// experiment, kept apart because it reports the refusal through its own
// integrity surface rather than in the community binary's stream layout — a
// divergence that predates #991 and shows identically on an ordinary tampered
// directory. What it owes here is the exit code and the reason.
func TestCompatMigrateLintDirectoryNamedSQL(t *testing.T) {
	for _, shape := range directoryNamedSQLShapes() {
		t.Run(shape.name, func(t *testing.T) {
			c := qt.New(t)
			dir := seedDirectoryNamedSQLSnapshot(c, shape)
			dbPath := filepath.Join(c.TempDir(), "evil.db")

			stdout, stderr, err := runCompatExit(
				"migrate", "lint", "--dir", "file://"+dir,
				"--dev-url", "sqlite://"+dbPath+"?mode=memory", "--latest", "1")

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stderr, qt.Contains, fmt.Sprintf(directoryNamedSQLRefusal, "2_evil.sql"))
		})
	}
}

// assertAtlasChecksumStreams pins the stream layout the community binary uses
// for a checksum refusal: guidance on stdout, the reason on stderr.
func assertAtlasChecksumStreams(c *qt.C, stdout, stderr string) {
	c.Helper()
	c.Assert(stdout, qt.Equals, atlasChecksumPreamble)
	c.Assert(stderr, qt.Equals,
		"Error: "+fmt.Sprintf(directoryNamedSQLRefusal, "2_evil.sql")+"\n")
}
