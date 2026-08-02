package atlas_test

import (
	"os"
	"path/filepath"
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
	c := qt.New(t)

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
		c.Run(tt.name, func(c *qt.C) {
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
	c := qt.New(t)

	c.Run("flag overrides project format", func(c *qt.C) {
		configPath, migrationsDir := writeIntegrityProject(c, "flyway")

		stdout, _, err := runCompatExit(
			"migrate", "hash", "--config", "file://"+configPath, "--env", "local", "--dir-format", "goose")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(sumEntryNames(c, migrationsDir), qt.DeepEquals, sqlSuffixCoveredSet)
	})

	c.Run("query overrides project format", func(c *qt.C) {
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
	c.Run("atlas query overrides a converted project format", func(c *qt.C) {
		configPath, migrationsDir := writeIntegrityProject(c, "flyway")

		stdout, stderr, err := runCompatExit(
			"migrate", "hash", "--config", "file://"+configPath, "--env", "local",
			"--dir", "file://"+migrationsDir+"?format=atlas")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
		c.Assert(sumEntryNames(c, migrationsDir), qt.DeepEquals, sqlSuffixCoveredSet)
	})

	c.Run("project format applies when no spelling is given", func(c *qt.C) {
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
	c := qt.New(t)

	c.Run("PTAH_DIR_FORMAT selects the layout", func(c *qt.C) {
		dir := writeIntegrityFixture(c)
		c.Setenv("PTAH_DIR_FORMAT", "flyway")

		stdout, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir)

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(sumEntryNames(c, dir), qt.DeepEquals, flywayCoveredSet)
	})

	c.Run("PTAH_DIR carries a converted query", func(c *qt.C) {
		dir := writeIntegrityFixture(c)
		c.Setenv("PTAH_DIR", "file://"+dir+"?format=golang-migrate")

		stdout, _, err := runCompatExit("migrate", "hash")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(sumEntryNames(c, dir), qt.DeepEquals, golangMigrateCoveredSet)
	})

	c.Run("an atlas query overrides PTAH_DIR_FORMAT", func(c *qt.C) {
		dir := writeIntegrityFixture(c)
		c.Setenv("PTAH_DIR_FORMAT", "goose")

		stdout, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=atlas")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
		c.Assert(sumEntryNames(c, dir), qt.DeepEquals, sqlSuffixCoveredSet)
	})

	c.Run("PTAH_DIR carries an atlas query", func(c *qt.C) {
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
	c := qt.New(t)

	c.Run("invalid sql on a clean directory", func(c *qt.C) {
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

	c.Run("integrity is checked before the dev database is touched", func(c *qt.C) {
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

// TestCompatMigrateHashDirectoryNamedSQL_KnownDivergence pins MINOR 7, measured
// against the pinned CE binary on 2026-08-02.
//
// A DIRECTORY whose name ends in .sql is matched by Atlas CE's own glob, which
// then fails to read it, so CE refuses to hash the whole directory and writes
// nothing:
//
//	$ atlas migrate hash --dir 'file://w?format=goose'
//	Error: sql/migrate: read file "weird.sql": read w/weird.sql: is a directory
//	exit=1, no atlas.sum written
//
// atlasmigrateimport.SumFileNames skips directories, so the converted path
// writes a sum instead — a sum CE then refuses to read:
//
//	$ atlas migrate validate --dir 'file://w?format=goose'   # sum written by Ptah
//	Error: sql/migrate: read file "weird.sql": read w/weird.sql: is a directory
//
// An earlier version of this comment called that the only direction that
// matters, because CE can never produce such a sum itself. That is true and
// beside the point: CE hashes the directory BEFORE the *.sql directory exists,
// and the shape then appears on a directory whose sum CE wrote. Measured on
// 2026-08-02, that is a refusal on CE and an apply here:
//
//	$ mkdir 2_evil.sql            # after `atlas migrate hash`
//	$ atlas       migrate apply … Error: … read "2_evil.sql": … is a directory   exit=1
//	$ ptah-compat migrate apply … Migration complete. Current version: 1         exit=0
//
// Nothing unverified executes — a directory holds no SQL — so this is loss of
// tamper DETECTION rather than of execution safety. It is still CE refusing
// where Ptah applies, so the apply direction is pinned here alongside the
// hash and validate ones (stokaro/ptah#991).
//
// The native atlas path globs like CE and fails, differently worded. The two
// Ptah paths therefore disagree with each other, which is why this is pinned
// rather than left to be discovered.
func TestCompatMigrateHashDirectoryNamedSQL_KnownDivergence(t *testing.T) {
	c := qt.New(t)

	c.Run("converted layout skips it and writes a sum", func(c *qt.C) {
		dir := c.TempDir()
		c.Assert(os.MkdirAll(filepath.Join(dir, "weird.sql"), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
			[]byte("-- +goose Up\nCREATE TABLE w (id int);\n"), 0o600), qt.IsNil)

		stdout, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=goose")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(stdout, qt.Equals, "")
		c.Assert(sumEntryNames(c, dir), qt.DeepEquals, []string{"1_init.sql"})
	})

	c.Run("apply accepts a sum CE wrote before the directory appeared", func(c *qt.C) {
		dir := c.TempDir()
		c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
			[]byte("-- +goose Up\nCREATE TABLE w (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
		// Hashed while the directory holds only the migration, so the sum is one
		// Atlas CE would have written too.
		stdout, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=goose")
		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(os.MkdirAll(filepath.Join(dir, "2_evil.sql"), 0o755), qt.IsNil)

		dbPath := filepath.Join(c.TempDir(), "evil.db")
		stdout, stderr, err := runCompatExit(
			"migrate", "apply",
			"--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir+"?format=goose",
		)

		// Atlas CE exits 1 here. Ptah applies, because SumFileNames skips the
		// directory and the recomputed sum still matches.
		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout, qt.Not(qt.Contains), "checksum")
	})

	c.Run("native atlas layout refuses it", func(c *qt.C) {
		dir := c.TempDir()
		c.Assert(os.MkdirAll(filepath.Join(dir, "weird.sql"), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
			[]byte("CREATE TABLE w (id int);\n"), 0o600), qt.IsNil)

		_, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir)

		c.Assert(err, qt.ErrorMatches, `failed to read weird.sql: .*is a directory`)
		c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
		_, statErr := os.Stat(filepath.Join(dir, migratesum.AtlasFileName))
		c.Assert(os.IsNotExist(statErr), qt.IsTrue)
	})
}
