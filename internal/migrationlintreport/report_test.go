package migrationlintreport_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/migrationlintreport"
	"ptah.run/internal/migrationsnapshot"
	migrationlint "ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

type gitExitStatus128 struct{}

func (gitExitStatus128) Error() string { return "exit status 128" }

// TestGitCommandErrorKeepsTheNativeRendering pins the producer's own text. The
// native surface prints the full invocation so a failed selection can be
// reproduced by hand, and the compat adapter builds its terser form from the
// fields rather than by trimming this string.
func TestGitCommandErrorKeepsTheNativeRendering(t *testing.T) {
	c := qt.New(t)

	err := &migrationlintreport.GitCommandError{
		Subcommand: "diff",
		Args:       []string{"diff", "--name-only", "nosuchbranch...HEAD"},
		Output:     "fatal: bad revision",
		Err:        gitExitStatus128{},
	}

	c.Assert(err.Error(), qt.Equals,
		"git diff --name-only nosuchbranch...HEAD: exit status 128: fatal: bad revision")
	c.Assert(errors.Unwrap(err), qt.Equals, error(gitExitStatus128{}))
}

func writeLintTestFile(c *qt.C, dir, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func writeWarningMigration(c *qt.C, dir string) {
	c.Helper()
	writeLintTestFile(c, dir, "0000000001_index.up.sql", "CREATE INDEX idx_users_id ON users (id);\n")
	writeLintTestFile(c, dir, "0000000001_index.down.sql", "DROP INDEX idx_users_id;\n")
}

func runGit(c *qt.C, dir string, args ...string) {
	c.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
}

// findingRules lists the reported rule codes in report order. The pair above
// produces one warning per direction: PG101 for the blocking build and PG106
// for the blocking drop the rollback performs (stokaro/ptah#997).
func findingRules(findings []migrationlint.Finding) []string {
	rules := make([]string, 0, len(findings))
	for _, finding := range findings {
		rules = append(rules, finding.Rule)
	}
	return rules
}

func TestBuild_UsesProjectConfigWithoutCobra(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	latest := 1
	writeLintTestFile(c, dir, "0000000001_old.up.sql", "DROP TABLE old_data;\n")
	writeLintTestFile(c, dir, "0000000001_old.down.sql", "CREATE TABLE old_data (id INT);\n")
	writeLintTestFile(c, dir, "0000000002_new.up.sql", "ALTER TABLE users DROP COLUMN legacy;\n")
	writeLintTestFile(c, dir, "0000000002_new.down.sql", "ALTER TABLE users ADD COLUMN legacy TEXT;\n")

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       dir,
		DirFormat: string(migrationfile.DirFormatPtah),
		FailOn:    migrationlintreport.FailOnNone,
	}, projectconfig.Config{
		Lint: projectconfig.LintConfig{
			Dialect:       "postgres",
			DisabledRules: []string{"MF"},
			Latest:        &latest,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed, qt.IsFalse)
	c.Assert(report.Dialect, qt.Equals, "postgres")
	c.Assert(report.DisabledRules, qt.DeepEquals, []string{"MF"})
	c.Assert(report.Versions, qt.DeepEquals, []int64{2})
	c.Assert(findingRules(report.Findings), qt.DeepEquals, []string{"BC104", "DS102"})
	c.Assert(report.Findings[1].File, qt.Contains, "0000000002_new.up.sql")
}

func TestBuild_ExplicitEmptyProjectDirReachesValidation(t *testing.T) {
	c := qt.New(t)
	cfg, err := projectconfig.ParsePtah([]byte(`
migration:
  dir: ""
`), "ptah.yaml", "")
	c.Assert(err, qt.IsNil)

	_, err = migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       t.TempDir(),
		DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect:   "sqlite",
		FailOn:    migrationlintreport.FailOnNone,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, cfg)

	c.Assert(err, qt.ErrorMatches, `migrations directory : .*`)
}

func TestBuild_ExplicitZeroProjectLatestReachesValidation(t *testing.T) {
	c := qt.New(t)
	cfg, err := projectconfig.ParsePtah([]byte(`
lint:
  latest: 0
`), "ptah.yaml", "")
	c.Assert(err, qt.IsNil)

	_, err = migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       t.TempDir(),
		FS:        fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);")}},
		DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect:   "sqlite",
		FailOn:    migrationlintreport.FailOnNone,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, cfg)

	c.Assert(err, qt.ErrorMatches, `--latest must be greater than zero`)
}

func TestBuild_ExplicitGitBaseSuppressesProjectLatest(t *testing.T) {
	c := qt.New(t)
	latest := 1
	cfg := projectconfig.Config{
		Lint: projectconfig.LintConfig{Latest: &latest},
	}

	_, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       t.TempDir(),
		FS:        fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);")}},
		DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect:   "sqlite",
		GitBase:   "-unsafe",
		FailOn:    migrationlintreport.FailOnNone,
		Changed: migrationlintreport.ChangedOptions{
			Dialect: true,
			GitBase: true,
		},
	}, cfg)

	c.Assert(err, qt.ErrorMatches, `--git-base "-unsafe" is not a safe Git ref`)
}

func TestBuild_ExplicitLatestSuppressesProjectGitSelector(t *testing.T) {
	c := qt.New(t)
	cfg := projectconfig.Config{
		Lint: projectconfig.LintConfig{
			GitBase: "-unsafe",
			GitDir:  "/not/a/repository",
		},
	}

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       t.TempDir(),
		FS:        fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);")}},
		DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect:   "sqlite",
		Latest:    1,
		FailOn:    migrationlintreport.FailOnNone,
		Changed: migrationlintreport.ChangedOptions{
			Dialect: true,
			Latest:  true,
		},
	}, cfg)

	c.Assert(err, qt.IsNil)
	c.Assert(report.Versions, qt.DeepEquals, []int64{1})
}

func TestBuild_LatestAndAnalysisShareOneSourceSnapshot(t *testing.T) {
	c := qt.New(t)
	source := &countingSnapshotSource{
		FS: fstest.MapFS{
			"1_old.sql": {Data: []byte("DROP TABLE old_data;\n")},
			"2_new.sql": {Data: []byte("DROP TABLE new_data;\n")},
		},
		reads: make(map[string]int),
	}

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       t.TempDir(),
		FS:        source,
		DirFormat: string(migrationfile.DirFormatAtlas),
		FailOn:    migrationlintreport.FailOnNone,
		Latest:    1,
		Changed:   migrationlintreport.ChangedOptions{Latest: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Versions, qt.DeepEquals, []int64{2})
	c.Assert(findingRules(report.Findings), qt.DeepEquals, []string{"BC103", "DS101"})
	c.Assert(report.Findings[0].File, qt.Contains, "2_new.sql")
	c.Assert(report.Findings[1].File, qt.Contains, "2_new.sql")
	c.Assert(source.reads, qt.DeepEquals, map[string]int{
		"1_old.sql": 1,
		"2_new.sql": 1,
	})
}

func TestBuild_LatestSelectsAtlasBareRepeatableAfterNumericMigrations(t *testing.T) {
	c := qt.New(t)

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime: selectedRuntime(c),
		Dir:     t.TempDir(),
		FS: fstest.MapFS{
			"1_create_users.sql": {Data: []byte("CREATE TABLE users (id int);\n")},
			"R__drop_users.sql":  {Data: []byte("DROP TABLE users;\n")},
		},
		DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect:   "sqlite",
		FailOn:    migrationlintreport.FailOnNone,
		Latest:    1,
		Changed:   migrationlintreport.ChangedOptions{Latest: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Versions, qt.DeepEquals, []int64{0})
	c.Assert(report.VersionKeys, qt.DeepEquals, []string{"R"})
	c.Assert(findingRules(report.Findings), qt.DeepEquals, []string{"BC103", "DS101"})
	c.Assert(report.Findings[0].File, qt.Contains, "R__drop_users.sql")
	c.Assert(report.Findings[1].File, qt.Contains, "R__drop_users.sql")
}

func TestBuild_GitBaseSelectsAtlasRepeatableByRevisionKey(t *testing.T) {
	c := qt.New(t)
	repo, err := filepath.EvalSymlinks(t.TempDir())
	c.Assert(err, qt.IsNil)
	migrationsDir := filepath.Join(repo, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o700), qt.IsNil)
	runGit(c, repo, "init")
	runGit(c, repo, "config", "commit.gpgsign", "false")
	runGit(c, repo, "config", "user.email", "ptah@example.com")
	runGit(c, repo, "config", "user.name", "Ptah Test")
	writeLintTestFile(c, migrationsDir, "1_create_users.sql", "CREATE TABLE users (id int);\n")
	writeLintTestFile(c, migrationsDir, "2_create_accounts.sql", "CREATE TABLE accounts (id int);\n")
	runGit(c, repo, "add", "migrations")
	runGit(c, repo, "commit", "-m", "baseline")
	writeLintTestFile(c, migrationsDir, "2R_drop_users.sql", "DROP TABLE users;\n")
	runGit(c, repo, "add", "migrations/2R_drop_users.sql")
	runGit(c, repo, "commit", "-m", "repeatable")

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       migrationsDir,
		DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect:   "sqlite",
		GitBase:   "HEAD~1",
		GitDir:    repo,
		FailOn:    migrationlintreport.FailOnNone,
		Changed: migrationlintreport.ChangedOptions{
			GitBase: true,
			GitDir:  true,
		},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Versions, qt.DeepEquals, []int64{2})
	c.Assert(report.VersionKeys, qt.DeepEquals, []string{"2R"})
	c.Assert(findingRules(report.Findings), qt.DeepEquals, []string{"BC103", "DS101"})
	c.Assert(report.Findings[0].File, qt.Contains, "2R_drop_users.sql")
	c.Assert(report.Findings[1].File, qt.Contains, "2R_drop_users.sql")
}

func TestBuild_ProvidedSnapshotDoesNotRequireSourceDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeLintTestFile(c, dir, "1_drop.sql", "DROP TABLE users;\n")
	snapshot, err := migrationsnapshot.Capture(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	c.Assert(os.RemoveAll(dir), qt.IsNil)

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       dir,
		FS:        snapshot,
		DirFormat: string(migrationfile.DirFormatAtlas),
		FailOn:    migrationlintreport.FailOnNone,
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(findingRules(report.Findings), qt.DeepEquals, []string{"BC103", "DS101"})
}

func TestBuild_LoadsConventionalLintConfigFromSnapshot(t *testing.T) {
	c := qt.New(t)
	source := &countingSnapshotSource{
		FS: fstest.MapFS{
			".ptah-lint.yaml": {Data: []byte("disabled-rules: [DS101]\n")},
			"1_drop.sql":      {Data: []byte("DROP TABLE users;\n")},
		},
		reads: make(map[string]int),
	}

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       t.TempDir(),
		FS:        source,
		DirFormat: string(migrationfile.DirFormatAtlas),
		FailOn:    migrationlintreport.FailOnNone,
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.DisabledRules, qt.DeepEquals, []string{"DS101"})
	// The config disables the data-loss rule and the drop is still reported,
	// for the rollout break it also is. A config written before that rule
	// existed silences one of the two consequences, not the statement.
	c.Assert(findingRules(report.Findings), qt.DeepEquals, []string{"BC103"})
	c.Assert(source.reads, qt.DeepEquals, map[string]int{
		".ptah-lint.yaml": 1,
		"1_drop.sql":      1,
	})
}

func TestBuild_FailOnErrorDoesNotFailWarnings(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeWarningMigration(c, dir)

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       dir,
		DirFormat: string(migrationfile.DirFormatPtah),
		Dialect:   "postgres",
		FailOn:    migrationlintreport.FailOnError,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed, qt.IsFalse)
	c.Assert(findingRules(report.Findings), qt.DeepEquals, []string{"PG106", "PG101"})
}

func TestBuild_FailOnAnyFailsWarnings(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeWarningMigration(c, dir)

	report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Runtime:   selectedRuntime(c),
		Dir:       dir,
		DirFormat: string(migrationfile.DirFormatPtah),
		Dialect:   "postgres",
		FailOn:    migrationlintreport.FailOnAny,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed, qt.IsTrue)
	c.Assert(findingRules(report.Findings), qt.DeepEquals, []string{"PG106", "PG101"})
}

func TestWrite_GitHubActionsEscapesWorkflowCommandCharacters(t *testing.T) {
	c := qt.New(t)

	var buf bytes.Buffer
	err := migrationlintreport.Write(&buf, migrationlintreport.FormatGitHubActions, migrationlintreport.Report{
		Findings: []migrationlint.Finding{{
			Rule:     "DS101",
			Severity: migrationlint.SeverityError,
			File:     "dir/evil,file::name.sql",
			Line:     3,
			Message:  "50% data loss\r\nsecond line",
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Contains, "::error file=dir/evil%2Cfile%3A%3Aname.sql,line=3::")
	c.Assert(buf.String(), qt.Contains, "DS101: 50%25 data loss%0D%0Asecond line")
	c.Assert(buf.String(), qt.Not(qt.Contains), "evil,file::name")
}

type countingSnapshotSource struct {
	fs.FS
	reads map[string]int
}

func (f *countingSnapshotSource) ReadFile(name string) ([]byte, error) {
	f.reads[name]++
	return fs.ReadFile(f.FS, name)
}

func TestWrite_GitHubActionsEscapesErrorReport(t *testing.T) {
	c := qt.New(t)

	var buf bytes.Buffer
	err := migrationlintreport.Write(&buf, migrationlintreport.FormatGitHubActions, migrationlintreport.ErrorReport(
		migrationlintreport.FailOnError,
		"bad\nnews: 100%",
	))

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Equals, "::error::bad%0Anews: 100%25\n")
}

// duckDBSchemes is a lookup that would serve the duckdb scheme, as a lazy
// provider session lists the schemes of the engines it can start. Nothing here
// connects.
type duckDBSchemes struct{}

func (duckDBSchemes) NativeRegistration(context.Context, string) (dbschema.NativeRegistration, bool, error) {
	return dbschema.NativeRegistration{}, false, nil
}

func (duckDBSchemes) NativeSchemes() map[string]string { return map[string]string{"duckdb": "duckdb"} }

// duckDBDev provisions duckdb dev databases, as a provider that owns the
// engine's docker URLs does. Nothing here provisions one.
type duckDBDev struct{}

func (duckDBDev) DevEngines() []string { return []string{"duckdb"} }

func (duckDBDev) ProvisionDev(context.Context, string) (string, func(), error) {
	return "", func() {}, errors.New("not provisioned")
}

// servingDuckDB is a context whose provider selection serves the duckdb engine.
func servingDuckDB(c *qt.C) context.Context {
	c.Helper()
	return devdocker.WithExternal(dbschema.WithNativeLookup(c.Context(), duckDBSchemes{}), duckDBDev{})
}

// duckDBRuntime is the bundled runtime beside a provider target duckdb, with
// the alias duck, as a run that selected the provider holds it.
func duckDBRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := builtin.New(engine.Provider{ID: "example.org/duckdb", Targets: []engine.Target{{Name: "duckdb", Aliases: []string{"duck"}}}})
	c.Assert(err, qt.IsNil)
	return runtime
}

// A dialect the run's runtime serves and a dev URL its selection serves name
// an engine that is not a built-in dialect, and pass the checks that run
// before the migration source is read, so a run reaches the provider that
// splits and replays the files. The dialect resolves to the target's name.
func TestPrepareSourceOptions_HappyPath_AProviderServedEngine(t *testing.T) {
	for _, row := range []struct{ name, dialect, devURL, want string }{
		{"a dev URL", "", "duckdb:///tmp/dev.db", ""},
		{"a docker dev URL", "", "docker://duckdb/1/dev", ""},
		{"a dialect and a dev URL", "duckdb", "duckdb:///tmp/dev.db", "duckdb"},
		{"an alias", " Duck ", "", "duckdb"},
		{"a built-in dialect beside it", "pgx", "", "postgres"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			opts, err := migrationlintreport.PrepareSourceOptions(servingDuckDB(c), migrationlintreport.Options{
				Runtime: duckDBRuntime(c), Dir: t.TempDir(), DirFormat: string(migrationfile.DirFormatPtah),
				Dialect: row.dialect, DevURL: row.devURL,
			}, projectconfig.Config{})

			c.Assert(err, qt.IsNil)
			c.Assert(opts.Dialect, qt.Equals, row.want)
		})
	}
}

// The selection and the runtime are what name the engine: a dev URL no
// selection serves, and a dialect the runtime does not serve, are refused as
// the built-in tables refuse them. A built-in dialect lint does not analyze
// stays refused beside a runtime that registers it.
func TestPrepareSourceOptions_FailurePath_AnEngineTheRunDoesNotServe(t *testing.T) {
	for _, row := range []struct{ name, dialect, devURL, want string }{
		{"a dev URL without a selection", "", "duckdb:///tmp/dev.db", `unsupported --dev-url dialect "duckdb:///tmp/dev.db"`},
		{"a dev URL", "", "nosuch://host/dev", `unsupported --dev-url dialect "nosuch://host/dev"`},
		{"a dialect the bundled runtime does not serve", "duckdb", "", `invalid --dialect value "duckdb": expected .*`},
		{"a dialect", "nosuch", "", `invalid --dialect value "nosuch": expected .*`},
		{"a built-in dialect lint does not analyze", "oracle", "", `invalid --dialect value "oracle": expected .*`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := migrationlintreport.PrepareSourceOptions(c.Context(), migrationlintreport.Options{
				Runtime: selectedRuntime(c), Dir: t.TempDir(), DirFormat: string(migrationfile.DirFormatPtah),
				Dialect: row.dialect, DevURL: row.devURL,
			}, projectconfig.Config{})

			c.Assert(err, qt.ErrorMatches, row.want)
		})
	}
}
