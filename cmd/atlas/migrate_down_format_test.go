package atlas_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/cmd/migratedown"
)

// writeMigrateDownFixture fills migrationsDir with two Atlas-format
// migrations plus ptah-style supplementary down files, and applies them to
// dbPath through `atlas migrate apply` so the revision table is Atlas-shaped.
func writeMigrateDownFixture(c *qt.C, migrationsDir, dbPath string) {
	c.Helper()
	write := func(name, sql string) {
		c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(sql+"\n"), 0o600), qt.IsNil)
	}
	write("1_init.sql", "CREATE TABLE down_fmt_users (id INTEGER PRIMARY KEY);")
	write("1_init.down.sql", "DROP TABLE down_fmt_users;")
	write("2_add_audit.sql", "CREATE TABLE down_fmt_audit (id INTEGER PRIMARY KEY);")
	write("2_add_audit.down.sql", "DROP TABLE down_fmt_audit;")

	apply := atlas.NewAtlasCommand()
	var out bytes.Buffer
	apply.SetOut(&out)
	apply.SetErr(&out)
	apply.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
	})
	c.Assert(apply.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
}

type migrateDownJSONReport struct {
	Driver  string `json:"Driver"`
	Dir     string `json:"Dir"`
	Planned []struct {
		Name        string `json:"Name"`
		Version     string `json:"Version"`
		Description string `json:"Description"`
	} `json:"Planned"`
	Reverted []struct {
		Name    string   `json:"Name"`
		Version string   `json:"Version"`
		Applied []string `json:"Applied"`
	} `json:"Reverted"`
	Current string `json:"Current"`
	Target  string `json:"Target"`
	Total   int    `json:"Total"`
	Error   string `json:"Error"`
}

func TestNewAtlasCommand_MigrateDownFormatRendersReportAndReverts(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-format.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)

	cmd := atlas.NewAtlasCommand()
	var out, errOut bytes.Buffer
	cmd.SetIn(strings.NewReader("YES\n"))
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--to-version", "1",
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stdout=%s stderr=%s", out.String(), errOut.String()))
	// The report is alone on stdout; the confirmation prompt went to stderr.
	c.Assert(errOut.String(), qt.Contains, "Type 'YES' to confirm")
	var report migrateDownJSONReport
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil, qt.Commentf("stdout=%s", out.String()))
	c.Assert(report.Driver, qt.Equals, "sqlite")
	c.Assert(report.Dir, qt.Equals, "file://"+migrationsDir)
	c.Assert(report.Planned, qt.HasLen, 1)
	c.Assert(report.Planned[0].Name, qt.Equals, "2_add_audit.sql")
	c.Assert(report.Planned[0].Version, qt.Equals, "2")
	c.Assert(report.Planned[0].Description, qt.Equals, "add_audit")
	c.Assert(report.Reverted, qt.HasLen, 1)
	c.Assert(report.Reverted[0].Applied, qt.DeepEquals, []string{"DROP TABLE down_fmt_audit"})
	c.Assert(report.Current, qt.Equals, "2")
	c.Assert(report.Target, qt.Equals, "1")
	c.Assert(report.Total, qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_users"), qt.Equals, 1)
}

func TestNewAtlasCommand_MigrateDownFormatDryRunPlansWithoutReverting(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-dry.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)

	cmd := atlas.NewAtlasCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--dry-run",
		"--format", `{{ len .Planned }}|{{ len .Reverted }}|{{ .Current }}|{{ .Target }}`,
	})

	err := cmd.Execute()

	// Dry run needs no confirmation, renders the plan, and reverts nothing.
	c.Assert(err, qt.IsNil, qt.Commentf("stderr=%s", errOut.String()))
	c.Assert(out.String(), qt.Equals, "2|0|2|")
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_users"), qt.Equals, 1)
}

func TestNewAtlasCommand_MigrateDownFormatFromEnvValue(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-env.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)
	t.Setenv("PTAH_FORMAT", "planned={{ len .Planned }}")

	cmd := atlas.NewAtlasCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--dry-run",
	})

	err := cmd.Execute()

	// PTAH_FORMAT selects the format path just like the arg mapper's env
	// convention would have injected --format.
	c.Assert(err, qt.IsNil, qt.Commentf("stderr=%s", errOut.String()))
	c.Assert(out.String(), qt.Equals, "planned=2")
}

func TestNewAtlasCommand_MigrateDownFormatDevURLVerifiesThenApplies(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-dev.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)

	cmd := atlas.NewAtlasCommand()
	var out, errOut bytes.Buffer
	cmd.SetIn(strings.NewReader("YES\n"))
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--to-version", "0",
		"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
		"--format", `reverted={{ len .Reverted }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stdout=%s stderr=%s", out.String(), errOut.String()))
	c.Assert(out.String(), qt.Equals, "reverted=2")
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_users"), qt.Equals, 0)
}

func TestNewAtlasCommand_MigrateDownFormatDevURLFailureAbortsTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-dev-abort.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)
	// Break the newest down file after applying, so only the dev replay can
	// catch it.
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "2_add_audit.down.sql"), []byte("DROP TABLE no_such_table;\n"), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--to-version", "1",
		"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	// The dev replay fails before the confirmation prompt and before the
	// target is touched.
	c.Assert(err, qt.ErrorMatches, `(?s)rollback verification failed: .*no_such_table.*`)
	c.Assert(out.String(), qt.Equals, "")
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 1)
	c.Assert(sqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "2"})
}

func TestNewAtlasCommand_MigrateDownDevURLForwardsToNativeShadowVerification(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-native-dev.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetIn(strings.NewReader("YES\n"))
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--to-version", "1",
		"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
		// No --revision-format: the forward defaults to Atlas revision
		// bookkeeping, matching the revision table `atlas migrate apply` wrote.
	})

	err := cmd.Execute()

	// Without --format the verb still forwards to the native command; --dev-url
	// now maps to the native --shadow-db verification instead of being
	// rejected.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "Rollback plan verified on shadow database")
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 0)
}

func TestNewAtlasCommand_MigrateDownDevURLForwardFailureAbortsTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-native-abort.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "2_add_audit.down.sql"), []byte("DROP TABLE no_such_table;\n"), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--to-version", "1",
		"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `(?s)rollback verification failed: .*no_such_table.*`)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 1)
}

// TestNewAtlasCommand_MigrateDownDefaultOutputMatchesNativeCommand pins the
// byte-identity contract: without --format, the wrapped verb forwards to the
// native `ptah migrations down` and its stdout is exactly the native
// command's stdout. The native invocation selects --revision-format atlas
// explicitly because the forward injects that default itself, so both runs
// perform the same real rollback against the Atlas revision table.
func TestNewAtlasCommand_MigrateDownDefaultOutputMatchesNativeCommand(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	runDown := func(dbName string, run func(migrationsDir, dbPath string) (string, error)) string {
		migrationsDir := filepath.Join(dir, "migrations-"+dbName)
		dbPath := filepath.Join(dir, dbName+".db")
		writeMigrateDownFixture(c, migrationsDir, dbPath)
		out, err := run(migrationsDir, dbPath)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		// The migration directory path differs per run; normalize it so the
		// remaining bytes must match exactly.
		return strings.ReplaceAll(out, migrationsDir, "<dir>")
	}

	atlasOut := runDown("atlas-default", func(migrationsDir, dbPath string) (string, error) {
		cmd := atlas.NewAtlasCommand()
		var out, errOut bytes.Buffer
		cmd.SetIn(strings.NewReader("YES\n"))
		cmd.SetOut(&out)
		cmd.SetErr(&errOut)
		cmd.SetArgs([]string{
			"migrate", "down",
			"--url", "sqlite://" + dbPath,
			"--dir", "file://" + migrationsDir,
			"--to-version", "1",
		})
		err := cmd.Execute()
		return strings.ReplaceAll(out.String(), dbPath, "<db>"), err
	})

	nativeOut := runDown("native-default", func(migrationsDir, dbPath string) (string, error) {
		cmd := migratedown.NewMigrateDownCommand()
		var out, errOut bytes.Buffer
		cmd.SetIn(strings.NewReader("YES\n"))
		cmd.SetOut(&out)
		cmd.SetErr(&errOut)
		cmd.SetArgs([]string{
			"--db-url", "sqlite://" + dbPath,
			"--migrations-dir", migrationsDir,
			"--target", "1",
			"--revision-format", "atlas",
		})
		err := cmd.Execute()
		return strings.ReplaceAll(out.String(), dbPath, "<db>"), err
	})

	c.Assert(atlasOut, qt.Equals, nativeOut)
}

// TestNewAtlasCommand_MigrateDownDefaultsToAtlasRevisionFormat pins the
// revision-format default of the forward: a bare `atlas migrate down` must
// revert revisions created by `atlas migrate apply` (Atlas revision
// bookkeeping), not silently no-op against an empty ptah revision table.
func TestNewAtlasCommand_MigrateDownDefaultsToAtlasRevisionFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-default-revisions.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetIn(strings.NewReader("YES\n"))
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--to-version", "1",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_users"), qt.Equals, 1)
	c.Assert(sqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1"})
}

// TestNewAtlasCommand_MigrateDownRevisionFormatPtahOverridesDefault pins the
// escape hatch: an explicit native `--revision-format ptah` pass-through is
// appended after the forward's Atlas default and wins, so the run reads ptah
// revision bookkeeping and leaves the Atlas-applied state untouched.
func TestNewAtlasCommand_MigrateDownRevisionFormatPtahOverridesDefault(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-ptah-revisions.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--revision-format", "ptah",
	})

	err := cmd.Execute()

	// The ptah revision table is empty, so the rollback is a no-op and the
	// Atlas-applied schema and revisions stay in place.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "already at or below target version 0")
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_users"), qt.Equals, 1)
	c.Assert(sqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "2"})
}

func TestNewAtlasCommand_MigrateDownFormatDeclinedConfirmationWritesNoReport(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-declined.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)

	cmd := atlas.NewAtlasCommand()
	var out, errOut bytes.Buffer
	cmd.SetIn(strings.NewReader("NO\n"))
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	// Declining keeps the native contract: exit 0, nothing reverted, and no
	// report bytes on stdout.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	c.Assert(errOut.String(), qt.Contains, "Migration rollback canceled.")
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 1)
}

func TestNewAtlasCommand_MigrateDownFormatUsesAtlasProjectConfig(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(projectDir, "migrations")
	dbPath := filepath.Join(dir, "down-project.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)
	c.Assert(os.MkdirAll(outsideDir, 0o755), qt.IsNil)
	t.Chdir(outsideDir)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
		"--dry-run",
		"--format", `{{ .Dir }}|{{ len .Planned }}`,
	})

	err := cmd.Execute()

	// env url and the project-relative migration dir resolve on the format
	// path with the same precedence the forward path applies. The report's Dir
	// is the value as the project loader stores it (file:// stripped), matching
	// the apply report's behavior with a config-supplied dir.
	c.Assert(err, qt.IsNil, qt.Commentf("stderr=%s", errOut.String()))
	c.Assert(out.String(), qt.Equals, "migrations|2")
}

func TestNewAtlasCommand_MigrateDownFormatValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "empty_format",
			args: []string{"--format", ""},
			want: `--format must not be empty`,
		},
		{
			name: "invalid_template",
			args: []string{"--format", "{{ .Broken"},
			want: `parse --format template: .*`,
		},
		{
			name: "missing_url",
			args: []string{"--format", "{{ json . }}", "--dir", "file://migrations"},
			want: `database URL is required`,
		},
		{
			name: "missing_dir",
			args: []string{"--format", "{{ json . }}", "--url", "sqlite://x.db"},
			want: `migrations directory is required`,
		},
		{
			name: "invalid_to_version",
			args: []string{"--format", "{{ json . }}", "--url", "sqlite://x.db", "--dir", "file://m", "--to-version", "banana"},
			want: `invalid --to-version "banana": .*`,
		},
		{
			name: "positional_rejected",
			args: []string{"--format", "{{ json . }}", "stray"},
			want: `atlas migrate down accepts no arguments, got \["stray"\]`,
		},
		{
			name: "native_only_flag_rejected",
			args: []string{"--format", "{{ json . }}", "--pre-down-hook", "echo"},
			want: `atlas migrate down: unknown flag: --pre-down-hook`,
		},
		{
			name: "remote_dir_rejected",
			args: []string{"--format", "{{ json . }}", "--url", "sqlite://x.db", "--dir", "atlas://repo"},
			want: `atlas migrate down --dir: only local file:// migration directories are supported`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := atlas.NewAtlasCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append([]string{"migrate", "down"}, tt.args...))

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

// TestNewAtlasCommand_MigrateDownWaivedFlagsRejectWithRationale pins the
// recorded waivers for the registry/cloud-bound down flags on both the
// forward path and the --format path: identical wording, no silent drops.
func TestNewAtlasCommand_MigrateDownWaivedFlagsRejectWithRationale(t *testing.T) {
	waived := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "to_tag",
			args: []string{"--to-tag", "release-v1"},
			want: `atlas migrate down accepts --to-tag, but Ptah does not implement its behavior: migration tags exist only in Atlas Registry \(Atlas Cloud\); use --to-version with a migration version instead`,
		},
		{
			name: "skip_checks",
			args: []string{"--skip-checks"},
			want: `atlas migrate down accepts --skip-checks, but Ptah does not implement its behavior: Atlas down checks are part of the Atlas Cloud plan-approval flow; Ptah reverts through locally reviewed down migrations and has no generated checks to skip`,
		},
		{
			name: "plan",
			args: []string{"--plan"},
			want: `atlas migrate down accepts --plan, but Ptah does not implement its behavior: dynamic down planning is bound to the Atlas Cloud plan-approval flow; use --dev-url to verify the pre-planned rollback on a dev database instead`,
		},
	}

	for _, tt := range waived {
		for _, path := range []struct {
			name  string
			extra []string
		}{
			{name: "forward"},
			{name: "format", extra: []string{"--format", "{{ json . }}"}},
		} {
			t.Run(tt.name+"_"+path.name, func(t *testing.T) {
				c := qt.New(t)
				cmd := atlas.NewAtlasCommand()
				var out bytes.Buffer
				cmd.SetOut(&out)
				cmd.SetErr(&out)
				cmd.SetArgs(append(append([]string{"migrate", "down"}, path.extra...), tt.args...))

				err := cmd.Execute()

				c.Assert(err, qt.ErrorMatches, tt.want)
			})
		}
	}
}

func TestNewCompatCommand_MigrateDownFormatResolvesAtRoot(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down-compat.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)

	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--dry-run",
		"--format", "planned={{ len .Planned }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr=%s", errOut.String()))
	c.Assert(out.String(), qt.Equals, "planned=2")
}
