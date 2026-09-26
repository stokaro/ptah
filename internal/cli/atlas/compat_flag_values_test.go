package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// The flag values below are refused, or accepted, as the pinned community
// binary v1.3.0 refuses or accepts them. Every row was measured there on
// 2026-09-26 against PostgreSQL 18 (stokaro/ptah#3689), and each refusal's
// whole standard error is asserted, with standard output empty.

// flagValueFixture is a SQLite target, a schema file, a hashed migration
// directory and a dev database, as paths and URLs.
type flagValueFixture struct {
	root      string
	target    string
	schema    string
	dir       string
	devURL    string
	targetDB  string
	emptyDB   string
	emptyPath string
}

func newFlagValueFixture(c *qt.C) flagValueFixture {
	c.Helper()
	root := c.TempDir()
	schema := filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	emptyPath := filepath.Join(root, "empty.db")
	return flagValueFixture{
		root:      root,
		target:    filepath.Join(root, "target.db"),
		schema:    "file://" + schema,
		dir:       "file://" + dir,
		devURL:    "sqlite://" + filepath.Join(root, "dev.db"),
		targetDB:  "sqlite://" + filepath.Join(root, "target.db"),
		emptyDB:   "sqlite://" + emptyPath,
		emptyPath: emptyPath,
	}
}

// TestCompatLockTimeout_FailurePath: --lock-timeout is a duration flag on the
// pinned binary, so a value that is not a duration is refused by the flag
// parser, before the verb runs -- ahead of a missing --url, too.
func TestCompatLockTimeout_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		args       func(fx flagValueFixture) []string
		wantStderr string
	}{
		{
			name: "schema apply, a word",
			args: func(fx flagValueFixture) []string {
				return []string{"schema", "apply", "--url", fx.targetDB, "--to", fx.schema, "--dev-url", fx.devURL,
					"--auto-approve", "--lock-timeout", "bogus"}
			},
			wantStderr: "Error: invalid argument \"bogus\" for \"--lock-timeout\" flag: time: invalid duration \"bogus\"\n",
		},
		{
			name: "schema apply, an empty value",
			args: func(fx flagValueFixture) []string {
				return []string{"schema", "apply", "--url", fx.targetDB, "--to", fx.schema, "--dev-url", fx.devURL,
					"--auto-approve", "--lock-timeout", ""}
			},
			wantStderr: "Error: invalid argument \"\" for \"--lock-timeout\" flag: time: invalid duration \"\"\n",
		},
		{
			name: "schema apply, a number with no unit",
			args: func(fx flagValueFixture) []string {
				return []string{"schema", "apply", "--url", fx.targetDB, "--to", fx.schema, "--dev-url", fx.devURL,
					"--auto-approve", "--lock-timeout", "1"}
			},
			wantStderr: "Error: invalid argument \"1\" for \"--lock-timeout\" flag: time: missing unit in duration \"1\"\n",
		},
		{
			name: "schema apply, ahead of a missing --url",
			args: func(fx flagValueFixture) []string {
				return []string{"schema", "apply", "--to", fx.schema, "--lock-timeout", "bogus"}
			},
			wantStderr: "Error: invalid argument \"bogus\" for \"--lock-timeout\" flag: time: invalid duration \"bogus\"\n",
		},
		{
			name: "migrate apply, an empty value",
			args: func(fx flagValueFixture) []string {
				return []string{"migrate", "apply", "--url", fx.targetDB, "--dir", fx.dir, "--lock-timeout", ""}
			},
			wantStderr: "Error: invalid argument \"\" for \"--lock-timeout\" flag: time: invalid duration \"\"\n",
		},
		{
			name: "migrate apply, ahead of a missing --url",
			args: func(fx flagValueFixture) []string {
				return []string{"migrate", "apply", "--dir", fx.dir, "--lock-timeout", "bogus"}
			},
			wantStderr: "Error: invalid argument \"bogus\" for \"--lock-timeout\" flag: time: invalid duration \"bogus\"\n",
		},
		{
			name: "migrate diff, an empty value",
			args: func(fx flagValueFixture) []string {
				return []string{"migrate", "diff", "next", "--dir", fx.dir, "--to", fx.schema, "--dev-url", fx.devURL,
					"--lock-timeout", ""}
			},
			wantStderr: "Error: invalid argument \"\" for \"--lock-timeout\" flag: time: invalid duration \"\"\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newFlagValueFixture(c)

			stdout, stderr, err := atlastest.RunCompat(tt.args(fx)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr, qt.Equals, tt.wantStderr)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestCompatLockTimeout_HappyPath: zero and negative values are accepted, as
// on the pinned binary, where they mean "do not wait". No other run holds a
// lock here, so each run completes.
func TestCompatLockTimeout_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		args       func(fx flagValueFixture) []string
		wantStdout string
	}{
		{
			name: "schema apply, zero",
			args: func(fx flagValueFixture) []string {
				return []string{"schema", "apply", "--url", fx.targetDB, "--to", fx.schema, "--dev-url", fx.devURL,
					"--auto-approve", "--lock-timeout", "0"}
			},
			wantStdout: "Schema apply completed successfully.",
		},
		{
			name: "schema apply, a negative duration",
			args: func(fx flagValueFixture) []string {
				return []string{"schema", "apply", "--url", fx.targetDB, "--to", fx.schema, "--dev-url", fx.devURL,
					"--auto-approve", "--lock-timeout", "-1s"}
			},
			wantStdout: "Schema apply completed successfully.",
		},
		{
			name: "migrate apply, zero",
			args: func(fx flagValueFixture) []string {
				return []string{"migrate", "apply", "--url", fx.targetDB, "--dir", fx.dir, "--lock-timeout", "0"}
			},
			wantStdout: "Migration complete.",
		},
		{
			name: "migrate apply, a negative duration",
			args: func(fx flagValueFixture) []string {
				return []string{"migrate", "apply", "--url", fx.targetDB, "--dir", fx.dir, "--lock-timeout", "-1s"}
			},
			wantStdout: "Migration complete.",
		},
		{
			name: "migrate diff, zero",
			args: func(fx flagValueFixture) []string {
				return []string{"migrate", "diff", "next", "--dir", fx.dir, "--to", fx.schema, "--dev-url", fx.devURL,
					"--lock-timeout", "0"}
			},
			wantStdout: "The migration directory is synced with the desired state",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newFlagValueFixture(c)

			stdout, stderr, err := atlastest.RunCompat(tt.args(fx)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr=%q", stderr))
			c.Assert(stdout, qt.Contains, tt.wantStdout)
		})
	}
}

// TestCompatLockTimeoutFromProject_FailurePath reads atlas.hcl
// `migration { lock_timeout }` as the pinned binary does: it sets the flag from
// the attribute, so a value that is not a duration is refused in the flag
// parser's words. The happy path below accepts zero and a negative value, and
// an empty value leaves the default.
func TestCompatLockTimeoutFromProject_FailurePath(t *testing.T) {
	c := qt.New(t)
	fx := newFlagValueFixture(c)
	config := writeLockTimeoutProject(c, fx, "bogus")

	stdout, stderr, err := atlastest.RunCompat("migrate", "apply", "--config", config, "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Equals, "Error: invalid argument \"bogus\" for \"--lock-timeout\" flag: time: invalid duration \"bogus\"\n")
	c.Assert(stdout, qt.Equals, "")
}

func TestCompatLockTimeoutFromProject_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "zero", value: "0"},
		{name: "a negative duration", value: "-1s"},
		{name: "an empty value", value: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newFlagValueFixture(c)
			config := writeLockTimeoutProject(c, fx, tt.value)

			stdout, stderr, err := atlastest.RunCompat("migrate", "apply", "--config", config, "--env", "local")

			c.Assert(err, qt.IsNil, qt.Commentf("stderr=%q", stderr))
			c.Assert(stdout, qt.Contains, "Migration complete.")
		})
	}
}

// writeLockTimeoutProject writes an atlas.hcl whose `local` env applies the
// fixture's directory to its target with the given lock_timeout.
func writeLockTimeoutProject(c *qt.C, fx flagValueFixture, lockTimeout string) string {
	c.Helper()
	path := filepath.Join(fx.root, "atlas.hcl")
	body := "env \"local\" {\n" +
		"  url = \"" + filepath.ToSlash(fx.targetDB) + "\"\n" +
		"  migration {\n" +
		"    dir = \"" + filepath.ToSlash(fx.dir) + "\"\n" +
		"    lock_timeout = \"" + lockTimeout + "\"\n" +
		"  }\n}\n"
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return "file://" + filepath.ToSlash(path)
}

// schemaApplyTxMode and migrateApplyTxMode build a run of each verb with a
// --tx-mode value, against the fixture's target or its empty database.
func schemaApplyTxMode(url string, fx flagValueFixture, value string, extra ...string) []string {
	return append([]string{"schema", "apply", "--url", url, "--to", fx.schema, "--dev-url", fx.devURL,
		"--auto-approve", "--tx-mode", value}, extra...)
}

func migrateApplyTxMode(url string, fx flagValueFixture, value string, extra ...string) []string {
	return append([]string{"migrate", "apply", "--url", url, "--dir", fx.dir, "--tx-mode", value}, extra...)
}

// TestCompatTxMode_FailurePath: the pinned binary compares --tx-mode as
// written, against `none` and `file` on `schema apply` and against those and
// `all` on `migrate apply`. `migrate apply --dry-run` is refused here too,
// deliberately: that binary's dry run accepts the value and its real run
// refuses it. Nothing is opened for a refused value, so the empty database is
// never created.
func TestCompatTxMode_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		args       func(fx flagValueFixture) []string
		wantStderr string
	}{
		{
			name:       "schema apply, all",
			args:       func(fx flagValueFixture) []string { return schemaApplyTxMode(fx.emptyDB, fx, "all") },
			wantStderr: "Error: unknown tx-mode \"all\"\n",
		},
		{
			name:       "schema apply, upper case",
			args:       func(fx flagValueFixture) []string { return schemaApplyTxMode(fx.emptyDB, fx, "File") },
			wantStderr: "Error: unknown tx-mode \"File\"\n",
		},
		{
			name:       "schema apply, empty",
			args:       func(fx flagValueFixture) []string { return schemaApplyTxMode(fx.emptyDB, fx, "") },
			wantStderr: "Error: unknown tx-mode \"\"\n",
		},
		{
			name:       "schema apply, a word",
			args:       func(fx flagValueFixture) []string { return schemaApplyTxMode(fx.emptyDB, fx, "bogus") },
			wantStderr: "Error: unknown tx-mode \"bogus\"\n",
		},
		{
			name:       "migrate apply, upper case",
			args:       func(fx flagValueFixture) []string { return migrateApplyTxMode(fx.emptyDB, fx, "ALL") },
			wantStderr: "Error: unknown tx-mode \"ALL\"\n",
		},
		{
			name:       "migrate apply, empty",
			args:       func(fx flagValueFixture) []string { return migrateApplyTxMode(fx.emptyDB, fx, "") },
			wantStderr: "Error: unknown tx-mode \"\"\n",
		},
		{
			name:       "migrate apply, a word",
			args:       func(fx flagValueFixture) []string { return migrateApplyTxMode(fx.emptyDB, fx, "bogus") },
			wantStderr: "Error: unknown tx-mode \"bogus\"\n",
		},
		{
			name: "migrate apply --dry-run, a word",
			args: func(fx flagValueFixture) []string {
				return migrateApplyTxMode(fx.emptyDB, fx, "bogus", "--dry-run")
			},
			wantStderr: "Error: unknown tx-mode \"bogus\"\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newFlagValueFixture(c)

			stdout, stderr, err := atlastest.RunCompat(tt.args(fx)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr, qt.Equals, tt.wantStderr)
			c.Assert(stdout, qt.Equals, "")
			_, statErr := os.Stat(fx.emptyPath)
			c.Assert(os.IsNotExist(statErr), qt.IsTrue)
		})
	}
}

// TestCompatTxMode_HappyPath: the values the pinned binary accepts, on the verb
// that accepts them, each creating the table in the target.
func TestCompatTxMode_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args func(fx flagValueFixture) []string
	}{
		{name: "schema apply, file", args: func(fx flagValueFixture) []string { return schemaApplyTxMode(fx.targetDB, fx, "file") }},
		{name: "schema apply, none", args: func(fx flagValueFixture) []string { return schemaApplyTxMode(fx.targetDB, fx, "none") }},
		{name: "migrate apply, file", args: func(fx flagValueFixture) []string { return migrateApplyTxMode(fx.targetDB, fx, "file") }},
		{name: "migrate apply, all", args: func(fx flagValueFixture) []string { return migrateApplyTxMode(fx.targetDB, fx, "all") }},
		{name: "migrate apply, none", args: func(fx flagValueFixture) []string { return migrateApplyTxMode(fx.targetDB, fx, "none") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newFlagValueFixture(c)

			stdout, stderr, err := atlastest.RunCompat(tt.args(fx)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
			c.Assert(atlastest.SqliteTableCount(c, fx.target, "widgets"), qt.Equals, 1)
		})
	}
}

// formatVerbArgs builds each verb with a --format template, against a target
// that already holds the fixture's table.
func formatVerbArgs(fx flagValueFixture, verb, format string) []string {
	return map[string][]string{
		"schema inspect": {"schema", "inspect", "--url", fx.targetDB, "--format", format},
		"schema diff":    {"schema", "diff", "--from", fx.targetDB, "--to", fx.targetDB, "--format", format},
		"schema apply": {"schema", "apply", "--url", fx.targetDB, "--to", fx.schema, "--dev-url", fx.devURL,
			"--dry-run", "--format", format},
		"migrate lint":   {"migrate", "lint", "--dir", fx.dir, "--latest", "1", "--dev-url", fx.devURL, "--format", format},
		"migrate status": {"migrate", "status", "--dir", fx.dir, "--url", fx.targetDB, "--format", format},
		"migrate apply":  {"migrate", "apply", "--dir", fx.dir, "--url", fx.emptyDB, "--format", format},
		"migrate diff": {"migrate", "diff", "next", "--dir", fx.dir, "--to", fx.schema, "--dev-url", fx.devURL,
			"--format", format},
	}[verb]
}

// TestCompatFormatTemplateParse_FailurePath: the pinned binary parses every
// --format template under the name "format", and refuses one that does not
// parse as `parse log format: …` on the schema verbs and `parse format: …` on
// the migrate verbs.
func TestCompatFormatTemplateParse_FailurePath(t *testing.T) {
	tests := []struct {
		verb       string
		wantStderr string
	}{
		{verb: "schema inspect", wantStderr: "Error: parse log format: template: format:1: function \"bogus\" not defined\n"},
		{verb: "schema diff", wantStderr: "Error: parse log format: template: format:1: function \"bogus\" not defined\n"},
		{verb: "schema apply", wantStderr: "Error: parse log format: template: format:1: function \"bogus\" not defined\n"},
		{verb: "migrate lint", wantStderr: "Error: parse format: template: format:1: function \"bogus\" not defined\n"},
		{verb: "migrate status", wantStderr: "Error: parse format: template: format:1: function \"bogus\" not defined\n"},
		{verb: "migrate apply", wantStderr: "Error: parse format: template: format:1: function \"bogus\" not defined\n"},
		{verb: "migrate diff", wantStderr: "Error: parse format: template: format:1: function \"bogus\" not defined\n"},
	}

	for _, tt := range tests {
		t.Run(tt.verb, func(t *testing.T) {
			c := qt.New(t)
			fx := newFlagValueFixture(c)
			atlastest.SeedSQLiteDBAt(t, fx.target, "CREATE TABLE widgets (id INTEGER PRIMARY KEY);")

			stdout, stderr, err := atlastest.RunCompat(formatVerbArgs(fx, tt.verb, "{{ bogus")...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr, qt.Equals, tt.wantStderr)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestCompatFormatTemplateExecute_FailurePath: a template that parses and fails
// while it runs is the template's own error on the pinned binary, under the
// same name, with `execute log template: ` in front on `migrate apply` alone.
// The Go type it names at the end is each binary's own report type, so only
// the part before it is pinned.
func TestCompatFormatTemplateExecute_FailurePath(t *testing.T) {
	tests := []struct {
		verb    string
		wantErr string
	}{
		{verb: "schema diff", wantErr: `template: format:1:3: executing "format" at <\.Nope>: can't evaluate field Nope in type .*`},
		{verb: "schema apply", wantErr: `template: format:1:3: executing "format" at <\.Nope>: can't evaluate field Nope in type .*`},
		{verb: "migrate status", wantErr: `template: format:1:3: executing "format" at <\.Nope>: can't evaluate field Nope in type .*`},
		{
			verb:    "migrate apply",
			wantErr: `execute log template: template: format:1:3: executing "format" at <\.Nope>: can't evaluate field Nope in type .*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.verb, func(t *testing.T) {
			c := qt.New(t)
			fx := newFlagValueFixture(c)
			atlastest.SeedSQLiteDBAt(t, fx.target, "CREATE TABLE widgets (id INTEGER PRIMARY KEY);")

			stdout, stderr, err := atlastest.RunCompat(formatVerbArgs(fx, tt.verb, "{{ .Nope }}")...)

			c.Assert(err, qt.ErrorMatches, tt.wantErr, qt.Commentf("stderr=%q", stderr))
			c.Assert(stdout, qt.Equals, "")
		})
	}
}
