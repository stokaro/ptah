package atlas_test

// CLI coverage for the named-lock family (`--lock-name`, `--skip-lock`) added
// for stokaro/ptah#951.
//
// Registration is measured by RUNNING each spelling and reading the error, not
// by grepping `--help`: on several verbs `--help` short-circuits before flag
// validation, and the string `--foo` appears inside `unknown flag: --foo`, so a
// help grep answers "registered" in both directions. Every row here carries
// both controls — a flag the verb certainly has, and a nonsense flag.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

const (
	// The note `migrate apply` prints when --lock-name is passed to a dialect
	// with no advisory locks. Naming the lock in it is what makes the flag's
	// arrival at the lock machinery observable from the command line.
	migrateApplyNamedLockNote = `note: migration locking is not supported for dialect "sqlite"; the advisory lock "atlas_migrate_execute" is not acquired and the migrations run without a database lock`
	schemaApplyNamedLockNote  = `note: schema apply locking is not supported for dialect "sqlite"; the advisory lock "atlas_migrate_execute" is not acquired and the apply proceeds without a database lock`
)

// TestCompatLockFlagRegistration measures which verbs register which member of
// the family. The four PRESENT rows are the flags Atlas's published CLI
// reference registers; the six MISSING rows are the spellings it does not, on
// verbs that carry only --lock-timeout from this family.
func TestCompatLockFlagRegistration(t *testing.T) {
	tests := []struct {
		name string
		path []string
		// positive is a flag the verb certainly registers.
		positive []string
		flag     []string
		want     bool
	}{
		{name: "migrate_apply_lock_name", path: []string{"migrate", "apply"}, positive: []string{"--dry-run"}, flag: []string{"--lock-name", "probe"}, want: true},
		{name: "migrate_apply_skip_lock", path: []string{"migrate", "apply"}, positive: []string{"--dry-run"}, flag: []string{"--skip-lock"}, want: true},
		{name: "schema_apply_lock_name", path: []string{"schema", "apply"}, positive: []string{"--dry-run"}, flag: []string{"--lock-name", "probe"}, want: true},
		{name: "schema_apply_skip_lock", path: []string{"schema", "apply"}, positive: []string{"--dry-run"}, flag: []string{"--skip-lock"}, want: true},

		{name: "migrate_diff_lock_name", path: []string{"migrate", "diff"}, positive: []string{"--lock-timeout", "1s"}, flag: []string{"--lock-name", "probe"}, want: false},
		{name: "migrate_diff_skip_lock", path: []string{"migrate", "diff"}, positive: []string{"--lock-timeout", "1s"}, flag: []string{"--skip-lock"}, want: false},
		{name: "migrate_down_lock_name", path: []string{"migrate", "down"}, positive: []string{"--lock-timeout", "1s"}, flag: []string{"--lock-name", "probe"}, want: false},
		{name: "migrate_down_skip_lock", path: []string{"migrate", "down"}, positive: []string{"--lock-timeout", "1s"}, flag: []string{"--skip-lock"}, want: false},
		{name: "migrate_checkpoint_lock_name", path: []string{"migrate", "checkpoint"}, positive: []string{"--dir-format", "atlas"}, flag: []string{"--lock-name", "probe"}, want: false},
		{name: "migrate_checkpoint_skip_lock", path: []string{"migrate", "checkpoint"}, positive: []string{"--dir-format", "atlas"}, flag: []string{"--skip-lock"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(compatFlagRegistered(tt.path, tt.positive), qt.IsTrue,
				qt.Commentf("positive control %v must be registered on %v", tt.positive, tt.path))
			c.Assert(compatFlagRegistered(tt.path, []string{"--frobnicate-nonsense"}), qt.IsFalse,
				qt.Commentf("negative control must be unregistered on %v", tt.path))

			c.Assert(compatFlagRegistered(tt.path, tt.flag), qt.Equals, tt.want)
		})
	}
}

// TestCompatLockFlagContradictionsRefused pins that the two inputs with no
// sound interpretation are refused rather than silently resolved: a blank name
// would fall back to the default lock, and naming a lock that --skip-lock does
// not take would drop one of the two flags.
func TestCompatLockFlagContradictionsRefused(t *testing.T) {
	tests := []struct {
		name string
		// verb builds the full argument list for one verb inside dir, so each
		// row supplies the source flags its own verb requires.
		verb    func(dir string) []string
		args    []string
		wantErr string
	}{
		{
			name:    "migrate_apply_blank_name",
			verb:    migrateApplyLockArgs,
			args:    []string{"--lock-name", "  "},
			wantErr: `--lock-name must not be empty`,
		},
		{
			name:    "schema_apply_blank_name",
			verb:    schemaApplyLockArgs,
			args:    []string{"--lock-name", ""},
			wantErr: `--lock-name must not be empty`,
		},
		{
			name:    "migrate_apply_name_with_skip",
			verb:    migrateApplyLockArgs,
			args:    []string{"--lock-name", "x", "--skip-lock"},
			wantErr: `--lock-name and --skip-lock cannot be used together: --skip-lock takes no lock, so there is no lock to name`,
		},
		{
			name:    "schema_apply_name_with_skip",
			verb:    schemaApplyLockArgs,
			args:    []string{"--lock-name", "x", "--skip-lock"},
			wantErr: `--lock-name and --skip-lock cannot be used together: --skip-lock takes no lock, so there is no lock to name`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()

			_, err := runCompatLockCommand(append(tt.verb(dir), tt.args...))

			// The contradiction is refused before the migration directory or
			// desired schema is read, so neither has to exist.
			c.Assert(err, qt.ErrorMatches, regexpQuote(tt.wantErr))
		})
	}
}

func migrateApplyLockArgs(dir string) []string {
	return []string{
		"migrate", "apply",
		"--url", "sqlite://" + filepath.Join(dir, "lock.db"),
		"--dir", "file://" + filepath.Join(dir, "migrations"),
	}
}

func schemaApplyLockArgs(dir string) []string {
	return []string{
		"schema", "apply",
		"--url", "sqlite://" + filepath.Join(dir, "lock.db"),
		"--to", "file://" + filepath.Join(dir, "schema.sql"),
	}
}

// TestMigrateApplyNamedLockReachesMigrator drives the flag end to end: the note
// is printed from the prepared migrator's own lock name, so it can only read
// "atlas_migrate_execute" if the flag reached the lock machinery. --skip-lock
// takes no lock at all and therefore has no dialect capability to report.
func TestMigrateApplyNamedLockReachesMigrator(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantNote bool
	}{
		{name: "named lock", args: []string{"--lock-name", "atlas_migrate_execute"}, wantNote: true},
		{name: "skip lock", args: []string{"--skip-lock"}, wantNote: false},
		{name: "default", args: nil, wantNote: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "lock-apply.db")
			migrationsDir := filepath.Join(dir, "migrations")
			writeAtlasApplyProjectMigration(c, migrationsDir, "20260101000001_init.sql",
				"CREATE TABLE lock_flag_users (id INTEGER PRIMARY KEY);\n")
			writeAtlasApplyProjectSum(c, migrationsDir)

			args := append([]string{
				"migrate", "apply",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + migrationsDir,
			}, tt.args...)

			out, err := runCompatLockCommand(args)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Migration complete. Current version: 20260101000001")
			c.Assert(strings.Contains(out, migrateApplyNamedLockNote), qt.Equals, tt.wantNote,
				qt.Commentf("output:\n%s", out))
			c.Assert(sqliteTableCount(c, dbPath, "lock_flag_users"), qt.Equals, 1)
		})
	}
}

// TestSchemaApplyNamedLockReachesLockMachinery is the same proof on the
// declarative verb: the note names the lock read back from the ACQUIRED lock
// object, and --skip-lock acquires nothing so there is no lock to name.
func TestSchemaApplyNamedLockReachesLockMachinery(t *testing.T) {
	allowSchemaApplyWithoutDevURL(t)
	tests := []struct {
		name     string
		args     []string
		wantNote bool
	}{
		{name: "named lock", args: []string{"--lock-name", "atlas_migrate_execute"}, wantNote: true},
		{name: "skip lock", args: []string{"--skip-lock"}, wantNote: false},
		{name: "skip lock silences the timeout note", args: []string{"--skip-lock", "--lock-timeout", "10s"}, wantNote: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "lock-schema.db")
			schemaPath := filepath.Join(dir, "schema.sql")
			c.Assert(os.WriteFile(schemaPath,
				[]byte(`CREATE TABLE lock_flag_schema (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

			args := append([]string{
				"schema", "apply",
				"--url", "sqlite://" + dbPath,
				"--to", "file://" + schemaPath,
				"--auto-approve",
			}, tt.args...)

			out, err := runCompatLockCommand(args)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Schema apply completed successfully.")
			c.Assert(strings.Contains(out, schemaApplyNamedLockNote), qt.Equals, tt.wantNote,
				qt.Commentf("output:\n%s", out))
			// The --lock-timeout wording never appears alongside --skip-lock:
			// a run that takes no lock has no timeout to ignore.
			c.Assert(out, qt.Not(qt.Contains), schemaApplyLockUnsupportedNote)
			c.Assert(sqliteTableCount(c, dbPath, "lock_flag_schema"), qt.Equals, 1)
		})
	}
}

// compatFlagRegistered runs the spelling and reports whether the command
// recognized it. It never inspects help text.
func compatFlagRegistered(path, flag []string) bool {
	_, err := runCompatLockCommand(append(append([]string(nil), path...), flag...))
	if err == nil {
		return true
	}
	return !strings.Contains(err.Error(), "unknown flag: "+flag[0])
}

func runCompatLockCommand(args []string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// regexpQuote escapes a literal error message for qt.ErrorMatches, which
// anchors and compiles its argument as a regular expression.
func regexpQuote(literal string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`, `.`, `\.`, `+`, `\+`, `*`, `\*`, `?`, `\?`,
		`(`, `\(`, `)`, `\)`, `[`, `\[`, `]`, `\]`, `{`, `\{`, `}`, `\}`,
		`^`, `\^`, `$`, `\$`, `|`, `\|`,
	)
	return replacer.Replace(literal)
}
