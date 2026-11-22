//go:build integration

package integration_test

import (
	"cmp"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
)

// The tests in this file drive the variables a writing command reads to
// narrow what it changes. Read as unset, an empty one would widen the run to
// what leaving its flag out selects, so it is refused before anything is
// written. Each variable has rows where it is unset and the default applies,
// where a value is applied, and where an empty or blank value is refused. A
// typed flag never reads its variable, so a typed value beside an empty
// variable is the control that the refusal is about the variable alone.

const (
	// writeVariableInitialSchema is the database each schema test starts from.
	writeVariableInitialSchema = "CREATE TABLE keep (id INTEGER PRIMARY KEY);\nCREATE TABLE extra (id INTEGER PRIMARY KEY);\n"
	// writeVariableDesiredSchema keeps keep, adds a column to it and leaves
	// extra out, so a plan over every object drops extra.
	writeVariableDesiredSchema = "CREATE TABLE keep (id INTEGER PRIMARY KEY, note TEXT);\n"
)

// keepAndExtraState is what remains of the tables keep and extra, and the
// columns keep has.
type keepAndExtraState struct {
	Tables  []string
	Columns []string
}

var (
	// keepAndExtraUntouched is the initial schema: nothing was applied.
	keepAndExtraUntouched = keepAndExtraState{Tables: []string{"extra", "keep"}, Columns: []string{"id"}}
	// keepAndExtraNarrowed is the desired schema applied with extra left alone.
	keepAndExtraNarrowed = keepAndExtraState{Tables: []string{"extra", "keep"}, Columns: []string{"id", "note"}}
	// keepAndExtraWhole is the desired schema applied to every object.
	keepAndExtraWhole = keepAndExtraState{Tables: []string{"keep"}, Columns: []string{"id", "note"}}
)

// withVariables is an environment that carries no PTAH_ variable but the
// ones given.
func withVariables(variables ...string) []string {
	return append(environmentWithoutPtahVariables(), variables...)
}

// keepAndExtra writes initial.sql and desired.sql to a new directory, applies
// initial.sql to a SQLite database beside them, and returns the directory and
// the database URL.
func keepAndExtra(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir = c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "initial.sql"), []byte(writeVariableInitialSchema), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "desired.sql"), []byte(writeVariableDesiredSchema), 0o600), qt.IsNil)
	targetURL = atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db"))
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables()},
		"schema", "apply", "--db-url", targetURL, "--schema-file", "initial.sql", "--auto-approve")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(readKeepAndExtra(c, targetURL), qt.DeepEquals, keepAndExtraUntouched)
	return dir, targetURL
}

// readKeepAndExtra reads the state of keep and extra back from the database.
func readKeepAndExtra(c *qt.C, targetURL string) keepAndExtraState {
	c.Helper()
	return keepAndExtraState{
		Tables: limitHistoryColumn(c, targetURL,
			"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('keep', 'extra') ORDER BY name"),
		Columns: limitHistoryColumn(c, targetURL, "SELECT name FROM pragma_table_info('keep') ORDER BY cid"),
	}
}

// substitute replaces each "{url}" in args with targetURL.
func substitute(args []string, targetURL string) []string {
	out := make([]string, len(args))
	for i, arg := range args {
		out[i] = strings.ReplaceAll(arg, "{url}", targetURL)
	}
	return out
}

func TestSchemaWriteVariablesE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		program clirun.Target
		env     []string
		args    []string
		want    keepAndExtraState
	}{
		{
			name: "unset, the apply changes every object",
			args: []string{"schema", "apply", "--db-url", "{url}", "--schema-file", "desired.sql", "--auto-approve"},
			want: keepAndExtraWhole,
		},
		{
			name: "PTAH_EXCLUDE keeps the table it names",
			env:  []string{"PTAH_EXCLUDE=extra"},
			args: []string{"schema", "apply", "--db-url", "{url}", "--schema-file", "desired.sql", "--auto-approve"},
			want: keepAndExtraNarrowed,
		},
		{
			name: "PTAH_INCLUDE leaves out the table it does not name",
			env:  []string{"PTAH_INCLUDE=keep"},
			args: []string{"schema", "apply", "--db-url", "{url}", "--schema-file", "desired.sql", "--auto-approve"},
			want: keepAndExtraNarrowed,
		},
		{
			name: "PTAH_PROTECTED_TABLE fences declared rows, which a SQL schema has none of",
			env:  []string{"PTAH_PROTECTED_TABLE=keep"},
			args: []string{"schema", "apply", "--db-url", "{url}", "--schema-file", "desired.sql", "--auto-approve"},
			want: keepAndExtraWhole,
		},
		{
			name: "a typed --exclude over an empty PTAH_EXCLUDE",
			env:  []string{"PTAH_EXCLUDE="},
			args: []string{"schema", "apply", "--db-url", "{url}", "--schema-file", "desired.sql", "--auto-approve", "--exclude", "extra"},
			want: keepAndExtraNarrowed,
		},
		{
			name: "a typed empty --exclude over an empty PTAH_EXCLUDE",
			env:  []string{"PTAH_EXCLUDE="},
			args: []string{"schema", "apply", "--db-url", "{url}", "--schema-file", "desired.sql", "--auto-approve", "--exclude", ""},
			want: keepAndExtraWhole,
		},
		{
			name: "a typed --include over an empty PTAH_INCLUDE",
			env:  []string{"PTAH_INCLUDE="},
			args: []string{"schema", "apply", "--db-url", "{url}", "--schema-file", "desired.sql", "--auto-approve", "--include", "keep"},
			want: keepAndExtraNarrowed,
		},
		{
			name:    "compat schema apply, unset",
			program: clirun.Compat,
			args: []string{"schema", "apply", "--url", "{url}", "--to", "file://desired.sql",
				"--dev-url", "sqlite://dev?mode=memory", "--auto-approve"},
			want: keepAndExtraWhole,
		},
		{
			name:    "compat schema apply, PTAH_EXCLUDE",
			program: clirun.Compat,
			env:     []string{"PTAH_EXCLUDE=extra"},
			args: []string{"schema", "apply", "--url", "{url}", "--to", "file://desired.sql",
				"--dev-url", "sqlite://dev?mode=memory", "--auto-approve"},
			want: keepAndExtraNarrowed,
		},
		{
			name:    "compat schema apply, PTAH_INCLUDE",
			program: clirun.Compat,
			env:     []string{"PTAH_INCLUDE=keep"},
			args: []string{"schema", "apply", "--url", "{url}", "--to", "file://desired.sql",
				"--dev-url", "sqlite://dev?mode=memory", "--auto-approve"},
			want: keepAndExtraNarrowed,
		},
		{
			name:    "compat schema clean, unset",
			program: clirun.Compat,
			args:    []string{"schema", "clean", "--url", "{url}", "--auto-approve"},
			want:    keepAndExtraState{Tables: make([]string, 0), Columns: make([]string, 0)},
		},
		{
			name:    "compat schema clean, PTAH_EXCLUDE",
			program: clirun.Compat,
			env:     []string{"PTAH_EXCLUDE=extra"},
			args:    []string{"schema", "clean", "--url", "{url}", "--auto-approve"},
			want:    keepAndExtraState{Tables: []string{"extra"}, Columns: make([]string, 0)},
		},
		{
			name:    "compat schema clean, PTAH_INCLUDE",
			program: clirun.Compat,
			env:     []string{"PTAH_INCLUDE=keep"},
			args:    []string{"schema", "clean", "--url", "{url}", "--auto-approve"},
			want:    keepAndExtraState{Tables: []string{"extra"}, Columns: make([]string, 0)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := keepAndExtra(c)

			got := clirun.Run(c, cmp.Or(tc.program, clirun.Ptah), clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				substitute(tc.args, targetURL)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(readKeepAndExtra(c, targetURL), qt.DeepEquals, tc.want)
		})
	}
}

func TestSchemaWriteVariablesE2E_FailurePath(t *testing.T) {
	t.Parallel()
	nativeApply := []string{"schema", "apply", "--db-url", "{url}", "--schema-file", "desired.sql", "--auto-approve"}
	compatApply := []string{"schema", "apply", "--url", "{url}", "--to", "file://desired.sql",
		"--dev-url", "sqlite://dev?mode=memory", "--auto-approve"}
	compatClean := []string{"schema", "clean", "--url", "{url}", "--auto-approve"}
	tests := []struct {
		name       string
		program    clirun.Target
		env        []string
		args       []string
		wantExit   int
		wantStderr string
	}{
		{
			name:       "empty PTAH_EXCLUDE",
			env:        []string{"PTAH_EXCLUDE="},
			args:       nativeApply,
			wantExit:   2,
			wantStderr: "error: PTAH_EXCLUDE is set but empty: give it a value, or unset it and leave --exclude out to exclude nothing\n",
		},
		{
			name:     "PTAH_EXCLUDE of blank fields",
			env:      []string{"PTAH_EXCLUDE= , "},
			args:     nativeApply,
			wantExit: 2,
			wantStderr: `error: PTAH_EXCLUDE is set to " , ", which names nothing: give it a value, or unset it and leave ` +
				"--exclude out to exclude nothing\n",
		},
		{
			name:       "empty PTAH_INCLUDE",
			env:        []string{"PTAH_INCLUDE="},
			args:       nativeApply,
			wantExit:   2,
			wantStderr: "error: PTAH_INCLUDE is set but empty: give it a value, or unset it and leave --include out to include every object\n",
		},
		{
			name:     "blank PTAH_INCLUDE",
			env:      []string{"PTAH_INCLUDE= "},
			args:     nativeApply,
			wantExit: 2,
			wantStderr: `error: PTAH_INCLUDE is set to " ", which names nothing: give it a value, or unset it and leave ` +
				"--include out to include every object\n",
		},
		{
			name:     "empty PTAH_PROTECTED_TABLE",
			env:      []string{"PTAH_PROTECTED_TABLE="},
			args:     nativeApply,
			wantExit: 2,
			wantStderr: "error: PTAH_PROTECTED_TABLE is set but empty: give it a value, or unset it and leave " +
				"--protected-table out to protect no table\n",
		},
		{
			name:       "compat schema apply, empty PTAH_EXCLUDE",
			program:    clirun.Compat,
			env:        []string{"PTAH_EXCLUDE="},
			args:       compatApply,
			wantExit:   1,
			wantStderr: "Error: PTAH_EXCLUDE is set but empty: give it a value, or unset it and leave --exclude out to exclude nothing\n",
		},
		{
			name:       "compat schema apply, empty PTAH_INCLUDE",
			program:    clirun.Compat,
			env:        []string{"PTAH_INCLUDE="},
			args:       compatApply,
			wantExit:   1,
			wantStderr: "Error: PTAH_INCLUDE is set but empty: give it a value, or unset it and leave --include out to include every object\n",
		},
		{
			name:       "compat schema clean, empty PTAH_EXCLUDE",
			program:    clirun.Compat,
			env:        []string{"PTAH_EXCLUDE="},
			args:       compatClean,
			wantExit:   1,
			wantStderr: "Error: PTAH_EXCLUDE is set but empty: give it a value, or unset it and leave --exclude out to exclude nothing\n",
		},
		{
			name:       "compat schema clean, empty PTAH_INCLUDE",
			program:    clirun.Compat,
			env:        []string{"PTAH_INCLUDE="},
			args:       compatClean,
			wantExit:   1,
			wantStderr: "Error: PTAH_INCLUDE is set but empty: give it a value, or unset it and leave --include out to include every object\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := keepAndExtra(c)

			got := clirun.Run(c, cmp.Or(tc.program, clirun.Ptah), clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				substitute(tc.args, targetURL)...)

			c.Assert(got.ExitCode, qt.Equals, tc.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, tc.wantStderr)
			c.Assert(readKeepAndExtra(c, targetURL), qt.DeepEquals, keepAndExtraUntouched)
		})
	}
}

// generatedUpStatements runs `ptah migrations generate` from desired.sql into
// the directory out, and returns the run and the statements of the up file it
// wrote, without comments and blank lines.
func generatedUpStatements(c *qt.C, dir, targetURL string, env []string) (clirun.Result, []string) {
	c.Helper()
	out := filepath.Join(dir, "out")
	c.Assert(os.Mkdir(out, 0o750), qt.IsNil)
	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(env...)},
		"migrations", "generate", "--db-url", targetURL, "--schema-file", "desired.sql", "--migrations-dir", "out")
	ups, err := filepath.Glob(filepath.Join(out, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	statements := make([]string, 0)
	for _, up := range ups {
		body, err := os.ReadFile(up)
		c.Assert(err, qt.IsNil)
		for line := range strings.Lines(string(body)) {
			line = strings.TrimSpace(line)
			statements = append(statements, line)
		}
	}
	return got, slices.DeleteFunc(statements, func(line string) bool { return line == "" || strings.HasPrefix(line, "--") })
}

func TestMigrationsGenerateWriteVariablesE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  []string
		want []string
	}{
		{
			name: "unset, the migration changes every object",
			want: []string{`ALTER TABLE "keep" ADD COLUMN "note" TEXT;`, `DROP TABLE "extra";`},
		},
		{
			name: "PTAH_EXCLUDE keeps the table it names",
			env:  []string{"PTAH_EXCLUDE=extra"},
			want: []string{`ALTER TABLE "keep" ADD COLUMN "note" TEXT;`},
		},
		{
			name: "PTAH_INCLUDE leaves out the table it does not name",
			env:  []string{"PTAH_INCLUDE=keep"},
			want: []string{`ALTER TABLE "keep" ADD COLUMN "note" TEXT;`},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := keepAndExtra(c)

			got, statements := generatedUpStatements(c, dir, targetURL, tc.env)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(statements, qt.DeepEquals, tc.want)
		})
	}
}

func TestMigrationsGenerateWriteVariablesE2E_FailurePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		env        []string
		wantStderr string
	}{
		{
			name:       "empty PTAH_EXCLUDE",
			env:        []string{"PTAH_EXCLUDE="},
			wantStderr: "error: PTAH_EXCLUDE is set but empty: give it a value, or unset it and leave --exclude out to exclude nothing\n",
		},
		{
			name:       "empty PTAH_INCLUDE",
			env:        []string{"PTAH_INCLUDE="},
			wantStderr: "error: PTAH_INCLUDE is set but empty: give it a value, or unset it and leave --include out to include every object\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := keepAndExtra(c)

			got, statements := generatedUpStatements(c, dir, targetURL, tc.env)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, tc.wantStderr)
			c.Assert(statements, qt.DeepEquals, make([]string, 0))
		})
	}
}

// TestMigrationsDataWriteVariablesE2E_FailurePath refuses an empty
// PTAH_PROTECTED_TABLE before the command reads its sources, so the run
// writes no migration.
func TestMigrationsDataWriteVariablesE2E_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir, targetURL := keepAndExtra(c)
	c.Assert(os.Mkdir(filepath.Join(dir, "entities"), 0o750), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(dir, "out"), 0o750), qt.IsNil)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables("PTAH_PROTECTED_TABLE=")},
		"migrations", "data", "--db-url", targetURL, "--root-dir", "entities", "--migrations-dir", "out")

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Equals, "error: PTAH_PROTECTED_TABLE is set but empty: give it a value, or unset it and leave "+
		"--protected-table out to protect no table\n")
	written, err := os.ReadDir(filepath.Join(dir, "out"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

// seededKeep writes a dev seed that inserts one row into keep, beside the
// database [keepAndExtra] creates.
func seededKeep(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir, targetURL = keepAndExtra(c)
	c.Assert(os.Mkdir(filepath.Join(dir, "seeds"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "seeds", "001_one.dev.sql"), []byte("INSERT INTO keep (id) VALUES (1);\n"), 0o600),
		qt.IsNil)
	return dir, targetURL
}

// protectedSeedRun runs `ptah seed` over the dev seeds of [seededKeep].
func protectedSeedRun(c *qt.C, dir, targetURL string, env ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(env...)},
		"seed", "--db-url", targetURL, "--seeds-dir", "seeds", "--env", "dev")
}

func TestSeedProtectedTableE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  []string
	}{
		{name: "unset, no table is protected"},
		// The fence reads the table list SQLite has. Without the seed table
		// query of the SQLite descriptor it asks information_schema, which
		// SQLite does not have, and the run fails before it seeds.
		{name: "PTAH_PROTECTED_TABLE names a table the database does not hold", env: []string{"PTAH_PROTECTED_TABLE=audit"}},
		{name: "PTAH_PROTECTED_TABLE names a table of another schema", env: []string{"PTAH_PROTECTED_TABLE=other.extra"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := seededKeep(c)

			got := protectedSeedRun(c, dir, targetURL, tc.env...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(limitHistoryColumn(c, targetURL, "SELECT CAST(id AS TEXT) FROM keep"), qt.DeepEquals, []string{"1"})
		})
	}
}

func TestSeedProtectedTableE2E_FailurePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		env        []string
		wantStderr string
	}{
		{
			name: "empty PTAH_PROTECTED_TABLE",
			env:  []string{"PTAH_PROTECTED_TABLE="},
			wantStderr: "error: PTAH_PROTECTED_TABLE is set but empty: give it a value, or unset it and leave " +
				"--protected-table out to protect no table\n",
		},
		{
			name: "PTAH_PROTECTED_TABLE of blank fields",
			env:  []string{"PTAH_PROTECTED_TABLE= , "},
			wantStderr: `error: PTAH_PROTECTED_TABLE is set to " , ", which names nothing: give it a value, or unset it ` +
				"and leave --protected-table out to protect no table\n",
		},
		{
			name: "PTAH_PROTECTED_TABLE names a table the database holds",
			env:  []string{"PTAH_PROTECTED_TABLE=extra"},
			wantStderr: "error: error applying seeds: refusing to seed target database because protected tables exist: extra; " +
				"pass --allow-prod to override\n",
		},
		// The fence asks about each table in the schema it lists them from,
		// SQLite's main, so a qualified entry names the table too.
		{
			name: "PTAH_PROTECTED_TABLE names a table the database holds by its schema",
			env:  []string{"PTAH_PROTECTED_TABLE=Main.Extra"},
			wantStderr: "error: error applying seeds: refusing to seed target database because protected tables exist: Main.Extra; " +
				"pass --allow-prod to override\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := seededKeep(c)

			got := protectedSeedRun(c, dir, targetURL, tc.env...)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, tc.wantStderr)
			c.Assert(limitHistoryColumn(c, targetURL, "SELECT CAST(id AS TEXT) FROM keep"), qt.DeepEquals, make([]string, 0))
		})
	}
}

// nativeStopHistory writes a native directory whose versions 1, 2 and 3
// create a, b and c and whose down files drop them, and names a SQLite
// database beside it.
func nativeStopHistory(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir = c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	for prefix, table := range map[string]string{"0000000001_a": "a", "0000000002_b": "b", "0000000003_c": "c"} {
		c.Assert(os.WriteFile(filepath.Join(migrations, prefix+".up.sql"),
			[]byte("CREATE TABLE "+table+" (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(migrations, prefix+".down.sql"), []byte("DROP TABLE "+table+";\n"), 0o600), qt.IsNil)
	}
	return dir, atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db"))
}

// appliedNativeStopHistory is [nativeStopHistory] with every migration applied.
func appliedNativeStopHistory(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir, targetURL = nativeStopHistory(c)
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables()},
		"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
	return dir, targetURL
}

func TestMigrationsUpStopVariablesE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  []string
		args []string
		want []string
	}{
		{name: "unset, every pending migration is applied", want: []string{"a", "b", "c"}},
		{name: "PTAH_LIMIT counts the migrations", env: []string{"PTAH_LIMIT=1"}, want: []string{"a"}},
		{name: "PTAH_TO_VERSION names the version", env: []string{"PTAH_TO_VERSION=2"}, want: []string{"a", "b"}},
		{
			name: "a typed --limit over an empty PTAH_LIMIT",
			env:  []string{"PTAH_LIMIT="},
			args: []string{"--limit", "2"},
			want: []string{"a", "b"},
		},
		{
			name: "a typed --to-version over an empty PTAH_TO_VERSION",
			env:  []string{"PTAH_TO_VERSION="},
			args: []string{"--to-version", "1"},
			want: []string{"a"},
		},
		{name: "PTAH_LIMIT unset beside a typed --to-version", args: []string{"--to-version", "2"}, want: []string{"a", "b"}},
		{
			name: "an empty PTAH_LIMIT beside a typed --to-version",
			env:  []string{"PTAH_LIMIT="},
			args: []string{"--to-version", "2"},
			want: []string{"a", "b"},
		},
		{
			name: "a blank PTAH_TO_VERSION beside a typed --limit",
			env:  []string{"PTAH_TO_VERSION= "},
			args: []string{"--limit", "1"},
			want: []string{"a"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := nativeStopHistory(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				append([]string{"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations"}, tc.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, tc.want)
		})
	}
}

func TestMigrationsUpStopVariablesE2E_FailurePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		env        []string
		wantStderr string
	}{
		{
			name: "empty PTAH_LIMIT",
			env:  []string{"PTAH_LIMIT="},
			wantStderr: "error: PTAH_LIMIT is set but empty: give it a value, or unset it and leave --limit out to " +
				"apply every pending migration\n",
		},
		{
			name: "blank PTAH_LIMIT",
			env:  []string{"PTAH_LIMIT= "},
			wantStderr: `error: PTAH_LIMIT is set to " ", which names nothing: give it a value, or unset it and leave ` +
				"--limit out to apply every pending migration\n",
		},
		{
			name: "empty PTAH_TO_VERSION",
			env:  []string{"PTAH_TO_VERSION="},
			wantStderr: "error: PTAH_TO_VERSION is set but empty: give it a value, or unset it and leave --to-version out to " +
				"apply every pending migration\n",
		},
		{
			name: "empty PTAH_LIMIT beside PTAH_TO_VERSION",
			env:  []string{"PTAH_TO_VERSION=2", "PTAH_LIMIT="},
			wantStderr: "error: PTAH_LIMIT is set but empty: give it a value, or unset it and leave --limit out to " +
				"apply every pending migration\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := nativeStopHistory(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations")

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, tc.wantStderr)
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, make([]string, 0))
		})
	}
}

func TestMigrationsDownStopVariablesE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  []string
		args []string
		want []string
	}{
		{name: "PTAH_TARGET names the version", env: []string{"PTAH_TARGET=1"}, want: []string{"a"}},
		{name: "PTAH_TARGET of 0 rolls back every migration", env: []string{"PTAH_TARGET=0"}, want: make([]string, 0)},
		{name: "a typed --target of 0", args: []string{"--target", "0"}, want: make([]string, 0)},
		{name: "PTAH_TARGET unset beside a typed --limit", args: []string{"--limit", "1"}, want: []string{"a", "b"}},
		{name: "PTAH_LIMIT counts the migrations", env: []string{"PTAH_LIMIT=2"}, want: []string{"a"}},
		{
			name: "a typed --target over an empty PTAH_TARGET",
			env:  []string{"PTAH_TARGET="},
			args: []string{"--target", "2"},
			want: []string{"a", "b"},
		},
		{
			name: "a typed --limit over an empty PTAH_LIMIT",
			env:  []string{"PTAH_LIMIT="},
			args: []string{"--limit", "1"},
			want: []string{"a", "b"},
		},
		{
			name: "an empty PTAH_TARGET beside a typed --limit",
			env:  []string{"PTAH_TARGET="},
			args: []string{"--limit", "1"},
			want: []string{"a", "b"},
		},
		{name: "PTAH_LIMIT unset beside a typed --target", args: []string{"--target", "1"}, want: []string{"a"}},
		{
			name: "an empty PTAH_LIMIT beside a typed --target",
			env:  []string{"PTAH_LIMIT="},
			args: []string{"--target", "1"},
			want: []string{"a"},
		},
		{
			name: "an empty PTAH_TO_TAG beside a typed --limit",
			env:  []string{"PTAH_TO_TAG="},
			args: []string{"--limit", "1"},
			want: []string{"a", "b"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := appliedNativeStopHistory(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				append([]string{"migrations", "down", "--db-url", targetURL, "--migrations-dir", "migrations", "--confirm"},
					tc.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, tc.want)
		})
	}
}

func TestMigrationsDownStopVariablesE2E_FailurePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		env        []string
		args       []string
		wantStderr string
	}{
		{
			name: "no stop",
			wantStderr: `error: name where the rollback stops: pass --target <version> (--target 0 rolls back every ` +
				`applied migration), --to-tag <tag> or --limit <n>; "ptah migrations status" lists the applied versions` + "\n",
		},
		{
			name: "empty PTAH_TARGET",
			env:  []string{"PTAH_TARGET="},
			wantStderr: "error: PTAH_TARGET is set but empty: give it a value, or unset it and leave --target out to " +
				"stop at --to-tag or --limit\n",
		},
		{
			name: "blank PTAH_TARGET",
			env:  []string{"PTAH_TARGET= "},
			wantStderr: `error: PTAH_TARGET is set to " ", which names nothing: give it a value, or unset it and leave ` +
				"--target out to stop at --to-tag or --limit\n",
		},
		{
			name: "empty PTAH_TARGET beside PTAH_LIMIT",
			env:  []string{"PTAH_LIMIT=1", "PTAH_TARGET="},
			wantStderr: "error: PTAH_TARGET is set but empty: give it a value, or unset it and leave --target out to " +
				"stop at --to-tag or --limit\n",
		},
		{
			name: "empty PTAH_LIMIT beside PTAH_TARGET of 0",
			env:  []string{"PTAH_TARGET=0", "PTAH_LIMIT="},
			wantStderr: "error: PTAH_LIMIT is set but empty: give it a value, or unset it and leave --limit out to " +
				"stop at --target or --to-tag\n",
		},
		{
			name: "empty PTAH_TO_TAG beside PTAH_TARGET of 0",
			env:  []string{"PTAH_TARGET=0", "PTAH_TO_TAG="},
			wantStderr: "error: PTAH_TO_TAG is set but empty: give it a value, or unset it and leave --to-tag out to " +
				"stop at --target or --limit\n",
		},
		{
			name: "PTAH_LIMIT of 0",
			env:  []string{"PTAH_LIMIT=0"},
			wantStderr: `error: PTAH_LIMIT=0 names no stop: name where the rollback stops: pass --target <version> ` +
				`(--target 0 rolls back every applied migration), --to-tag <tag> or --limit <n>; "ptah migrations status" ` +
				"lists the applied versions\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := appliedNativeStopHistory(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				append([]string{"migrations", "down", "--db-url", targetURL, "--migrations-dir", "migrations", "--confirm"},
					tc.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, tc.wantStderr)
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
		})
	}
}

func TestCompatMigrateApplyStopVariableE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  []string
		want []string
	}{
		{name: "unset, every pending migration is applied", want: []string{"a", "b", "c"}},
		{name: "PTAH_TO_VERSION names the version", env: []string{"PTAH_TO_VERSION=2"}, want: []string{"a", "b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := pendingLimitHistory(c)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				"migrate", "apply", "--url", targetURL, "--dir", "file://migrations")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, tc.want)
		})
	}
}

func TestCompatMigrateApplyStopVariableE2E_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir, targetURL := pendingLimitHistory(c)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables("PTAH_TO_VERSION=")},
		"migrate", "apply", "--url", targetURL, "--dir", "file://migrations")

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Equals, "Error: PTAH_TO_VERSION is set but empty: give it a value, or unset it and leave "+
		"--to-version out to apply every pending migration\n")
	c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, make([]string, 0))
}

// TestCompatMigrateDownStopVariablesE2E_FailurePath refuses an empty or blank
// PTAH_TO_VERSION or PTAH_TO_TAG on `ptah-compat migrate down`, as `migrate
// apply` refuses PTAH_TO_VERSION, on the forwarded and the --format path. Read
// as unset, the variable reverts the last applied migration, which is not the
// stop the pipeline meant to set.
func TestCompatMigrateDownStopVariablesE2E_FailurePath(t *testing.T) {
	t.Parallel()
	const omitted = "out to revert the last applied migration\n"
	tests := []struct {
		name       string
		variable   string
		args       []string
		wantStderr string
	}{
		{
			name:       "empty PTAH_TO_VERSION",
			variable:   "PTAH_TO_VERSION=",
			wantStderr: "Error: PTAH_TO_VERSION is set but empty: give it a value, or unset it and leave --to-version " + omitted,
		},
		{
			name:       "blank PTAH_TO_VERSION",
			variable:   "PTAH_TO_VERSION= ",
			wantStderr: `Error: PTAH_TO_VERSION is set to " ", which names nothing: give it a value, or unset it and leave --to-version ` + omitted,
		},
		{
			name:       "empty PTAH_TO_TAG",
			variable:   "PTAH_TO_TAG=",
			wantStderr: "Error: PTAH_TO_TAG is set but empty: give it a value, or unset it and leave --to-tag " + omitted,
		},
		{
			name:       "blank PTAH_TO_VERSION with --format",
			variable:   "PTAH_TO_VERSION= ",
			args:       []string{"--format", compatDownFormat},
			wantStderr: `Error: PTAH_TO_VERSION is set to " ", which names nothing: give it a value, or unset it and leave --to-version ` + omitted,
		},
		{
			name:       "empty PTAH_TO_TAG with --format",
			variable:   "PTAH_TO_TAG=",
			args:       []string{"--format", compatDownFormat},
			wantStderr: "Error: PTAH_TO_TAG is set but empty: give it a value, or unset it and leave --to-tag " + omitted,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := appliedLimitHistory(c)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables(tc.variable)},
				append([]string{"migrate", "down", "--url", targetURL, "--dir", "file://migrations"}, tc.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, tc.wantStderr)
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
		})
	}
}

// TestCompatMigrateDownStopVariablesE2E_HappyPath runs `ptah-compat migrate
// down` with PTAH_TO_VERSION unset, and with an empty PTAH_TARGET or
// PTAH_LIMIT. The native command it forwards to refuses either, but the
// forward types every stop and reads neither variable, so each run reverts the
// last applied migration.
func TestCompatMigrateDownStopVariablesE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  []string
	}{
		{name: "PTAH_TO_VERSION unset"},
		{name: "empty PTAH_TARGET", env: []string{"PTAH_TARGET="}},
		{name: "empty PTAH_LIMIT", env: []string{"PTAH_LIMIT="}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := appliedLimitHistory(c)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				"migrate", "down", "--url", targetURL, "--dir", "file://migrations")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b"})
		})
	}
}
