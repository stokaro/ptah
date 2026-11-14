package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/envbool/envbooltest"
)

// migrateDiffExcludeVar is the variable `migrate diff` takes its exclusion
// from, in place of the --exclude flag neither Atlas edition registers there.
const migrateDiffExcludeVar = "PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE"

// migrateDiffExcludeSchema declares three tables, so a selector list can name
// one, name two, or leave every table in place.
const migrateDiffExcludeSchema = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n" +
	"CREATE TABLE manual (id INTEGER PRIMARY KEY);\n" +
	"CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n"

// createdTablePattern reads a table name out of a generated SQLite statement.
var createdTablePattern = regexp.MustCompile(`(?m)^CREATE TABLE "([^"]+)"`)

// createdViewPattern reads a view name out of a generated SQLite statement.
var createdViewPattern = regexp.MustCompile(`(?m)^CREATE VIEW "([^"]+)"`)

// TestAtlasMigrateDiff_RegistersNoExcludeFlag holds both profiles to the flag
// set of the pinned community binary, which has no --exclude on this command.
// The help text must not name the flag either.
func TestAtlasMigrateDiff_RegistersNoExcludeFlag(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy atlascompatpolicy.Policy
	}{
		{name: "full compatibility", policy: atlascompatpolicy.Full()},
		{name: "strict Community Edition", policy: atlascompatpolicy.StrictCE()},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, _, err := atlas.NewCompatCommandWithPolicy("atlas", test.policy).Find([]string{"migrate", "diff"})

			c.Assert(err, qt.IsNil)
			c.Assert(diff.CommandPath(), qt.Equals, "atlas migrate diff")
			c.Assert(diff.Flags().Lookup("exclude"), qt.IsNil)
			c.Assert(diff.Long, qt.Not(qt.Contains), "--exclude")
		})
	}
}

// TestAtlasMigrateDiff_FullHelpNamesTheExcludeVariable is where a reader of
// the full profile's help learns the spelling that replaces the flag.
func TestAtlasMigrateDiff_FullHelpNamesTheExcludeVariable(t *testing.T) {
	c := qt.New(t)

	diff, _, err := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.Full()).Find([]string{"migrate", "diff"})

	c.Assert(err, qt.IsNil)
	c.Assert(diff.Long, qt.Contains, migrateDiffExcludeVar)
}

// TestAtlasMigrateDiff_StrictHelpLeavesTheExcludeVariableOut keeps the
// variable out of the help of the profile that refuses it.
func TestAtlasMigrateDiff_StrictHelpLeavesTheExcludeVariableOut(t *testing.T) {
	c := qt.New(t)

	diff, _, err := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE()).Find([]string{"migrate", "diff"})

	c.Assert(err, qt.IsNil)
	c.Assert(diff.Long, qt.Not(qt.Contains), migrateDiffExcludeVar)
}

// TestAtlasMigrateDiff_ExcludeFlag_FailurePath is the in-process half of the
// measurement on the pinned community binary v1.3.0, which answers
// `migrate diff --exclude x` with `Error: unknown flag: --exclude` and exit 1
// and creates no directory. The exit code is the e2e test's to measure.
func TestAtlasMigrateDiff_ExcludeFlag_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy atlascompatpolicy.Policy
	}{
		{name: "full compatibility", policy: atlascompatpolicy.Full()},
		{name: "strict Community Edition", policy: atlascompatpolicy.StrictCE()},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(migrateDiffExcludeVar)(t)
			schema, migrations := migrateDiffExcludeFixture(c)

			stdout, stderr, err := runMigrateDiffExclude(test.policy,
				"migrate", "diff", "--exclude", "manual",
				"--dev-url", "sqlite://file?mode=memory",
				"--dir", "file://"+filepath.ToSlash(migrations),
				"--to", "file://"+filepath.ToSlash(schema),
			)

			c.Assert(err, qt.ErrorMatches, `unknown flag: --exclude`)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "Error: unknown flag: --exclude\n")
			_, err = os.Stat(migrations)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// TestAtlasMigrateDiff_ExcludeVariable_HappyPath reads the generated file back
// to see which tables the variable left out. The rows naming no selector are
// the controls: the fixture writes every table when nothing is excluded, so a
// missing table in the other rows is the selector's work.
func TestAtlasMigrateDiff_ExcludeVariable_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		env  func(testing.TB)
		want []string
	}{
		{name: "unset", env: envbooltest.Unset(migrateDiffExcludeVar), want: []string{"gadgets", "manual", "widgets"}},
		{name: "empty", env: envbooltest.Set(migrateDiffExcludeVar, ""), want: []string{"gadgets", "manual", "widgets"}},
		{name: "blank", env: envbooltest.Set(migrateDiffExcludeVar, "  "), want: []string{"gadgets", "manual", "widgets"}},
		{name: "commas and spaces only", env: envbooltest.Set(migrateDiffExcludeVar, " , ,"), want: []string{"gadgets", "manual", "widgets"}},
		{name: "one selector", env: envbooltest.Set(migrateDiffExcludeVar, "manual"), want: []string{"gadgets", "widgets"}},
		{name: "comma-separated selectors", env: envbooltest.Set(migrateDiffExcludeVar, "manual,gadgets"), want: []string{"widgets"}},
		{name: "selectors with spaces and an empty part", env: envbooltest.Set(migrateDiffExcludeVar, " manual , ,gadgets "), want: []string{"widgets"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(t)
			schema, migrations := migrateDiffExcludeFixture(c)

			_, stderr, err := runMigrateDiffExclude(atlascompatpolicy.Full(),
				"migrate", "diff", "add",
				"--dev-url", "sqlite://file?mode=memory",
				"--dir", "file://"+filepath.ToSlash(migrations),
				"--to", "file://"+filepath.ToSlash(schema),
			)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(generatedTables(c, migrations), qt.DeepEquals, test.want)
		})
	}
}

// TestAtlasMigrateDiff_EnvExclude_HappyPath covers the Atlas spelling of the
// same selection, the selected env's `exclude` list, which both profiles read,
// and how the variable combines with it: a value naming a selector replaces
// the list, and a value naming none leaves it in place.
func TestAtlasMigrateDiff_EnvExclude_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy atlascompatpolicy.Policy
		env    func(testing.TB)
		want   []string
	}{
		{
			name:   "the env's list under full compatibility",
			policy: atlascompatpolicy.Full(),
			env:    envbooltest.Unset(migrateDiffExcludeVar),
			want:   []string{"gadgets", "widgets"},
		},
		{
			name:   "the env's list under strict Community Edition",
			policy: atlascompatpolicy.StrictCE(),
			env:    envbooltest.Unset(migrateDiffExcludeVar),
			want:   []string{"gadgets", "widgets"},
		},
		{
			name:   "the variable replaces the env's list",
			policy: atlascompatpolicy.Full(),
			env:    envbooltest.Set(migrateDiffExcludeVar, "widgets"),
			want:   []string{"gadgets", "manual"},
		},
		{
			name:   "an empty variable leaves the env's list",
			policy: atlascompatpolicy.Full(),
			env:    envbooltest.Set(migrateDiffExcludeVar, ""),
			want:   []string{"gadgets", "widgets"},
		},
		{
			name:   "a blank variable leaves the env's list",
			policy: atlascompatpolicy.Full(),
			env:    envbooltest.Set(migrateDiffExcludeVar, "  "),
			want:   []string{"gadgets", "widgets"},
		},
		{
			name:   "a variable of commas and spaces leaves the env's list",
			policy: atlascompatpolicy.Full(),
			env:    envbooltest.Set(migrateDiffExcludeVar, " , ,"),
			want:   []string{"gadgets", "widgets"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(t)
			schema, migrations := migrateDiffExcludeFixture(c)
			project := filepath.Join(filepath.Dir(schema), "atlas.hcl")
			c.Assert(os.WriteFile(project, []byte(`env "local" {
  src     = `+strconv.Quote("file://"+filepath.ToSlash(schema))+`
  dev     = "sqlite://file?mode=memory"
  exclude = ["manual"]
  migration {
    dir = `+strconv.Quote("file://"+filepath.ToSlash(migrations))+`
  }
}
`), 0o600), qt.IsNil)

			_, stderr, err := runMigrateDiffExclude(test.policy,
				"migrate", "diff", "add",
				"--config", "file://"+filepath.ToSlash(project),
				"--env", "local",
			)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(generatedTables(c, migrations), qt.DeepEquals, test.want)
		})
	}
}

// TestAtlasMigrateDiff_SchemaModeExclude_HappyPath holds the patterns a
// disabled `schema.mode` implies, which the run carries whether or not the
// variable replaced the env's `exclude` list. The env disables views, so the
// view the desired schema declares is missing from the file in every row.
func TestAtlasMigrateDiff_SchemaModeExclude_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		env    func(testing.TB)
		tables []string
	}{
		{name: "the env's list", env: envbooltest.Unset(migrateDiffExcludeVar), tables: []string{"gadgets", "widgets"}},
		{name: "the variable", env: envbooltest.Set(migrateDiffExcludeVar, "widgets"), tables: []string{"gadgets", "manual"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(t)
			schema, migrations := migrateDiffExcludeFixture(c)
			c.Assert(os.WriteFile(schema, []byte(migrateDiffExcludeSchema+
				"CREATE VIEW recent AS SELECT id FROM gadgets;\n"), 0o600), qt.IsNil)
			project := filepath.Join(filepath.Dir(schema), "atlas.hcl")
			c.Assert(os.WriteFile(project, []byte(`env "local" {
  dev     = "sqlite://file?mode=memory"
  exclude = ["manual"]
  schema {
    src = `+strconv.Quote("file://"+filepath.ToSlash(schema))+`
    mode {
      views = false
    }
  }
  migration {
    dir = `+strconv.Quote("file://"+filepath.ToSlash(migrations))+`
  }
}
`), 0o600), qt.IsNil)

			_, stderr, err := runMigrateDiffExclude(atlascompatpolicy.Full(),
				"migrate", "diff", "add",
				"--config", "file://"+filepath.ToSlash(project),
				"--env", "local",
			)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(generatedTables(c, migrations), qt.DeepEquals, test.tables)
			c.Assert(generatedViews(c, migrations), qt.IsNil)
		})
	}
}

// TestAtlasMigrateDiff_SchemaModeExcludeControl_HappyPath is the control for
// the test above: with the view mode left alone, the same schema writes the
// view, so its absence there is the mode's work.
func TestAtlasMigrateDiff_SchemaModeExcludeControl_HappyPath(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(migrateDiffExcludeVar)(t)
	schema, migrations := migrateDiffExcludeFixture(c)
	c.Assert(os.WriteFile(schema, []byte(migrateDiffExcludeSchema+
		"CREATE VIEW recent AS SELECT id FROM gadgets;\n"), 0o600), qt.IsNil)

	_, stderr, err := runMigrateDiffExclude(atlascompatpolicy.Full(),
		"migrate", "diff", "add",
		"--dev-url", "sqlite://file?mode=memory",
		"--dir", "file://"+filepath.ToSlash(migrations),
		"--to", "file://"+filepath.ToSlash(schema),
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(generatedViews(c, migrations), qt.DeepEquals, []string{"recent"})
}

// TestAtlasMigrateDiff_ExcludeVariable_FailurePath builds the strict tree
// without the process boundary that refuses the variable first, which is how
// an embedder or a test builds it, and expects the same refusal from the
// command: honoring the variable would make strict mode looser than the pinned
// binary, and ignoring it would drop a selection without a word. Presence is
// what is refused, so the empty value is refused too.
func TestAtlasMigrateDiff_ExcludeVariable_FailurePath(t *testing.T) {
	for _, value := range []string{"manual", ""} {
		t.Run(strconv.Quote(value), func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(migrateDiffExcludeVar, value)
			schema, migrations := migrateDiffExcludeFixture(c)

			stdout, stderr, err := runMigrateDiffExclude(atlascompatpolicy.StrictCE(),
				"migrate", "diff", "add",
				"--dev-url", "sqlite://file?mode=memory",
				"--dir", "file://"+filepath.ToSlash(migrations),
				"--to", "file://"+filepath.ToSlash(schema),
			)

			c.Assert(err, qt.ErrorMatches, `PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE`)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "Error: PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE\n")
			_, err = os.Stat(migrations)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// migrateDiffExcludeFixture writes the desired schema into a new directory
// and names a migration directory beside it that does not exist yet.
func migrateDiffExcludeFixture(c *qt.C) (schema, migrations string) {
	c.Helper()
	dir := c.TempDir()
	schema = filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(migrateDiffExcludeSchema), 0o600), qt.IsNil)
	return schema, filepath.Join(dir, "migrations")
}

// runMigrateDiffExclude runs one invocation of the compatibility tree under
// policy and returns what it printed.
func runMigrateDiffExclude(policy atlascompatpolicy.Policy, args ...string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommandWithPolicy("atlas", policy)
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), diagnostics.String(), err
}

// generatedTables returns the tables the generated migration files create,
// sorted.
func generatedTables(c *qt.C, migrations string) []string {
	c.Helper()
	return generatedNames(c, migrations, createdTablePattern)
}

// generatedViews returns the views the generated migration files create,
// sorted.
func generatedViews(c *qt.C, migrations string) []string {
	c.Helper()
	return generatedNames(c, migrations, createdViewPattern)
}

// generatedNames returns the first group of every match of pattern in the
// generated migration files, sorted, and nil when nothing matches.
func generatedNames(c *qt.C, migrations string, pattern *regexp.Regexp) []string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(migrations, "*.sql"))
	c.Assert(err, qt.IsNil)
	var names []string
	for _, file := range files {
		contents, err := os.ReadFile(file)
		c.Assert(err, qt.IsNil)
		for _, match := range pattern.FindAllStringSubmatch(string(contents), -1) {
			names = append(names, match[1])
		}
	}
	slices.Sort(names)
	return names
}
