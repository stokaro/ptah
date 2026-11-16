package atlas_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/envbool/envbooltest"
)

// migrateDiffIncludeVar is the variable `migrate diff` takes its positive
// selection from, in place of an --include flag neither Atlas edition
// registers there.
const migrateDiffIncludeVar = "PTAH_ATLAS_MIGRATE_DIFF_INCLUDE"

// droppedTablePattern reads a table name out of a generated SQLite DROP.
var droppedTablePattern = regexp.MustCompile(`(?m)^DROP TABLE "([^"]+)"`)

// TestAtlasMigrateDiff_RegistersNoIncludeFlag holds both profiles to the flag
// set of the pinned community binary, which has no --include on this command.
func TestAtlasMigrateDiff_RegistersNoIncludeFlag(t *testing.T) {
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
			c.Assert(diff.Flags().Lookup("include"), qt.IsNil)
		})
	}
}

// TestAtlasMigrateDiff_HelpNamesTheIncludeVariable is where a reader of the
// full profile's help learns the variable, and the strict profile, which
// refuses it, leaves it out.
func TestAtlasMigrateDiff_HelpNamesTheIncludeVariable(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy atlascompatpolicy.Policy
		names  bool
	}{
		{name: "full compatibility", policy: atlascompatpolicy.Full(), names: true},
		{name: "strict Community Edition", policy: atlascompatpolicy.StrictCE(), names: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, _, err := atlas.NewCompatCommandWithPolicy("atlas", test.policy).Find([]string{"migrate", "diff"})

			c.Assert(err, qt.IsNil)
			c.Assert(regexp.MustCompile(migrateDiffIncludeVar).MatchString(diff.Long), qt.Equals, test.names)
		})
	}
}

// TestAtlasMigrateDiff_IncludeVariable_HappyPath reads the generated file back
// to see which tables the variable kept. The rows naming no selector are the
// controls: the fixture writes every table when nothing is selected, so a
// missing table in the other rows is the selection's work.
func TestAtlasMigrateDiff_IncludeVariable_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		include func(testing.TB)
		exclude func(testing.TB)
		want    []string
	}{
		{name: "unset", include: envbooltest.Unset(migrateDiffIncludeVar), exclude: envbooltest.Unset(migrateDiffExcludeVar), want: []string{"gadgets", "manual", "widgets"}},
		{name: "empty", include: envbooltest.Set(migrateDiffIncludeVar, ""), exclude: envbooltest.Unset(migrateDiffExcludeVar), want: []string{"gadgets", "manual", "widgets"}},
		{name: "commas and spaces only", include: envbooltest.Set(migrateDiffIncludeVar, " , ,"), exclude: envbooltest.Unset(migrateDiffExcludeVar), want: []string{"gadgets", "manual", "widgets"}},
		{name: "one selector", include: envbooltest.Set(migrateDiffIncludeVar, "widgets"), exclude: envbooltest.Unset(migrateDiffExcludeVar), want: []string{"widgets"}},
		{name: "selectors with spaces and an empty part", include: envbooltest.Set(migrateDiffIncludeVar, " widgets , ,gadgets "), exclude: envbooltest.Unset(migrateDiffExcludeVar), want: []string{"gadgets", "widgets"}},
		{name: "a glob", include: envbooltest.Set(migrateDiffIncludeVar, "*gets"), exclude: envbooltest.Unset(migrateDiffExcludeVar), want: []string{"gadgets", "widgets"}},
		{name: "with the exclusion", include: envbooltest.Set(migrateDiffIncludeVar, "widgets,manual"), exclude: envbooltest.Set(migrateDiffExcludeVar, "manual"), want: []string{"widgets"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.include(t)
			test.exclude(t)
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

// TestAtlasMigrateDiff_IncludeKeepsWhatTheHistoryOwns_HappyPath writes a
// first migration that creates a table the desired schema then stops
// declaring, and reads which tables the second migration drops. The row
// without the variable is the control: the history's table is dropped there,
// so its absence in the other row is the selection's work.
func TestAtlasMigrateDiff_IncludeKeepsWhatTheHistoryOwns_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		include func(testing.TB)
		dropped []string
	}{
		{name: "unset", include: envbooltest.Unset(migrateDiffIncludeVar), dropped: []string{"manual"}},
		{name: "the declared tables", include: envbooltest.Set(migrateDiffIncludeVar, "widgets,gadgets"), dropped: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(migrateDiffIncludeVar)(t)
			envbooltest.Unset(migrateDiffExcludeVar)(t)
			schema, migrations := migrateDiffExcludeFixture(c)
			args := []string{
				"--dev-url", "sqlite://file?mode=memory",
				"--dir", "file://" + filepath.ToSlash(migrations),
				"--to", "file://" + filepath.ToSlash(schema),
			}
			_, stderr, err := runMigrateDiffExclude(atlascompatpolicy.Full(), append([]string{"migrate", "diff", "init"}, args...)...)
			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"+
				"CREATE TABLE gadgets (id INTEGER PRIMARY KEY, name TEXT);\n"), 0o600), qt.IsNil)
			test.include(t)

			_, stderr, err = runMigrateDiffExclude(atlascompatpolicy.Full(), append([]string{"migrate", "diff", "next"}, args...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(generatedNames(c, migrations, droppedTablePattern), qt.DeepEquals, test.dropped)
		})
	}
}

// TestAtlasMigrateDiff_IncludeVariable_FailurePath refuses a selection that
// selects nothing, which would plan an empty migration that reads as synced,
// and a selector the grammar rejects. Both name the variable, since the
// command has no flag to name, and neither writes a migration.
func TestAtlasMigrateDiff_IncludeVariable_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "selects nothing", value: "nothing_here", want: `the PTAH_ATLAS_MIGRATE_DIFF_INCLUDE selection matched no objects: "nothing_here"`},
		{name: "malformed", value: "widgets[type=", want: `.*widgets\[type=.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(migrateDiffExcludeVar)(t)
			t.Setenv(migrateDiffIncludeVar, test.value)
			schema, migrations := migrateDiffExcludeFixture(c)

			stdout, _, err := runMigrateDiffExclude(atlascompatpolicy.Full(),
				"migrate", "diff", "add",
				"--dev-url", "sqlite://file?mode=memory",
				"--dir", "file://"+filepath.ToSlash(migrations),
				"--to", "file://"+filepath.ToSlash(schema),
			)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(generatedTables(c, migrations), qt.IsNil)
		})
	}
}

// TestAtlasMigrateDiff_StrictRefusesTheIncludeVariable_FailurePath builds the
// strict tree without the process boundary that refuses the variable first,
// and expects the same refusal from the command. Presence is what is refused,
// so the empty value is refused too.
func TestAtlasMigrateDiff_StrictRefusesTheIncludeVariable_FailurePath(t *testing.T) {
	for _, value := range []string{"widgets", ""} {
		t.Run(strconv.Quote(value), func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(migrateDiffExcludeVar)(t)
			t.Setenv(migrateDiffIncludeVar, value)
			schema, migrations := migrateDiffExcludeFixture(c)

			stdout, stderr, err := runMigrateDiffExclude(atlascompatpolicy.StrictCE(),
				"migrate", "diff", "add",
				"--dev-url", "sqlite://file?mode=memory",
				"--dir", "file://"+filepath.ToSlash(migrations),
				"--to", "file://"+filepath.ToSlash(schema),
			)

			c.Assert(err, qt.ErrorMatches, `PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ATLAS_MIGRATE_DIFF_INCLUDE`)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "Error: PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ATLAS_MIGRATE_DIFF_INCLUDE\n")
			_, err = os.Stat(migrations)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}
}
