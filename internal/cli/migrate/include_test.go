package migrate_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/migrate"
)

// TestMigrateGenerateInclude_HappyPath runs the exclusion fixture under an
// inclusion. The tests of the exclusion carry the controls: without a
// selection each command creates `reporting_cache` and drops the table the
// current state holds that the declaration does not name.
func TestMigrateGenerateInclude_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		args    func(excludeCLIFixture) []string
		include []string
		has     []string
		lacks   []string
	}{
		{
			name:    "the target",
			args:    excludeCLIFixture.targetArgs,
			include: []string{"--include", "widgets"},
			has:     []string{`ADD COLUMN "name" TEXT`},
			lacks:   []string{"reporting_cache", "external_audit"},
		},
		{
			name:    "a replay",
			args:    excludeCLIFixture.replayArgs,
			include: []string{"--include", "widgets"},
			has:     []string{`ADD COLUMN "name" TEXT`},
			lacks:   []string{"reporting_cache", "legacy_log"},
		},
		{
			name:    "a replay with an exclusion",
			args:    excludeCLIFixture.replayArgs,
			include: []string{"--include", "widgets,reporting_cache", "--exclude", "reporting_cache"},
			has:     []string{`ADD COLUMN "name" TEXT`},
			lacks:   []string{"reporting_cache", "legacy_log"},
		},
		{
			name:    "a replay selecting a declared table",
			args:    excludeCLIFixture.replayArgs,
			include: []string{"--include", "widgets", "--include", "reporting_cache"},
			has:     []string{`ADD COLUMN "name" TEXT`, `CREATE TABLE "reporting_cache"`},
			lacks:   []string{"legacy_log"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeExcludeCLIFixture(c)

			out, err := runGenerate(append(test.args(fixture), test.include...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			up := secondUpBody(c, fixture.migrationsDir)
			for _, fragment := range test.has {
				c.Assert(up, qt.Contains, fragment)
			}
			for _, fragment := range test.lacks {
				c.Assert(up, qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// TestMigrateGenerateInclude_FailurePath refuses a syntax error before the
// replay resets the dev database, so that file is never created, and a
// selection that selects nothing before a migration file is written.
func TestMigrateGenerateInclude_FailurePath(t *testing.T) {
	t.Run("a syntax error", func(t *testing.T) {
		c := qt.New(t)
		fixture := writeExcludeCLIFixture(c)

		out, err := runGenerate(append(fixture.replayArgs(), "--include", "us[ers")...)

		c.Assert(err, qt.ErrorMatches, `invalid --include: .*us\[ers.*`, qt.Commentf("%s", out))
		_, statErr := os.Stat(fixture.devPath)
		c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		matches, globErr := filepath.Glob(filepath.Join(fixture.migrationsDir, "*_second.*"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 0)
	})
	t.Run("a selection that selects nothing", func(t *testing.T) {
		c := qt.New(t)
		fixture := writeExcludeCLIFixture(c)

		out, err := runGenerate(append(fixture.replayArgs(), "--include", "nothing_here")...)

		c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "nothing_here"`, qt.Commentf("%s", out))
		matches, globErr := filepath.Glob(filepath.Join(fixture.migrationsDir, "*_second.*"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 0)
	})
}

func TestMigratePlanInclude_HappyPath(t *testing.T) {
	c := qt.New(t)
	fixture := writeExcludeCLIFixture(c)

	stdout, stderr, err := runPlan("--db-url", fixture.targetURL, "--schema-file", fixture.schemaFile, "--include", "widgets")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Contains, `ADD COLUMN "name" TEXT`)
	c.Assert(stdout, qt.Not(qt.Contains), "reporting_cache")
	c.Assert(stdout, qt.Not(qt.Contains), "external_audit")
}

func TestMigratePlanInclude_FailurePath(t *testing.T) {
	c := qt.New(t)
	fixture := writeExcludeCLIFixture(c)

	stdout, stderr, err := runPlan("--db-url", fixture.targetURL, "--schema-file", fixture.schemaFile, "--include", "nothing_here")

	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "nothing_here"`, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Not(qt.Contains), "=== MIGRATION SQL ===")
}

// TestMigrationsPlanningCommandsTakeInclude holds both verbs to a repeatable
// flag, so the root's environment binding gives it PTAH_INCLUDE.
func TestMigrationsPlanningCommandsTakeInclude(t *testing.T) {
	tests := []struct {
		name    string
		command func() *cobra.Command
	}{
		{name: "plan", command: migrate.NewMigrateCommand},
		{name: "generate", command: migrate.NewMigrateGenerateCommand},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			flag := test.command().Flags().Lookup("include")

			c.Assert(flag, qt.IsNotNil)
			c.Assert(flag.Value.Type(), qt.Equals, "stringArray")
		})
	}
}
