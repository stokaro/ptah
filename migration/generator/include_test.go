package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/selectext"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasfilter"
	"ptah.run/migration/generator"
)

// TestGenerateMigration_IncludeKeepsOnlyWhatItSelects_HappyPath runs the
// exclusion fixture under an inclusion. The row selecting nothing is the
// control: there the migration creates `reporting_cache` and drops
// `external_audit`, so their absence in the other rows is the selection's
// work, and `widgets` still gains its column.
func TestGenerateMigration_IncludeKeepsOnlyWhatItSelects_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		include  []string
		exclude  []string
		upHas    []string
		upLacks  []string
		downLack []string
	}{
		{
			name:  "nothing selected",
			upHas: []string{`ADD COLUMN "name" TEXT`, `CREATE TABLE "reporting_cache"`, `DROP TABLE "external_audit"`},
		},
		{
			name:     "the managed table",
			include:  []string{"widgets"},
			upHas:    []string{`ADD COLUMN "name" TEXT`},
			upLacks:  []string{"reporting_cache", "external_audit"},
			downLack: []string{"reporting_cache", "external_audit"},
		},
		{
			name:     "one comma-separated value",
			include:  []string{"widgets, reporting_cache"},
			upHas:    []string{`ADD COLUMN "name" TEXT`, `CREATE TABLE "reporting_cache"`},
			upLacks:  []string{"external_audit"},
			downLack: []string{"external_audit"},
		},
		{
			name:     "with an exclusion",
			include:  []string{"widgets", "reporting_cache"},
			exclude:  []string{"reporting_cache"},
			upHas:    []string{`ADD COLUMN "name" TEXT`},
			upLacks:  []string{"reporting_cache", "external_audit"},
			downLack: []string{"reporting_cache", "external_audit"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeExcludeFixture(c, excludeTargetDDL, "")

			_, err := generator.GenerateMigration(t.Context(), generator.GenerateMigrationOptions{
				Runtime:       must.Must(builtin.New()),
				GoEntitiesDir: fixture.modelsDir,
				DatabaseURL:   fixture.targetURL,
				MigrationName: "second",
				OutputDir:     fixture.migrationsDir,
				Include:       test.include,
				Exclude:       test.exclude,
			})

			c.Assert(err, qt.IsNil)
			up, down := generatedBodies(c, fixture.migrationsDir)
			for _, fragment := range test.upHas {
				c.Assert(up, qt.Contains, fragment)
			}
			for _, fragment := range test.upLacks {
				c.Assert(up, qt.Not(qt.Contains), fragment)
			}
			for _, fragment := range test.downLack {
				c.Assert(down, qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// TestGenerateMigration_IncludeReachesTheShadowVerification replays a history
// that created `legacy_log`, which the target still holds and the entities no
// longer declare. Selecting only `widgets` keeps the plan from dropping it, so
// the shadow database holds it too, and the verification has to leave it out
// of its read-back as the plan did.
func TestGenerateMigration_IncludeReachesTheShadowVerification(t *testing.T) {
	c := qt.New(t)
	fixture := writeExcludeFixture(c,
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE legacy_log (id INTEGER PRIMARY KEY);",
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE legacy_log (id INTEGER PRIMARY KEY);\n",
	)

	_, err := generator.GenerateMigration(t.Context(), generator.GenerateMigrationOptions{
		Runtime:           must.Must(builtin.New()),
		GoEntitiesDir:     fixture.modelsDir,
		DatabaseURL:       fixture.targetURL,
		MigrationName:     "second",
		OutputDir:         fixture.migrationsDir,
		ShadowDatabaseURL: fixture.shadowURL,
		Include:           []string{"widgets"},
	})

	c.Assert(err, qt.IsNil)
	up, down := generatedBodies(c, fixture.migrationsDir)
	c.Assert(up, qt.Contains, `ADD COLUMN "name" TEXT`)
	c.Assert(up, qt.Not(qt.Contains), "legacy_log")
	c.Assert(down, qt.Not(qt.Contains), "legacy_log")
}

// TestGenerateMigration_IncludeDropsAllItSelectsUnderTheShadow selects only
// `external_audit`, which the history created and the entities no longer
// declare. The plan drops it, so after the replay the shadow database holds
// nothing the selection selects, and neither does the declaration: the
// verification compares the two empty sides rather than refuse a selection
// the plan accepted.
func TestGenerateMigration_IncludeDropsAllItSelectsUnderTheShadow(t *testing.T) {
	c := qt.New(t)
	fixture := writeExcludeFixture(c, excludeTargetDDL,
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE external_audit (id INTEGER PRIMARY KEY);\n",
	)

	_, err := generator.GenerateMigration(t.Context(), generator.GenerateMigrationOptions{
		Runtime:           must.Must(builtin.New()),
		GoEntitiesDir:     fixture.modelsDir,
		DatabaseURL:       fixture.targetURL,
		MigrationName:     "second",
		OutputDir:         fixture.migrationsDir,
		ShadowDatabaseURL: fixture.shadowURL,
		Include:           []string{"external_audit"},
	})

	c.Assert(err, qt.IsNil)
	up, down := generatedBodies(c, fixture.migrationsDir)
	c.Assert(up, qt.Contains, `DROP TABLE "external_audit"`)
	c.Assert(up, qt.Not(qt.Contains), "widgets")
	c.Assert(down, qt.Contains, `CREATE TABLE "external_audit"`)
}

// TestGenerateMigration_IncludeSelectingNothing_FailurePath refuses a
// selection that selects nothing on either side, which would write a
// migration that reads as synced, and writes no file.
func TestGenerateMigration_IncludeSelectingNothing_FailurePath(t *testing.T) {
	c := qt.New(t)
	fixture := writeExcludeFixture(c, excludeTargetDDL, "")

	files, err := generator.GenerateMigration(t.Context(), generator.GenerateMigrationOptions{
		Runtime:       must.Must(builtin.New()),
		GoEntitiesDir: fixture.modelsDir,
		DatabaseURL:   fixture.targetURL,
		MigrationName: "second",
		OutputDir:     fixture.migrationsDir,
		Include:       []string{"nothing_here"},
	})

	c.Assert(err, qt.ErrorAs, new(*atlasfilter.EmptySelectionError))
	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "nothing_here"`)
	c.Assert(files, qt.IsNil)
	written, globErr := filepath.Glob(filepath.Join(fixture.migrationsDir, "*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

// TestPlanMigration_Include_FailurePath refuses before connecting: the target
// file is never created.
func TestPlanMigration_Include_FailurePath(t *testing.T) {
	t.Run("a runtime that serves no scope selections", func(t *testing.T) {
		c := qt.New(t)
		target := filepath.Join(t.TempDir(), "target.db")

		plan, err := generator.PlanMigration(t.Context(), generator.GenerateMigrationOptions{
			Runtime:     runtimeWithoutSelections{must.Must(builtin.New())},
			DatabaseURL: "sqlite://" + filepath.ToSlash(target),
			OutputDir:   t.TempDir(),
			Include:     []string{"widgets"},
		})

		c.Assert(err, qt.ErrorIs, selectext.ErrUnselected)
		c.Assert(plan, qt.IsNil)
		_, statErr := os.Stat(target)
		c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
	})
	t.Run("a malformed selector", func(t *testing.T) {
		c := qt.New(t)
		target := filepath.Join(t.TempDir(), "target.db")

		plan, err := generator.PlanMigration(t.Context(), generator.GenerateMigrationOptions{
			Runtime:     must.Must(builtin.New()),
			DatabaseURL: "sqlite://" + filepath.ToSlash(target),
			OutputDir:   t.TempDir(),
			Include:     []string{"us[ers"},
		})

		c.Assert(err, qt.ErrorMatches, `invalid include selectors: .*us\[ers.*`)
		c.Assert(plan, qt.IsNil)
		_, statErr := os.Stat(target)
		c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
	})
}
