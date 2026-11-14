package generator_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/selectext"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
)

// The exclusion fixture is a SQLite target holding `widgets` and an unmanaged
// `external_audit`, and Go entities declaring `widgets` with one more column
// and a `reporting_cache` the selectors keep out. Without an exclusion the
// migration creates `reporting_cache` and drops `external_audit`; with one it
// only adds the column, in both directions of the migration.

const excludeModels = `package models

//ptah:schema:table name="widgets"
type Widget struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
	//ptah:schema:field name="name" type="TEXT"
	Name string
}

//ptah:schema:table name="reporting_cache"
type ReportingCache struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`

// excludeFixture is one generation's inputs: the entities, the target and the
// migration directory.
type excludeFixture struct {
	modelsDir     string
	targetURL     string
	migrationsDir string
	shadowURL     string
}

// writeExcludeFixture creates the target with ddl and writes the entities.
// prior, when set, is the up body of a migration already in the directory.
func writeExcludeFixture(c *qt.C, ddl, prior string) excludeFixture {
	c.Helper()
	dir := c.TempDir()
	fixture := excludeFixture{
		modelsDir:     filepath.Join(dir, "models"),
		targetURL:     "sqlite://" + filepath.ToSlash(filepath.Join(dir, "target.db")),
		migrationsDir: filepath.Join(dir, "migrations"),
		shadowURL:     "sqlite://" + filepath.ToSlash(filepath.Join(dir, "shadow.db")),
	}
	c.Assert(os.MkdirAll(fixture.modelsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixture.modelsDir, "models.go"), []byte(excludeModels), 0o600), qt.IsNil)
	c.Assert(os.MkdirAll(fixture.migrationsDir, 0o755), qt.IsNil)
	if prior != "" {
		c.Assert(os.WriteFile(filepath.Join(fixture.migrationsDir, "0000000001_init.up.sql"), []byte(prior), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(fixture.migrationsDir, "0000000001_init.down.sql"),
			[]byte("DROP TABLE legacy_log;\nDROP TABLE widgets;\n"), 0o600), qt.IsNil)
	}
	target, err := dbschema.ConnectToDatabase(c.Context(), fixture.targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	_, err = target.ExecContext(c.Context(), ddl)
	c.Assert(err, qt.IsNil)
	return fixture
}

// generatedBodies returns the up and down bodies of the one migration named
// "second" in dir.
func generatedBodies(c *qt.C, dir string) (up, down string) {
	c.Helper()
	ups, err := filepath.Glob(filepath.Join(dir, "*_second.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(ups, qt.HasLen, 1)
	downs, err := filepath.Glob(filepath.Join(dir, "*_second.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(downs, qt.HasLen, 1)
	return string(must.Must(os.ReadFile(ups[0]))), string(must.Must(os.ReadFile(downs[0])))
}

const excludeTargetDDL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE external_audit (id INTEGER PRIMARY KEY);"

// TestGenerateMigration_ExcludeLeavesBothSidesOut_HappyPath reads each written
// file back. The unexcluded row is the control: it shows the fixture creates
// and drops the two tables the other rows protect.
func TestGenerateMigration_ExcludeLeavesBothSidesOut_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		exclude  []string
		upHas    []string
		upLacks  []string
		downHas  []string
		downLack []string
	}{
		{
			name:    "nothing excluded",
			upHas:   []string{`ADD COLUMN "name" TEXT`, `CREATE TABLE "reporting_cache"`, `DROP TABLE "external_audit"`},
			downHas: []string{`DROP TABLE "reporting_cache"`, `CREATE TABLE "external_audit"`},
		},
		{
			name:     "a declared table and an unmanaged one",
			exclude:  []string{"reporting_cache", "external_audit"},
			upHas:    []string{`ADD COLUMN "name" TEXT`},
			upLacks:  []string{"reporting_cache", "external_audit"},
			downLack: []string{"reporting_cache", "external_audit"},
		},
		{
			name:     "one comma-separated value",
			exclude:  []string{"reporting_cache, external_audit"},
			upHas:    []string{`ADD COLUMN "name" TEXT`},
			upLacks:  []string{"reporting_cache", "external_audit"},
			downLack: []string{"reporting_cache", "external_audit"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeExcludeFixture(c, excludeTargetDDL, "")

			files, err := generator.GenerateMigration(t.Context(), generator.GenerateMigrationOptions{
				Runtime:       must.Must(builtin.New()),
				GoEntitiesDir: fixture.modelsDir,
				DatabaseURL:   fixture.targetURL,
				MigrationName: "second",
				OutputDir:     fixture.migrationsDir,
				Exclude:       test.exclude,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.IsNotNil)
			up, down := generatedBodies(c, fixture.migrationsDir)
			for _, fragment := range test.upHas {
				c.Assert(up, qt.Contains, fragment)
			}
			for _, fragment := range test.upLacks {
				c.Assert(up, qt.Not(qt.Contains), fragment)
			}
			for _, fragment := range test.downHas {
				c.Assert(down, qt.Contains, fragment)
			}
			for _, fragment := range test.downLack {
				c.Assert(down, qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// TestGenerateMigration_ExcludeReachesTheShadowVerification replays a history
// that created `legacy_log`, which the target still holds and the entities no
// longer declare. Excluding it keeps the plan from dropping it, so the shadow
// database, which replays the same history, holds it too: the verification
// has to leave it out of its read-back as the plan did, and leave
// `reporting_cache` out of the declaration it compares with.
func TestGenerateMigration_ExcludeReachesTheShadowVerification(t *testing.T) {
	c := qt.New(t)
	fixture := writeExcludeFixture(c,
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE legacy_log (id INTEGER PRIMARY KEY);",
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE legacy_log (id INTEGER PRIMARY KEY);\n",
	)

	files, err := generator.GenerateMigration(t.Context(), generator.GenerateMigrationOptions{
		Runtime:           must.Must(builtin.New()),
		GoEntitiesDir:     fixture.modelsDir,
		DatabaseURL:       fixture.targetURL,
		MigrationName:     "second",
		OutputDir:         fixture.migrationsDir,
		ShadowDatabaseURL: fixture.shadowURL,
		Exclude:           []string{"legacy_log", "reporting_cache"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	up, down := generatedBodies(c, fixture.migrationsDir)
	c.Assert(up, qt.Contains, `ADD COLUMN "name" TEXT`)
	c.Assert(up, qt.Not(qt.Contains), "legacy_log")
	c.Assert(up, qt.Not(qt.Contains), "reporting_cache")
	c.Assert(down, qt.Not(qt.Contains), "legacy_log")
}

// TestGenerateMigration_ExcludeWarnsAboutASelectorThatMatchedNothing writes
// one warning line for the selector that named nothing on either side, and
// none for the selector that named a declared table only.
func TestGenerateMigration_ExcludeWarnsAboutASelectorThatMatchedNothing(t *testing.T) {
	c := qt.New(t)
	fixture := writeExcludeFixture(c, excludeTargetDDL, "")
	var diagnostics bytes.Buffer

	_, err := generator.GenerateMigration(t.Context(), generator.GenerateMigrationOptions{
		Runtime:       must.Must(builtin.New()),
		GoEntitiesDir: fixture.modelsDir,
		DatabaseURL:   fixture.targetURL,
		MigrationName: "second",
		OutputDir:     fixture.migrationsDir,
		Exclude:       []string{"reporting_cache", "nothing_here"},
		Diagnostics:   &diagnostics,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.String(), qt.Equals, "Warning: the --exclude selection matched no objects: \"nothing_here\".\n")
}

// runtimeWithoutSelections hides the scope selections of the runtime it
// embeds, as a runtime that does not implement selectext.Runtime would.
type runtimeWithoutSelections struct {
	generator.Runtime
}

// TestPlanMigration_Exclude_FailurePath refuses before connecting: the target
// file is never created.
func TestPlanMigration_Exclude_FailurePath(t *testing.T) {
	t.Run("a runtime that serves no scope selections", func(t *testing.T) {
		c := qt.New(t)
		target := filepath.Join(t.TempDir(), "target.db")

		plan, err := generator.PlanMigration(t.Context(), generator.GenerateMigrationOptions{
			Runtime:     runtimeWithoutSelections{must.Must(builtin.New())},
			DatabaseURL: "sqlite://" + filepath.ToSlash(target),
			OutputDir:   t.TempDir(),
			Exclude:     []string{"reporting_cache"},
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
			Exclude:     []string{"us[ers"},
		})

		c.Assert(err, qt.ErrorMatches, `invalid exclude selectors: invalid Atlas exclude glob "us\[ers": syntax error in pattern`)
		c.Assert(plan, qt.IsNil)
		_, statErr := os.Stat(target)
		c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
	})
}
