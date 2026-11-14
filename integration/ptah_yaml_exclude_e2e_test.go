//go:build integration

package integration_test

import (
	"bufio"
	"cmp"
	"database/sql"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// The fixture declares `widgets` with a column the target lacks, and a
// `reporting_cache` the target does not hold. The target holds `widgets` and
// an `external_audit` another tool owns. Without an exclusion a plan adds the
// column, creates `reporting_cache` and drops `external_audit`. Each project
// file below sits in the directory the command runs in, as ./ptah.yaml.

const (
	yamlExcludeDesiredSQL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT);\n" +
		"CREATE TABLE reporting_cache (id INTEGER PRIMARY KEY);\n"
	yamlExcludeTargetDDL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE external_audit (id INTEGER PRIMARY KEY);"

	// yamlExcludeWithoutKey is the control: a project file that sets
	// something, and no exclude list.
	yamlExcludeWithoutKey = "migration:\n  connect_timeout: 10s\n"
	// yamlExcludeBoth leaves both tables alone.
	yamlExcludeBoth = "exclude: [external_audit, reporting_cache]\n"
	// yamlExcludeUnmatched leaves both tables alone and lists a selector that
	// names nothing on either side.
	yamlExcludeUnmatched = "exclude: [external_audit, reporting_cache, nosuch_table]\n"
	// yamlExcludeAudit leaves only the table another tool owns alone.
	yamlExcludeAudit = "exclude: [external_audit]\n"
	// yamlExcludeEnvs carries a top-level list and an env whose own list
	// replaces it when --env names the env.
	yamlExcludeEnvs = "exclude: [external_audit]\nenv:\n  reporting:\n    exclude: [reporting_cache]\n"
)

// yamlExcludeFixture is one run's directory and target database.
type yamlExcludeFixture struct {
	dir           string
	targetURL     string
	migrationsDir string
}

// writeYAMLExcludeFixture writes project as ./ptah.yaml beside the desired
// schema, and creates the target.
func writeYAMLExcludeFixture(c *qt.C, project string) yamlExcludeFixture {
	c.Helper()
	dir := c.TempDir()
	fixture := yamlExcludeFixture{
		dir:           dir,
		targetURL:     "sqlite://" + filepath.ToSlash(filepath.Join(dir, "target.db")),
		migrationsDir: filepath.Join(dir, "migrations"),
	}
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte(project), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(yamlExcludeDesiredSQL), 0o600), qt.IsNil)
	c.Assert(os.MkdirAll(fixture.migrationsDir, 0o755), qt.IsNil)
	target, err := dbschema.ConnectToDatabase(c.Context(), fixture.targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	_, err = target.ExecContext(c.Context(), yamlExcludeTargetDDL)
	c.Assert(err, qt.IsNil)
	return fixture
}

// run runs the shipped ptah in the fixture's directory, with no PTAH_*
// variable but env.
func (f yamlExcludeFixture) run(c *qt.C, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: f.dir, Env: append(environmentWithoutPtahVariables(), env...)}, args...)
}

// yamlExcludePrecedenceRow is one project file and one command line. The
// precedence rows are the reason the table exists: a flag or PTAH_EXCLUDE adds
// to the file's list, where the pinned Atlas community binary's replacement
// would drop `external_audit`.
type yamlExcludePrecedenceRow struct {
	name    string
	project string
	args    []string
	env     []string
	has     []string
	lacks   []string
}

// yamlExcludePrecedenceRows are read back from the database by the apply test
// and from the written file by the generate test. has and lacks name tables.
var yamlExcludePrecedenceRows = []yamlExcludePrecedenceRow{
	{
		name:    "a project file without the key",
		project: yamlExcludeWithoutKey,
		has:     []string{"reporting_cache"},
		lacks:   []string{"external_audit"},
	},
	{
		name:    "the key",
		project: yamlExcludeBoth,
		has:     []string{"external_audit"},
		lacks:   []string{"reporting_cache"},
	},
	{
		name:    "the flag without the key",
		project: yamlExcludeWithoutKey,
		args:    []string{"--exclude", "reporting_cache"},
		lacks:   []string{"external_audit", "reporting_cache"},
	},
	{
		name:    "the flag adds to the key",
		project: yamlExcludeAudit,
		args:    []string{"--exclude", "reporting_cache"},
		has:     []string{"external_audit"},
		lacks:   []string{"reporting_cache"},
	},
	{
		name:    "PTAH_EXCLUDE adds to the key",
		project: yamlExcludeAudit,
		env:     []string{"PTAH_EXCLUDE=reporting_cache"},
		has:     []string{"external_audit"},
		lacks:   []string{"reporting_cache"},
	},
	{
		name:    "an env no run selects",
		project: yamlExcludeEnvs,
		has:     []string{"external_audit", "reporting_cache"},
	},
	{
		name:    "the selected env's list replaces the top-level list",
		project: yamlExcludeEnvs,
		args:    []string{"--env", "reporting"},
		lacks:   []string{"external_audit", "reporting_cache"},
	},
}

// TestPtahYAMLExcludeSchemaApply_HappyPathE2E applies the plan and reads the
// target's tables back. A table in has is still there, or was created; one in
// lacks was dropped, or never created.
func TestPtahYAMLExcludeSchemaApply_HappyPathE2E(t *testing.T) {
	for _, row := range yamlExcludePrecedenceRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeYAMLExcludeFixture(c, row.project)
			args := slices.Concat([]string{
				"schema", "apply", "--db-url", fixture.targetURL, "--schema-file", "schema.sql", "--auto-approve",
			}, row.args)

			result := fixture.run(c, row.env, args...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			tables := sqliteTableNames(c, fixture.targetURL)
			c.Assert(tables, qt.Contains, "widgets")
			for _, table := range row.has {
				c.Assert(tables, qt.Contains, table)
			}
			for _, table := range row.lacks {
				c.Assert(tables, qt.Not(qt.Contains), table)
			}
		})
	}
}

// TestPtahYAMLExcludeMigrationsGenerate_HappyPathE2E reads the written up
// file back. A table in has is created or dropped by it; one in lacks is not
// named at all.
func TestPtahYAMLExcludeMigrationsGenerate_HappyPathE2E(t *testing.T) {
	rows := []struct {
		name    string
		project string
		args    []string
		env     []string
		has     []string
		lacks   []string
	}{
		{
			name:    "a project file without the key",
			project: yamlExcludeWithoutKey,
			has:     []string{`CREATE TABLE "reporting_cache"`, `DROP TABLE "external_audit"`},
		},
		{
			name:    "the key",
			project: yamlExcludeBoth,
			lacks:   []string{"reporting_cache", "external_audit"},
		},
		{
			name:    "the flag adds to the key",
			project: yamlExcludeAudit,
			args:    []string{"--exclude", "reporting_cache"},
			lacks:   []string{"reporting_cache", "external_audit"},
		},
		{
			name:    "PTAH_EXCLUDE adds to the key",
			project: yamlExcludeAudit,
			env:     []string{"PTAH_EXCLUDE=reporting_cache"},
			lacks:   []string{"reporting_cache", "external_audit"},
		},
		{
			name:    "the selected env's list replaces the top-level list",
			project: yamlExcludeEnvs,
			args:    []string{"--env", "reporting"},
			has:     []string{`DROP TABLE "external_audit"`},
			lacks:   []string{"reporting_cache"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeYAMLExcludeFixture(c, row.project)
			args := slices.Concat([]string{
				"migrations", "generate", "--db-url", fixture.targetURL, "--schema-file", "schema.sql",
				"--migrations-dir", fixture.migrationsDir, "--name", "second",
			}, row.args)

			result := fixture.run(c, row.env, args...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			up, down := generatedMigration(c, fixture.migrationsDir)
			c.Assert(up, qt.Contains, `ADD COLUMN "name" TEXT`)
			for _, fragment := range row.has {
				c.Assert(up, qt.Contains, fragment)
			}
			for _, fragment := range row.lacks {
				c.Assert(up, qt.Not(qt.Contains), fragment)
				c.Assert(down, qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// TestPtahYAMLExcludeMigrationsGenerateNamesTheFile_HappyPathE2E lists a
// selector that names nothing in the project file and generates a migration:
// the warning names the file, and the migration is written all the same.
func TestPtahYAMLExcludeMigrationsGenerateNamesTheFile_HappyPathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := writeYAMLExcludeFixture(c, yamlExcludeUnmatched)

	result := fixture.run(c, nil,
		"migrations", "generate", "--db-url", fixture.targetURL, "--schema-file", "schema.sql",
		"--migrations-dir", fixture.migrationsDir, "--name", "second",
	)

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(result.Stderr, qt.Contains, `Warning: the exclude selection from ptah.yaml matched no objects: "nosuch_table".`)
	up, _ := generatedMigration(c, fixture.migrationsDir)
	c.Assert(up, qt.Contains, `ADD COLUMN "name" TEXT`)
}

// TestPtahYAMLExcludeComparisonVerbs_HappyPathE2E runs each native verb that
// compares the fixture, once without the key and once with it. Each run
// without the key is the control for the run after it: it shows the verb
// reports the two tables the key leaves out.
func TestPtahYAMLExcludeComparisonVerbs_HappyPathE2E(t *testing.T) {
	verbs := []struct {
		name     string
		args     func(yamlExcludeFixture) []string
		exitCode int
		// kept is reported by both runs, so a run that printed nothing
		// cannot pass for one that left the two tables out.
		kept     string
		reported []string
		withheld []string
		// unmatched is what the warning lists for yamlExcludeUnmatched: the
		// selectors that name nothing on either side the verb reads.
		unmatched string
	}{
		{
			name: "schema diff",
			args: func(f yamlExcludeFixture) []string {
				return []string{"schema", "diff", "--from", f.targetURL, "--to", "file://schema.sql", "--dev-url", "sqlite://dev?mode=memory"}
			},
			kept:      `ADD COLUMN "name" TEXT`,
			reported:  []string{`CREATE TABLE "reporting_cache"`, `DROP TABLE "external_audit"`},
			withheld:  []string{"CREATE TABLE", "DROP TABLE"},
			unmatched: `"nosuch_table"`,
		},
		{
			name: "schema plan",
			args: func(f yamlExcludeFixture) []string {
				return []string{"schema", "plan", "--db-url", f.targetURL, "--schema-file", "schema.sql", "--dry-run"}
			},
			kept:      `ADD COLUMN \"name\" TEXT`,
			reported:  []string{`CREATE TABLE \"reporting_cache\"`, `DROP TABLE \"external_audit\"`},
			withheld:  []string{"CREATE TABLE", "DROP TABLE"},
			unmatched: `"nosuch_table"`,
		},
		{
			name: "schema inspect",
			args: func(f yamlExcludeFixture) []string {
				return []string{"schema", "inspect", "--db-url", f.targetURL}
			},
			kept:     `table "widgets"`,
			reported: []string{`table "external_audit"`},
			withheld: []string{"external_audit"},
			// Inspect reads the database alone, where reporting_cache does not
			// exist either.
			unmatched: `"reporting_cache", "nosuch_table"`,
		},
		{
			name: "schema compare",
			args: func(f yamlExcludeFixture) []string {
				return []string{"schema", "compare", "--db-url", f.targetURL, "--schema-file", "schema.sql"}
			},
			kept:      `ADD COLUMN "name" TEXT`,
			reported:  []string{`CREATE TABLE "reporting_cache"`, `DROP TABLE "external_audit"`},
			withheld:  []string{"reporting_cache", "external_audit"},
			unmatched: `"nosuch_table"`,
		},
		{
			name: "schema drift",
			args: func(f yamlExcludeFixture) []string {
				return []string{"schema", "drift", "--db-url", f.targetURL, "--schema-file", "schema.sql", "--format", "json"}
			},
			// The column the target lacks is drift either way.
			exitCode:  1,
			kept:      `"columns_added"`,
			reported:  []string{`"reporting_cache"`, `"external_audit"`},
			withheld:  []string{"reporting_cache", "external_audit"},
			unmatched: `"nosuch_table"`,
		},
		{
			name: "migrations plan",
			args: func(f yamlExcludeFixture) []string {
				return []string{"migrations", "plan", "--db-url", f.targetURL, "--schema-file", "schema.sql"}
			},
			kept:      `ADD COLUMN "name" TEXT`,
			reported:  []string{`CREATE TABLE "reporting_cache"`, `DROP TABLE "external_audit"`},
			withheld:  []string{"reporting_cache", "external_audit"},
			unmatched: `"nosuch_table"`,
		},
	}

	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			t.Run("a project file without the key", func(t *testing.T) {
				c := qt.New(t)
				fixture := writeYAMLExcludeFixture(c, yamlExcludeWithoutKey)

				result := fixture.run(c, nil, verb.args(fixture)...)

				c.Assert(result.ExitCode, qt.Equals, verb.exitCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
				c.Assert(result.Stdout, qt.Contains, verb.kept)
				for _, fragment := range verb.reported {
					c.Assert(result.Stdout, qt.Contains, fragment)
				}
			})
			t.Run("the key", func(t *testing.T) {
				c := qt.New(t)
				fixture := writeYAMLExcludeFixture(c, yamlExcludeBoth)

				result := fixture.run(c, nil, verb.args(fixture)...)

				c.Assert(result.ExitCode, qt.Equals, verb.exitCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
				c.Assert(result.Stdout, qt.Contains, verb.kept)
				for _, fragment := range verb.withheld {
					c.Assert(result.Stdout, qt.Not(qt.Contains), fragment)
				}
			})
			// Each verb hands the list's sources to its own comparison, so a
			// call site that dropped them would name the file's selector as
			// the flag's on that verb alone.
			t.Run("a selector the key lists that matches nothing", func(t *testing.T) {
				c := qt.New(t)
				fixture := writeYAMLExcludeFixture(c, yamlExcludeUnmatched)

				result := fixture.run(c, nil, verb.args(fixture)...)

				c.Assert(result.ExitCode, qt.Equals, verb.exitCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
				c.Assert(result.Stderr, qt.Contains, "Warning: the exclude selection from ptah.yaml matched no objects: "+verb.unmatched+".\n")
				c.Assert(result.Stderr, qt.Not(qt.Contains), "--exclude selection")
			})
		})
	}
}

// yamlExcludeEverything carries a top-level list and an env that empties it.
const yamlExcludeEverything = "exclude: [external_audit, reporting_cache]\nenv:\n  everything:\n    exclude: []\n"

// TestPtahYAMLExcludePrecedenceOnComparisons_HappyPathE2E runs the
// precedence rows through two more verbs that print their plan. A table in
// named appears in the output; one in left does not.
func TestPtahYAMLExcludePrecedenceOnComparisons_HappyPathE2E(t *testing.T) {
	verbs := []struct {
		name string
		args func(yamlExcludeFixture) []string
	}{
		{
			name: "schema diff",
			args: func(f yamlExcludeFixture) []string {
				return []string{"schema", "diff", "--from", f.targetURL, "--to", "file://schema.sql", "--dev-url", "sqlite://dev?mode=memory"}
			},
		},
		{
			name: "migrations plan",
			args: func(f yamlExcludeFixture) []string {
				return []string{"migrations", "plan", "--db-url", f.targetURL, "--schema-file", "schema.sql"}
			},
		},
	}
	rows := []struct {
		name    string
		project string
		args    []string
		env     []string
		named   []string
		left    []string
	}{
		{
			name:    "the flag adds to the key",
			project: yamlExcludeAudit,
			args:    []string{"--exclude", "reporting_cache"},
			left:    []string{"external_audit", "reporting_cache"},
		},
		{
			name:    "PTAH_EXCLUDE adds to the key",
			project: yamlExcludeAudit,
			env:     []string{"PTAH_EXCLUDE=reporting_cache"},
			left:    []string{"external_audit", "reporting_cache"},
		},
		{
			name:    "the selected env's list replaces the top-level list",
			project: yamlExcludeEnvs,
			args:    []string{"--env", "reporting"},
			named:   []string{"external_audit"},
			left:    []string{"reporting_cache"},
		},
		{
			name:    "an env with an empty list excludes nothing",
			project: yamlExcludeEverything,
			args:    []string{"--env", "everything"},
			named:   []string{"external_audit", "reporting_cache"},
		},
		{
			name:    "the flag adds to an emptied list",
			project: yamlExcludeEverything,
			args:    []string{"--env", "everything", "--exclude", "reporting_cache"},
			named:   []string{"external_audit"},
			left:    []string{"reporting_cache"},
		},
	}

	for _, verb := range verbs {
		for _, row := range rows {
			t.Run(verb.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				fixture := writeYAMLExcludeFixture(c, row.project)

				result := fixture.run(c, row.env, slices.Concat(verb.args(fixture), row.args)...)

				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
				c.Assert(result.Stdout, qt.Contains, `ADD COLUMN "name" TEXT`)
				for _, table := range row.named {
					c.Assert(result.Stdout, qt.Contains, `"`+table+`"`)
				}
				for _, table := range row.left {
					c.Assert(result.Stdout, qt.Not(qt.Contains), table)
				}
			})
		}
	}
}

// TestPtahYAMLExcludeAtlasEnvReplacesTheList_HappyPathE2E applies the plan
// with ./atlas.hcl beside ./ptah.yaml and reads the tables back. The selected
// atlas.hcl env's list replaces the ptah.yaml list it is merged over; the env
// without a list is the control, and keeps the ptah.yaml list.
func TestPtahYAMLExcludeAtlasEnvReplacesTheList_HappyPathE2E(t *testing.T) {
	rows := []struct {
		name  string
		atlas string
		has   []string
		lacks []string
	}{
		{
			name:  "an atlas.hcl env without a list",
			atlas: "env \"audit\" {\n}\n",
			has:   []string{"external_audit", "reporting_cache"},
		},
		{
			name:  "an atlas.hcl env with its own list",
			atlas: "env \"audit\" {\n  exclude = [\"reporting_cache\"]\n}\n",
			lacks: []string{"external_audit", "reporting_cache"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeYAMLExcludeFixture(c, "exclude: [external_audit]\nenv:\n  audit:\n    migration:\n      connect_timeout: 10s\n")
			c.Assert(os.WriteFile(filepath.Join(fixture.dir, "atlas.hcl"), []byte(row.atlas), 0o600), qt.IsNil)

			result := fixture.run(c, nil, "schema", "apply", "--db-url", fixture.targetURL, "--schema-file", "schema.sql",
				"--auto-approve", "--env", "audit")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			tables := sqliteTableNames(c, fixture.targetURL)
			c.Assert(tables, qt.Contains, "widgets")
			for _, table := range row.has {
				c.Assert(tables, qt.Contains, table)
			}
			for _, table := range row.lacks {
				c.Assert(tables, qt.Not(qt.Contains), table)
			}
		})
	}
}

// TestPtahYAMLExcludeSchemaApply_FailurePathE2E refuses a project list the
// run cannot honor as a usage error, before the target is changed.
func TestPtahYAMLExcludeSchemaApply_FailurePathE2E(t *testing.T) {
	t.Run("a malformed selector names the file", func(t *testing.T) {
		c := qt.New(t)
		fixture := writeYAMLExcludeFixture(c, "exclude: [\"us[ers\"]\n")

		result := fixture.run(c, nil, "schema", "apply", "--db-url", fixture.targetURL, "--schema-file", "schema.sql", "--auto-approve")

		c.Assert(result.ExitCode, qt.Equals, 2)
		c.Assert(result.Stderr, qt.Equals,
			"error: invalid exclude in ptah.yaml: invalid Atlas exclude glob \"us[ers\": syntax error in pattern\n")
		c.Assert(sqliteTableNames(c, fixture.targetURL), qt.DeepEquals, []string{"external_audit", "widgets"})
	})
	t.Run("a selector that matches nothing protects nothing", func(t *testing.T) {
		c := qt.New(t)
		fixture := writeYAMLExcludeFixture(c, "exclude: [nosuch_table]\n")

		result := fixture.run(c, nil, "schema", "apply", "--db-url", fixture.targetURL, "--schema-file", "schema.sql", "--auto-approve")

		c.Assert(result.ExitCode, qt.Equals, 2)
		c.Assert(result.Stderr, qt.Contains, `the exclude selection from ptah.yaml matched no objects: "nosuch_table"`)
		c.Assert(sqliteTableNames(c, fixture.targetURL), qt.DeepEquals, []string{"external_audit", "widgets"})
	})
}

// TestPtahYAMLExcludeSchemaApplyAllowsAnUnmatchedSelector_HappyPathE2E is
// the control for the refusal above: the variable that relaxes it for
// --exclude relaxes it for the project list, and the run applies the plan.
func TestPtahYAMLExcludeSchemaApplyAllowsAnUnmatchedSelector_HappyPathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := writeYAMLExcludeFixture(c, "exclude: [nosuch_table, external_audit]\n")

	result := fixture.run(c, []string{"PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1"},
		"schema", "apply", "--db-url", fixture.targetURL, "--schema-file", "schema.sql", "--auto-approve")

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(result.Stderr, qt.Contains, `the exclude selection from ptah.yaml matched no objects: "nosuch_table"`)
	tables := sqliteTableNames(c, fixture.targetURL)
	c.Assert(tables, qt.Contains, "external_audit")
	c.Assert(tables, qt.Contains, "reporting_cache")
}

// TestPtahYAMLExcludeUnmatchedNamesItsSource_HappyPathE2E plans with a
// project list, a flag and a PTAH_* variable that each name an object that
// does not exist, and reads the warning: each selector is named by the place
// the operator wrote it, never by an --exclude the command line did not carry.
func TestPtahYAMLExcludeUnmatchedNamesItsSource_HappyPathE2E(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		args []string
		want string
	}{
		{
			name: "the project file",
			want: `Warning: the exclude selection from ptah.yaml matched no objects: "nosuch_table".` + "\n",
		},
		{
			name: "the project file as --config spells it, and the flag",
			args: []string{"--config", "./ptah.yaml", "--exclude", "nosuch_flag"},
			want: `Warning: the exclude selection from ./ptah.yaml matched no objects: "nosuch_table"; ` +
				`the --exclude selection matched no objects: "nosuch_flag".` + "\n",
		},
		{
			name: "the project file and the variable that set the flag",
			env:  []string{"PTAH_EXCLUDE=nosuch_env"},
			want: `Warning: the exclude selection from ptah.yaml matched no objects: "nosuch_table"; ` +
				`the exclude selection from PTAH_EXCLUDE matched no objects: "nosuch_env".` + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeYAMLExcludeFixture(c, "exclude: [nosuch_table, external_audit]\n")

			result := fixture.run(c, test.env, slices.Concat(
				[]string{"migrations", "plan", "--db-url", fixture.targetURL, "--schema-file", "schema.sql"}, test.args,
			)...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Equals, test.want)
		})
	}
}

// writeYAMLExcludeHistory writes a directory whose first migration creates
// `widgets` and whose second adds the column, applies the first to the
// target, and lets another tool create `external_audit` afterwards.
func writeYAMLExcludeHistory(c *qt.C, project string) yamlExcludeFixture {
	c.Helper()
	dir := c.TempDir()
	fixture := yamlExcludeFixture{
		dir:           dir,
		targetURL:     "sqlite://" + filepath.ToSlash(filepath.Join(dir, "target.db")),
		migrationsDir: filepath.Join(dir, "migrations"),
	}
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte(project), 0o600), qt.IsNil)
	c.Assert(os.MkdirAll(fixture.migrationsDir, 0o755), qt.IsNil)
	files := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		"0000000001_init.down.sql": "DROP TABLE widgets;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(fixture.migrationsDir, name), []byte(body), 0o600), qt.IsNil)
	}
	result := fixture.run(c, nil, "migrations", "up", "--db-url", fixture.targetURL, "--migrations-dir", fixture.migrationsDir)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	target, err := dbschema.ConnectToDatabase(c.Context(), fixture.targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	_, err = target.ExecContext(c.Context(), "CREATE TABLE external_audit (id INTEGER PRIMARY KEY);")
	c.Assert(err, qt.IsNil)
	files = map[string]string{
		"0000000002_name.up.sql":   "ALTER TABLE widgets ADD COLUMN name TEXT;\n",
		"0000000002_name.down.sql": "ALTER TABLE widgets DROP COLUMN name;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(fixture.migrationsDir, name), []byte(body), 0o600), qt.IsNil)
	}
	return fixture
}

// TestPtahYAMLExcludeMigrationsUpDriftCheck_HappyPathE2E runs `migrations up
// --check-drift`, which compares the target with the schema its applied
// history produces. The table another tool created is drift without the key,
// and the run applies nothing; with the key it is left out, and the pending
// migration runs.
func TestPtahYAMLExcludeMigrationsUpDriftCheck_HappyPathE2E(t *testing.T) {
	rows := []struct {
		name     string
		project  string
		exitCode int
		output   string
		version  string
	}{
		{
			name:     "a project file without the key",
			project:  yamlExcludeWithoutKey,
			exitCode: 1,
			output:   "extra table external_audit",
			version:  "1",
		},
		{
			name:     "the key",
			project:  yamlExcludeAudit,
			exitCode: 0,
			output:   "the database matches the schema its 1 applied migration produces",
			version:  "2",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeYAMLExcludeHistory(c, row.project)
			shadowURL := "sqlite://" + filepath.ToSlash(filepath.Join(fixture.dir, "shadow.db"))

			result := fixture.run(c, nil, "migrations", "up", "--db-url", fixture.targetURL,
				"--migrations-dir", fixture.migrationsDir, "--check-drift", "--shadow-db", shadowURL)

			c.Assert(result.ExitCode, qt.Equals, row.exitCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stdout+result.Stderr, qt.Contains, row.output)
			c.Assert(sqliteMigrationVersion(c, fixture.targetURL), qt.Equals, row.version)
		})
	}
}

// TestPtahYAMLExcludeMigrationsDownPlan_HappyPathE2E derives a rollback with
// `migrations down --plan`, which compares the target with the schema the
// directory defines at the target version. That schema has no
// `external_audit`, so the derived plan drops it unless the key leaves it out.
func TestPtahYAMLExcludeMigrationsDownPlan_HappyPathE2E(t *testing.T) {
	rows := []struct {
		name    string
		project string
		has     []string
		lacks   []string
	}{
		{
			name:    "a project file without the key",
			project: yamlExcludeWithoutKey,
			lacks:   []string{"external_audit"},
		},
		{
			name:    "the key",
			project: yamlExcludeAudit,
			has:     []string{"external_audit"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeYAMLExcludeHistory(c, row.project)
			up := fixture.run(c, nil, "migrations", "up", "--db-url", fixture.targetURL, "--migrations-dir", fixture.migrationsDir)
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			devURL := "sqlite://" + filepath.ToSlash(filepath.Join(fixture.dir, "dev.db"))

			result := fixture.run(c, nil, "migrations", "down", "--db-url", fixture.targetURL,
				"--migrations-dir", fixture.migrationsDir, "--target", "1", "--plan", "--shadow-db", devURL, "--confirm")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(sqliteMigrationVersion(c, fixture.targetURL), qt.Equals, "1")
			tables := sqliteTableNames(c, fixture.targetURL)
			c.Assert(tables, qt.Contains, "widgets")
			for _, table := range row.has {
				c.Assert(tables, qt.Contains, table)
			}
			for _, table := range row.lacks {
				c.Assert(tables, qt.Not(qt.Contains), table)
			}
		})
	}
}

// TestAtlasCompatMigrateDownPlanKeepsAnExcludedTable_HappyPathE2E derives a
// rollback with `ptah-compat migrate down --plan`, which hands the native verb
// the selected atlas.hcl env merged over ./ptah.yaml. The env without a list,
// beside a ptah.yaml without one, is the control: its rollback drops the table
// another tool created after version 1.
func TestAtlasCompatMigrateDownPlanKeepsAnExcludedTable_HappyPathE2E(t *testing.T) {
	rows := []struct {
		name  string
		env   string
		ptah  string
		has   []string
		lacks []string
	}{
		{
			name:  "an env without the list",
			env:   "plain",
			lacks: []string{"external_audit"},
		},
		{
			name: "an env that excludes the table",
			env:  "owned",
			has:  []string{"external_audit"},
		},
		{
			name: "the ptah.yaml list merged under an env without one",
			env:  "plain",
			ptah: "exclude: [external_audit]\nenv:\n  plain:\n    migration:\n      connect_timeout: 10s\n",
			has:  []string{"external_audit"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			migrations := filepath.Join(dir, "migrations")
			c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
			files := map[string]string{
				"20260101000001_init.sql": "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
				"20260101000002_name.sql": "ALTER TABLE widgets ADD COLUMN name TEXT;\n",
			}
			for name, body := range files {
				c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
			}
			targetURL := "sqlite://" + filepath.ToSlash(filepath.Join(dir, "target.db"))
			project := "env \"plain\" {\n  url = \"" + targetURL + "\"\n}\n" +
				"env \"owned\" {\n  url = \"" + targetURL + "\"\n  exclude = [\"external_audit\"]\n}\n"
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
			ptahProject := cmp.Or(row.ptah, "env:\n  plain:\n    migration:\n      connect_timeout: 10s\n  owned:\n    migration:\n      connect_timeout: 10s\n")
			c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte(ptahProject), 0o600), qt.IsNil)
			options := clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}
			hash := clirun.Run(c, clirun.Compat, options, "migrate", "hash", "--dir", "file://migrations")
			c.Assert(hash.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hash.Stderr))
			apply := clirun.Run(c, clirun.Compat, options, "migrate", "apply", "--env", row.env, "--dir", "file://migrations")
			c.Assert(apply.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", apply.Stderr))
			target, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
			c.Assert(err, qt.IsNil)
			_, err = target.ExecContext(c.Context(), "CREATE TABLE external_audit (id INTEGER PRIMARY KEY);")
			c.Assert(err, qt.IsNil)
			dbschema.CloseAndWarn(target)
			devURL := "sqlite://" + filepath.ToSlash(filepath.Join(dir, "dev.db"))

			result := clirun.Run(c, clirun.Compat, options, "migrate", "down", "--env", row.env, "--dir", "file://migrations",
				"--to-version", "20260101000001", "--plan", "--dev-url", devURL)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			tables := sqliteTableNames(c, targetURL)
			c.Assert(tables, qt.Contains, "widgets")
			for _, table := range row.has {
				c.Assert(tables, qt.Contains, table)
			}
			for _, table := range row.lacks {
				c.Assert(tables, qt.Not(qt.Contains), table)
			}
		})
	}
}

// sqliteMigrationVersion is the highest version the target's revision table
// records.
func sqliteMigrationVersion(c *qt.C, dbURL string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var version string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT CAST(MAX(version) AS TEXT) FROM schema_migrations").Scan(&version), qt.IsNil)
	return version
}

// TestPtahYAMLExcludeSchemaServe_HappyPathE2E starts `schema serve`, which
// has no --exclude of its own, and reads the drift table of the page it
// serves. Without the key the page reports a table to add and one to remove.
func TestPtahYAMLExcludeSchemaServe_HappyPathE2E(t *testing.T) {
	rows := []struct {
		name    string
		project string
		has     []string
		lacks   []string
	}{
		{
			name:    "a project file without the key",
			project: yamlExcludeWithoutKey,
			has:     []string{"columns_added", "tables_added", "tables_removed"},
		},
		{
			name:    "the key",
			project: yamlExcludeBoth,
			has:     []string{"columns_added"},
			lacks:   []string{"tables_added", "tables_removed"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeYAMLExcludeFixture(c, row.project)

			page := servedPage(c, fixture, "schema", "serve", "--db-url", fixture.targetURL,
				"--schema-file", "schema.sql", "--addr", "127.0.0.1:0", "--refresh", "0")

			for _, fragment := range row.has {
				c.Assert(page, qt.Contains, fragment)
			}
			for _, fragment := range row.lacks {
				c.Assert(page, qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// servedPage starts the shipped ptah with args, reads the address it prints,
// fetches the page once and stops the server.
func servedPage(c *qt.C, fixture yamlExcludeFixture, args ...string) string {
	c.Helper()
	// #nosec G204 -- the test runs the ptah binary clirun built from this checkout
	cmd := exec.CommandContext(c.Context(), clirun.Build(c, clirun.Ptah), args...)
	cmd.Dir = fixture.dir
	cmd.Env = environmentWithoutPtahVariables()
	stdout, err := cmd.StdoutPipe()
	c.Assert(err, qt.IsNil)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	c.Assert(cmd.Start(), qt.IsNil)
	c.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	lines := bufio.NewScanner(stdout)
	c.Assert(lines.Scan(), qt.IsTrue, qt.Commentf("stderr:\n%s", stderr.String()))
	_, address, found := strings.Cut(lines.Text(), "Serving a read-only schema view on ")
	c.Assert(found, qt.IsTrue, qt.Commentf("first line: %q", lines.Text()))
	client := &http.Client{Timeout: 30 * time.Second}
	request, err := http.NewRequestWithContext(c.Context(), http.MethodGet, address+"/", nil)
	c.Assert(err, qt.IsNil)
	response, err := client.Do(request)
	c.Assert(err, qt.IsNil)
	defer response.Body.Close()
	c.Assert(response.StatusCode, qt.Equals, http.StatusOK)
	body, err := io.ReadAll(response.Body)
	c.Assert(err, qt.IsNil)
	return string(body)
}

// The PostgreSQL fixture is the SQLite one in schema public. A selector names
// a table bare, in the connection's default schema, or qualified by its
// schema; the key carries one of each.

const (
	yamlExcludePostgresDesiredSQL = "CREATE TABLE widgets (id bigint PRIMARY KEY, name text);\n" +
		"CREATE TABLE reporting_cache (id bigint PRIMARY KEY);\n"
	yamlExcludePostgresTargetDDL = "CREATE TABLE widgets (id bigint PRIMARY KEY);\n" +
		"CREATE TABLE external_audit (id bigint PRIMARY KEY);\n"
)

// TestPtahYAMLExcludeOnPostgreSQL_HappyPathE2E applies the plan and reads
// the tables of schema public back, then generates a migration against a
// second target and reads the file. The run without the key is the control.
func TestPtahYAMLExcludeOnPostgreSQL_HappyPathE2E(t *testing.T) {
	rows := []struct {
		name     string
		project  string
		tables   []string
		upHas    []string
		upLacks  []string
		upAlways string
	}{
		{
			name:     "a project file without the key",
			project:  yamlExcludeWithoutKey,
			tables:   []string{"reporting_cache", "widgets"},
			upHas:    []string{`CREATE TABLE "reporting_cache"`, `DROP TABLE "external_audit"`},
			upAlways: `ADD COLUMN "name" text`,
		},
		{
			name:     "the key",
			project:  "exclude: [public.external_audit, reporting_cache]\n",
			tables:   []string{"external_audit", "widgets"},
			upLacks:  []string{"reporting_cache", "external_audit"},
			upAlways: `ADD COLUMN "name" text`,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte(row.project), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(yamlExcludePostgresDesiredSQL), 0o600), qt.IsNil)
			env := environmentWithoutPtahVariables()
			applyTarget := yamlExcludePostgresTarget(c)
			generateTarget := yamlExcludePostgresTarget(c)
			migrationsDir := filepath.Join(dir, "migrations")
			c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

			apply := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: env},
				"schema", "apply", "--db-url", applyTarget, "--schema-file", "schema.sql", "--auto-approve")
			generate := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: env},
				"migrations", "generate", "--db-url", generateTarget, "--schema-file", "schema.sql",
				"--migrations-dir", migrationsDir, "--name", "second")

			c.Assert(apply.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", apply.Stdout, apply.Stderr))
			c.Assert(postgresPublicTables(c, applyTarget), qt.DeepEquals, row.tables)
			c.Assert(generate.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generate.Stdout, generate.Stderr))
			up, down := generatedMigration(c, migrationsDir)
			c.Assert(up, qt.Contains, row.upAlways)
			for _, fragment := range row.upHas {
				c.Assert(up, qt.Contains, fragment)
			}
			for _, fragment := range row.upLacks {
				c.Assert(up, qt.Not(qt.Contains), fragment)
				c.Assert(down, qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// yamlExcludePostgresTarget is a scratch database holding the fixture.
func yamlExcludePostgresTarget(c *qt.C) string {
	c.Helper()
	targetURL := postgresScratchDevURL(c, "ptah_yaml_exclude")
	target, err := sql.Open("pgx", targetURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(target.Close(), qt.IsNil) }()
	_, err = target.ExecContext(c.Context(), yamlExcludePostgresTargetDDL)
	c.Assert(err, qt.IsNil)
	return targetURL
}

// postgresPublicTables lists the base tables of schema public, without the
// apply log `schema apply` keeps there.
func postgresPublicTables(c *qt.C, dbURL string) []string {
	c.Helper()
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), "SELECT table_name FROM information_schema.tables "+
		"WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name <> 'ptah_schema_apply_log' ORDER BY table_name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}
