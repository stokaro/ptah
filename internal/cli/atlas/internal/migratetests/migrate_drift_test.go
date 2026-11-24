package migratetests_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/internal/cli/internal/exitcode"
)

// These tests run `migrate drift` on this surface, which forwards to `ptah
// migrations drift` when PTAH_ATLAS_MIGRATE_DRIFT asks for it. The targets are
// SQLite files a `migrate apply` over the same env migrated, so the report
// reads the Atlas revision table that verb wrote.

const driftVariable = "PTAH_ATLAS_MIGRATE_DRIFT"

// driftProject declares a two-target env, an env without for_each, the same
// env naming no directory, a for_each env of one target, and a two-target env
// whose targets ask for two lock timeouts.
const driftProject = `env "tenants" {
  for_each = toset(["a", "b"])
  url      = "sqlite://${each.value}.db"
  dev      = "sqlite://dev?mode=memory"
  migration {
    dir = "file://migrations"
  }
}

env "local" {
  url = "sqlite://a.db"
  dev = "sqlite://dev?mode=memory"
  migration {
    dir = "file://migrations"
  }
}

env "bare" {
  url = "sqlite://a.db"
  dev = "sqlite://dev?mode=memory"
}

env "single" {
  for_each = toset(["a"])
  url      = "sqlite://${each.value}.db"
  dev      = "sqlite://dev?mode=memory"
  migration {
    dir = "file://migrations"
  }
}

env "timeouts" {
  for_each = toset(["a", "b"])
  url      = "sqlite://${each.value}.db"
  dev      = "sqlite://dev?mode=memory"
  migration {
    dir          = "file://migrations"
    lock_timeout = each.value == "a" ? "1s" : "2s"
  }
}
`

// splitProject declares an env whose two targets each name a directory of
// their own.
const splitProject = `env "split" {
  for_each = toset(["a", "b"])
  url      = "sqlite://${each.value}.db"
  dev      = "sqlite://dev?mode=memory"
  migration {
    dir = "file://migrations-${each.value}"
  }
}
`

// cleanTarget is the report on a target that matches its two applied
// migrations, after the line naming it.
const cleanTarget = `Dialect: sqlite
Revision table: "atlas_schema_revisions"
Current version: 20240102000000
Pending migrations: 0

No drift: the database matches the schema its 2 applied migrations produce.
`

// driftedTarget is the report on a target that holds a table its migrations
// do not create.
const driftedTarget = `Dialect: sqlite
Revision table: "atlas_schema_revisions"
Current version: 20240102000000
Pending migrations: 0

Drift: the database differs from the schema its 2 applied migrations produce:
  - extra table extra

Statements that would bring the database back to its history (review them; nothing ran):
DROP TABLE "extra";
`

const oneDatabaseForEveryTarget = `--url or PTAH_URL sets one database for every target, and env "tenants" names 2 targets ` +
	`with a url each; leave both unset to report each target`

// twoTargets is the report on a.db and b.db of the env "tenants", both clean.
const twoTargets = `Instance 1 of env "tenants": sqlite://a.db
` + cleanTarget + `
Instance 2 of env "tenants": sqlite://b.db
` + cleanTarget + `
Summary: 2 targets: 2 without drift, 0 with drift, 0 not compared.
`

// writeDriftProject writes driftProject into projectDir with a hashed
// migration directory beside it, migrates a.db and b.db in the working
// directory through the env "tenants", and returns the path of the project
// file.
func writeDriftProject(c *qt.C, projectDir string) string {
	c.Helper()
	dir := filepath.Join(projectDir, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_one.sql"),
		[]byte("CREATE TABLE t1 (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20240102000000_two.sql"),
		[]byte("CREATE TABLE t2 (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
	_, stderr, err := atlastest.RunCompatStreams(c, "migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	project := filepath.Join(projectDir, "atlas.hcl")
	c.Assert(os.WriteFile(project, []byte(driftProject), 0o600), qt.IsNil)
	config := "file://" + filepath.ToSlash(project)
	_, stderr, err = atlastest.RunCompatStreams(c, "migrate", "apply", "-c", config, "--env", "tenants")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	return config
}

// enterDriftProject moves the test into a fresh directory holding the project
// and returns the project file's address.
func enterDriftProject(c *qt.C) string {
	c.Helper()
	root := c.TempDir()
	c.Chdir(root)
	return writeDriftProject(c, root)
}

// writeMigrations writes files into a new directory dir and hashes it.
func writeMigrations(c *qt.C, dir string, files map[string]string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, stderr, err := atlastest.RunCompatStreams(c, "migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
}

// enterSplitProject moves the test into a fresh directory, writes
// splitProject into a project directory below it with migrations-a holding
// two migrations and migrations-b one, migrates a.db and b.db in the working
// directory through the env "split", and returns the project file's address.
func enterSplitProject(c *qt.C) string {
	c.Helper()
	root := c.TempDir()
	c.Chdir(root)
	projectDir := filepath.Join(root, "project")
	one := map[string]string{"20240101000000_one.sql": "CREATE TABLE t1 (id integer PRIMARY KEY);\n"}
	writeMigrations(c, filepath.Join(projectDir, "migrations-a"), map[string]string{
		"20240101000000_one.sql": one["20240101000000_one.sql"],
		"20240102000000_two.sql": "CREATE TABLE t2 (id integer PRIMARY KEY);\n",
	})
	writeMigrations(c, filepath.Join(projectDir, "migrations-b"), one)
	project := filepath.Join(projectDir, "atlas.hcl")
	c.Assert(os.WriteFile(project, []byte(splitProject), 0o600), qt.IsNil)
	config := "file://" + filepath.ToSlash(project)
	_, stderr, err := atlastest.RunCompatStreams(c, "migrate", "apply", "-c", config, "--env", "split")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	return config
}

func TestCompatMigrateDrift_HappyPathReportsEveryTargetOfAnEnv(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "tenants")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, twoTargets)
	c.Assert(stderr, qt.Equals, "")
}

// Each target reads the directory its own instance names, against the
// project file's directory, and b.db reports the one migration its directory
// holds. Read against migrations-a, it would report a pending migration.
func TestCompatMigrateDrift_HappyPathReadsTheDirectoryEachTargetNames(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterSplitProject(c)

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "split")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, `Instance 1 of env "split": sqlite://a.db
`+cleanTarget+`
Instance 2 of env "split": sqlite://b.db
Dialect: sqlite
Revision table: "atlas_schema_revisions"
Current version: 20240101000000
Pending migrations: 0

No drift: the database matches the schema its 1 applied migration produces.

Summary: 2 targets: 2 without drift, 0 with drift, 0 not compared.
`)
	c.Assert(stderr, qt.Equals, "")
}

// --dir names one directory, which every target reads in place of its own.
func TestCompatMigrateDrift_HappyPathReadsTheDirectoryOfTheFlagForEveryTarget(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterSplitProject(c)

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "split",
		"--dir", "file://project/migrations-a")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(strings.Count(stdout, "Pending migrations: 0\n"), qt.Equals, 1)
	c.Assert(stdout, qt.Contains, `Instance 2 of env "split": sqlite://b.db
Dialect: sqlite
Revision table: "atlas_schema_revisions"
Current version: 20240101000000
Pending migrations: 1
`)
}

// The report reads the database and the history out of what this surface
// forwards. A variable of a native flag this surface refuses changes nothing:
// PTAH_JSON would print the native document, and the others would point every
// target at one dev database, directory or revision table. The project sits
// below the working directory, so a directory read against the working
// directory is not found.
func TestCompatMigrateDrift_HappyPathReadsNoNativeVariable(t *testing.T) {
	variables := []struct {
		name, value string
	}{
		{name: "PTAH_JSON", value: "1"},
		{name: "PTAH_SHADOW_DB", value: "sqlite:///nonexistent/dev.db"},
		{name: "PTAH_MIGRATIONS_DIR", value: "nonexistent"},
		{name: "PTAH_MIGRATIONS_TABLE", value: "other_revisions"},
		{name: "PTAH_REVISION_FORMAT", value: "ptah"},
	}
	for _, variable := range variables {
		t.Run(variable.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(driftVariable, "1")
			root := c.TempDir()
			c.Chdir(root)
			config := writeDriftProject(c, filepath.Join(root, "project"))
			c.Setenv(variable.name, variable.value)

			stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "tenants")

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Equals, twoTargets)
		})
	}
}

// An env that names no directory reads the documented default, file://migrations,
// even with PTAH_MIGRATIONS_DIR set: the target reads no variable of its own,
// so the variable must not hold the default back either.
func TestCompatMigrateDrift_HappyPathReadsTheDefaultDirectoryBesideTheNativeVariable(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)
	c.Setenv("PTAH_MIGRATIONS_DIR", "nonexistent")

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "bare")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, cleanTarget)
}

// A setting the report refuses is refused only in the env the run selects:
// another env's is that env's business.
func TestCompatMigrateDrift_HappyPathLeavesTheSettingsOfAnotherEnvAlone(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	enterDriftProject(c)
	c.Assert(os.WriteFile("settings.hcl", []byte(`env "local" {
  url = "sqlite://a.db"
  dev = "sqlite://dev?mode=memory"
  migration {
    dir = "file://migrations"
  }
}

env "other" {
  url = "sqlite://b.db"
  migration {
    skip_lock = true
  }
}
`), 0o600), qt.IsNil)

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", "file://settings.hcl", "--env", "local")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, cleanTarget)
}

// The migration directory of a project file elsewhere is read against that
// file's directory, as a `migrate apply` over the same env reads it. The
// working directory holds no migrations.
func TestCompatMigrateDrift_HappyPathReadsTheDirectoryBesideTheProjectFile(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	root := c.TempDir()
	c.Chdir(root)
	config := writeDriftProject(c, filepath.Join(root, "project"))

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "tenants")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, `Instance 1 of env "tenants": sqlite://a.db
`+cleanTarget+`
Instance 2 of env "tenants": sqlite://b.db
`+cleanTarget+`
Summary: 2 targets: 2 without drift, 0 with drift, 0 not compared.
`)
	_, err = os.Stat(filepath.Join(root, "migrations"))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

// --url over an env without for_each is the one database the run reads, so
// it is not refused. The env's own url names the drifted database.
func TestCompatMigrateDrift_HappyPathTakesAURLFlagForOneEnv(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)
	atlastest.SqliteExec(c, "a.db", "CREATE TABLE extra (id integer)")

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "local",
		"--url", "sqlite://b.db")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, cleanTarget)
}

// PTAH_URL reaches a run over an env without for_each, as --url does.
func TestCompatMigrateDrift_HappyPathTakesAURLVariableForOneEnv(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)
	atlastest.SqliteExec(c, "a.db", "CREATE TABLE extra (id integer)")
	c.Setenv("PTAH_URL", "sqlite://b.db")

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "local")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, cleanTarget)
}

// --no-cache asks for an expected state computed again, which every run
// computes, so the run is the one without it.
func TestCompatMigrateDrift_HappyPathTakesNoCache(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "local", "--no-cache")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, cleanTarget)
}

// Unset or false, the variable leaves `migrate drift` the group help the
// community binary prints, and a flag after it is refused as it refuses one.
// Without the variable, every spelling of `migrate drift` answers as the same
// spelling with a word the group does not register: the tree without the
// verb, which is what the community binary answers.
func TestCompatMigrateDrift_HappyPathAnswersAsAnUnregisteredWordWithoutTheVariable(t *testing.T) {
	spellings := []struct {
		name          string
		before, after []string
	}{
		{name: "the word alone", before: []string{"migrate"}},
		{name: "an Atlas drift flag", before: []string{"migrate"}, after: []string{"--url", "sqlite://a.db"}},
		{name: "a help flag", before: []string{"migrate"}, after: []string{"--help"}},
		{name: "a short help flag", before: []string{"migrate"}, after: []string{"-h"}},
		{name: "the help command", before: []string{"help", "migrate"}},
		{name: "more words", before: []string{"migrate"}, after: []string{"extra", "words"}},
		{name: "a group flag", before: []string{"migrate"}, after: []string{"--env", "nope"}},
		{name: "flag completion", before: []string{"__complete", "migrate"}, after: []string{"--e"}},
	}
	// set is what the variable is set to, in order; an unset variable sets
	// nothing.
	values := []struct {
		name string
		set  []string
	}{
		{name: "unset"},
		{name: "false", set: []string{"false"}},
		{name: "zero", set: []string{"0"}},
	}
	for _, value := range values {
		for _, spelling := range spellings {
			t.Run(value.name+"/"+spelling.name, func(t *testing.T) {
				c := qt.New(t)
				c.Setenv(driftVariable, "")
				c.Assert(os.Unsetenv(driftVariable), qt.IsNil)
				for _, set := range value.set {
					c.Setenv(driftVariable, set)
				}
				c.Chdir(c.TempDir())
				word := append(append(slices.Clone(spelling.before), "unregistered"), spelling.after...)
				drift := append(append(slices.Clone(spelling.before), "drift"), spelling.after...)
				wantOut, wantErrOut, wantErr := atlastest.RunCompatStreams(c, word...)

				gotOut, gotErrOut, gotErr := atlastest.RunCompatStreams(c, drift...)

				c.Assert(gotOut, qt.Equals, wantOut)
				c.Assert(gotErrOut, qt.Equals, wantErrOut)
				c.Assert(fmt.Sprint(gotErr), qt.Equals, fmt.Sprint(wantErr))
				c.Assert(exitcode.Code(gotErr, 0), qt.Equals, exitcode.Code(wantErr, 0))
			})
		}
	}
}

// Unset is the default, and the group help lists no drift verb.
func TestCompatMigrateDrift_HappyPathPrintsTheGroupHelpWithTheVariableUnset(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "")
	c.Assert(os.Unsetenv(driftVariable), qt.IsNil)
	want, _, wantErr := atlastest.RunCompatStreams(c, "migrate")
	_, _, wantFlagErr := atlastest.RunCompatStreams(c, "migrate", "--url", "sqlite://a.db")

	got, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift")
	_, _, flagErr := atlastest.RunCompatStreams(c, "migrate", "drift", "--url", "sqlite://a.db")

	c.Assert(wantErr, qt.IsNil)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, want)
	c.Assert(got, qt.Not(qt.Contains), "drift")
	c.Assert(stderr, qt.Equals, "")
	c.Assert(flagErr, qt.ErrorMatches, "unknown flag: --url")
	c.Assert(flagErr.Error(), qt.Equals, wantFlagErr.Error())
}

// With the variable, the help command prints the verb's own help.
func TestCompatMigrateDrift_HappyPathPrintsTheVerbHelpWithTheVariable(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")

	viaCommand, _, commandErr := atlastest.RunCompatStreams(c, "help", "migrate", "drift")
	viaFlag, _, flagErr := atlastest.RunCompatStreams(c, "migrate", "drift", "--help")

	c.Assert(commandErr, qt.IsNil)
	c.Assert(flagErr, qt.IsNil)
	c.Assert(viaCommand, qt.Contains, "Forwards to `ptah migrations drift`.")
	c.Assert(viaCommand, qt.Contains, "  atlas migrate drift [flags]\n")
	c.Assert(viaFlag, qt.Equals, viaCommand)
}

// The directory data.template_dir renders is the one every target reads: the
// adapter hands the rendered directory to the native report, which finds no
// such directory on disk. Neither database ran a migration, so both report the
// two the directory holds as pending.
func TestCompatMigrateDrift_HappyPathReadsATemplateDirectoryForEveryTarget(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	root := c.TempDir()
	c.Chdir(root)
	templates := filepath.Join(root, "templates")
	c.Assert(os.Mkdir(templates, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(templates, "20240101000000_one.sql"),
		[]byte("CREATE TABLE t1 (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(templates, "20240102000000_two.sql"),
		[]byte("CREATE TABLE t2 (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(`data "template_dir" "migrations" {
  path = "templates"
  vars = {}
}

env "tenants" {
  for_each = toset(["a", "b"])
  url      = "sqlite://${each.value}.db"
  dev      = "sqlite://dev?mode=memory"
  migration {
    dir = data.template_dir.migrations.url
  }
}
`), 0o600), qt.IsNil)

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "--env", "tenants")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(strings.Count(stdout, "Pending migrations: 2\n"), qt.Equals, 2)
	c.Assert(stdout, qt.Contains, "Summary: 2 targets: 2 without drift, 0 with drift, 0 not compared.\n")
}

// A drifted target exits 1 beside a clean one, and both are reported.
func TestCompatMigrateDrift_FailurePathReportsADriftedTarget(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)
	atlastest.SqliteExec(c, "b.db", "CREATE TABLE extra (id integer)")

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "tenants")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, "1 of 2 targets differ from the schema their applied migrations produce")
	c.Assert(stdout, qt.Equals, `Instance 1 of env "tenants": sqlite://a.db
`+cleanTarget+`
Instance 2 of env "tenants": sqlite://b.db
`+driftedTarget+`
Summary: 2 targets: 1 without drift, 1 with drift, 0 not compared.
`)
	c.Assert(stderr, qt.Equals, "")
}

// PTAH_DB_URL is the native verb's variable, and this surface reads the
// database out of --url or PTAH_URL. Over an env with for_each the variable
// does not replace the url of every target: the drifted target is reported.
func TestCompatMigrateDrift_FailurePathReportsEachTargetBesideTheNativeVariable(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)
	atlastest.SqliteExec(c, "b.db", "CREATE TABLE extra (id integer)")
	c.Setenv("PTAH_DB_URL", "sqlite://a.db")

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "tenants")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stdout, qt.Contains, `Instance 2 of env "tenants": sqlite://b.db
`+driftedTarget)
	c.Assert(stderr, qt.Equals, "")
}

func TestCompatMigrateDrift_FailurePathRefusesAURLFlagOverTargets(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)
	atlastest.SqliteExec(c, "b.db", "CREATE TABLE extra (id integer)")

	stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "tenants",
		"--url", "sqlite://a.db")

	c.Assert(err, qt.ErrorMatches, oneDatabaseForEveryTarget)
	c.Assert(stdout, qt.Equals, "")
}

func TestCompatMigrateDrift_FailurePathRefusesAURLVariableOverTargets(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)
	atlastest.SqliteExec(c, "b.db", "CREATE TABLE extra (id integer)")
	c.Setenv("PTAH_URL", "sqlite://a.db")

	stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "tenants")

	c.Assert(err, qt.ErrorMatches, oneDatabaseForEveryTarget)
	c.Assert(stdout, qt.Equals, "")
}

// One --lock-timeout is forwarded for every target, so targets that ask for
// two are refused rather than each given the first one's.
func TestCompatMigrateDrift_FailurePathRefusesTargetsThatDisagreeOnALockTimeout(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)

	stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "timeouts")

	c.Assert(err, qt.ErrorMatches, `the targets of env "timeouts" do not agree on --lock-timeout, which this surface `+
		`forwards once for every target; give them one value or pass --lock-timeout`)
	c.Assert(stdout, qt.Equals, "")
}

// A for_each env of one element is a list of targets too, and the refusal
// names the one.
func TestCompatMigrateDrift_FailurePathRefusesAURLFlagOverOneInstance(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)

	stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "single",
		"--url", "sqlite://b.db")

	c.Assert(err, qt.ErrorMatches, `--url or PTAH_URL sets one database for the target, and env "single" names `+
		`one target with a url of its own; leave both unset to report that target`)
	c.Assert(stdout, qt.Equals, "")
}

// An empty --url names no database, and is refused as set.
func TestCompatMigrateDrift_FailurePathRefusesAnEmptyURLOverTargets(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)

	stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "tenants", "--url", "")

	c.Assert(err, qt.ErrorMatches, `--url or PTAH_URL is set, though empty, and env "tenants" names 2 targets `+
		`with a url each; leave both unset to report each target`)
	c.Assert(stdout, qt.Equals, "")
}

// The settings the Atlas drift documentation gives an effect, and this
// surface does not carry out, are refused where the project file sets them,
// as the flags asking for the same are.
func TestCompatMigrateDrift_FailurePathRefusesASettingItDoesNotCarryOut(t *testing.T) {
	settings := []struct {
		name, body, wantErr string
	}{
		{
			name: "migration.skip_lock",
			body: "migration {\n    dir       = \"file://migrations\"\n    skip_lock = true\n  }",
			wantErr: `atlas migrate drift reads the env's migration.skip_lock, set at settings.hcl:6, but Ptah does ` +
				`not implement its behavior: the report reads the revision table under the migration lock, so that ` +
				`a migration being applied is not reported as drift`,
		},
		{
			name: "migration.exclude",
			body: "migration {\n    dir     = \"file://migrations\"\n    exclude = [\"main.t2\"]\n  }",
			wantErr: `atlas migrate drift reads the env's migration.exclude, set at settings.hcl:6, but Ptah does ` +
				`not read it yet; pass the patterns with --exclude, which takes precedence`,
		},
		{
			name: "migration.lock_name",
			body: "migration {\n    dir       = \"file://migrations\"\n    lock_name = \"custom\"\n  }",
			wantErr: `atlas migrate drift reads the env's migration.lock_name, set at settings.hcl:6, but Ptah does ` +
				`not read it yet; pass the name with --lock-name, which takes precedence`,
		},
		{
			name: "format.migrate.drift",
			body: "migration {\n    dir = \"file://migrations\"\n  }\n  format {\n    migrate {\n" +
				"      drift = \"{{ json . }}\"\n    }\n  }",
			wantErr: `atlas migrate drift reads the env's format.migrate.drift, set at settings.hcl:9, but Ptah ` +
				`does not implement its behavior: Ptah prints its own drift report, and has no template data in the ` +
				`shape Atlas documents for it`,
		},
	}
	for _, setting := range settings {
		t.Run(setting.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(driftVariable, "1")
			enterDriftProject(c)
			c.Assert(os.WriteFile("settings.hcl", []byte("env \"local\" {\n  url = \"sqlite://a.db\"\n"+
				"  dev = \"sqlite://dev?mode=memory\"\n  "+setting.body+"\n}\n"), 0o600), qt.IsNil)

			stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", "file://settings.hcl",
				"--env", "local")

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(setting.wantErr))
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// writeDriftSettings writes settings.hcl, an env "local" over a.db whose
// migration block holds migration, and returns its address.
func writeDriftSettings(c *qt.C, migration string) string {
	c.Helper()
	c.Assert(os.WriteFile("settings.hcl", []byte("env \"local\" {\n  url = \"sqlite://a.db\"\n"+
		"  dev = \"sqlite://dev?mode=memory\"\n  migration {\n    dir = \"file://migrations\"\n"+
		migration+"  }\n}\n"), 0o600), qt.IsNil)
	return "file://settings.hcl"
}

// A flag takes precedence over the setting of the same meaning, as an Atlas
// flag overrides an env attribute: the setting is not refused, and the flag's
// value is what runs. The setting names no table here and the flag names the
// table that drifted, so the report is clean.
func TestCompatMigrateDrift_HappyPathRunsTheFlagOverTheSetting(t *testing.T) {
	settings := []struct {
		name, migration string
		args            []string
		env             map[string]string
	}{
		{
			name:      "exclude",
			migration: "    exclude = [\"nothing\"]\n",
			args:      []string{"--exclude", "extra"},
		},
		{
			name:      "the variable of exclude",
			migration: "    exclude = [\"nothing\"]\n",
			env:       map[string]string{"PTAH_EXCLUDE": "extra"},
		},
		{
			name:      "lock_name",
			migration: "    lock_name = \"custom\"\n",
			args:      []string{"--exclude", "extra", "--lock-name", "other"},
		},
	}
	for _, setting := range settings {
		t.Run(setting.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(driftVariable, "1")
			for name, value := range setting.env {
				c.Setenv(name, value)
			}
			enterDriftProject(c)
			atlastest.SqliteExec(c, "a.db", "CREATE TABLE extra (id integer)")
			config := writeDriftSettings(c, setting.migration)

			stdout, stderr, err := atlastest.RunCompatStreams(c,
				append([]string{"migrate", "drift", "-c", config, "--env", "local"}, setting.args...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Equals, cleanTarget)
		})
	}
}

// The setting is not what runs beside the flag: it names the table that
// drifted, the flag names none, and the drift is reported.
func TestCompatMigrateDrift_FailurePathReportsWhatOnlyTheSettingExcludes(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	enterDriftProject(c)
	atlastest.SqliteExec(c, "a.db", "CREATE TABLE extra (id integer)")
	config := writeDriftSettings(c, "    exclude = [\"extra\"]\n")

	stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "local",
		"--exclude", "nothing")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stdout, qt.Equals, driftedTarget)
}

// skip_lock = false asks for the lock the report takes anyway, so it is not
// refused.
func TestCompatMigrateDrift_HappyPathAcceptsASkipLockOfFalse(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	enterDriftProject(c)
	config := writeDriftSettings(c, "    skip_lock = false\n")

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "local")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, cleanTarget)
	// The run takes the lock the setting asks for, so the warning that a
	// tolerated attribute has no effect does not name it.
	c.Assert(stderr, qt.Not(qt.Contains), "skip_lock")
}

// A skip_lock the targets of an env disagree on asks one of them to skip the
// lock, and is refused.
func TestCompatMigrateDrift_FailurePathRefusesASkipLockTheTargetsDisagreeOn(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	enterDriftProject(c)
	c.Assert(os.WriteFile("settings.hcl", []byte(`env "tenants" {
  for_each = toset(["a", "b"])
  url      = "sqlite://${each.value}.db"
  dev      = "sqlite://dev?mode=memory"
  migration {
    dir       = "file://migrations"
    skip_lock = each.value == "b"
  }
}
`), 0o600), qt.IsNil)

	stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", "file://settings.hcl", "--env", "tenants")

	c.Assert(err, qt.ErrorMatches, `atlas migrate drift reads the env's migration.skip_lock, set at settings.hcl:7, `+
		`but the instances of the env give it different values; give it one value in every instance`)
	c.Assert(stdout, qt.Equals, "")
}

func TestCompatMigrateDrift_FailurePathRefusesWhatItDoesNotImplement(t *testing.T) {
	t.Run("format", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv(driftVariable, "1")
		config := enterDriftProject(c)

		stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "local",
			"--format", "{{ json . }}")

		c.Assert(err, qt.ErrorMatches, `atlas migrate drift accepts --format, but Ptah does not implement its `+
			`behavior: Ptah prints its own drift report, and has no template data in the shape Atlas documents for it`)
		c.Assert(stdout, qt.Equals, "")
	})
	t.Run("skip-lock", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv(driftVariable, "1")
		config := enterDriftProject(c)

		stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "local", "--skip-lock")

		c.Assert(err, qt.ErrorMatches, `atlas migrate drift accepts --skip-lock, but Ptah does not implement its `+
			`behavior: the report reads the revision table under the migration lock, so that a migration being `+
			`applied is not reported as drift`)
		c.Assert(stdout, qt.Equals, "")
	})
	t.Run("a no-cache value that is not a boolean", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv(driftVariable, "1")
		config := enterDriftProject(c)

		stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "local",
			"--no-cache=maybe")

		c.Assert(err, qt.ErrorMatches, `invalid argument "maybe" for "--no-cache" flag: .*`)
		c.Assert(stdout, qt.Equals, "")
	})
	t.Run("the native json flag", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv(driftVariable, "1")
		config := enterDriftProject(c)

		stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "local", "--json")

		c.Assert(err, qt.ErrorMatches, `atlas migrate drift does not accept native Ptah flag --json`)
		c.Assert(stdout, qt.Equals, "")
	})
}

// A value that is not a boolean fails `migrate drift` and leaves the verbs
// that do not read the variable alone.
func TestCompatMigrateDrift_FailurePathRefusesAMalformedVariable(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "yes")
	root := c.TempDir()
	c.Chdir(root)

	stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "--url", "sqlite://a.db")
	helpOut, _, helpErr := atlastest.RunCompatStreams(c, "help", "migrate", "drift")
	flagOut, _, flagErr := atlastest.RunCompatStreams(c, "migrate", "drift", "--help")
	_, hashStderr, hashErr := atlastest.RunCompatStreams(c, "migrate", "hash", "--dir", "file://"+filepath.ToSlash(root))

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "yes" for PTAH_ATLAS_MIGRATE_DRIFT`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(helpErr, qt.ErrorMatches, `invalid boolean value "yes" for PTAH_ATLAS_MIGRATE_DRIFT`)
	c.Assert(helpOut, qt.Equals, "")
	c.Assert(flagErr, qt.ErrorMatches, `invalid boolean value "yes" for PTAH_ATLAS_MIGRATE_DRIFT`)
	c.Assert(flagOut, qt.Equals, "")
	c.Assert(hashErr, qt.IsNil, qt.Commentf("%s", hashStderr))
}

// A blank lock name, on the command line or in PTAH_LOCK_NAME, is refused
// before the report runs, even beside a migration.lock_name the flag would
// take precedence over. Read as unset it would take the default lock, and a
// runner holding the named one would not exclude this report.
func TestCompatMigrateDrift_FailurePathRefusesABlankLockName(t *testing.T) {
	tests := []struct {
		name, migration string
		args            []string
		env             map[string]string
		wantErr         string
	}{
		{
			name:    "the flag",
			args:    []string{"--lock-name", " "},
			wantErr: `--lock-name must not be empty`,
		},
		{
			name:      "the variable beside the setting",
			migration: "    lock_name = \"custom\"\n",
			env:       map[string]string{"PTAH_LOCK_NAME": ""},
			wantErr:   `PTAH_LOCK_NAME is set but empty.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(driftVariable, "1")
			enterDriftProject(c)
			config := writeDriftSettings(c, test.migration)
			for name, value := range test.env {
				c.Setenv(name, value)
			}

			stdout, _, err := atlastest.RunCompatStreams(c,
				append([]string{"migrate", "drift", "-c", config, "--env", "local"}, test.args...)...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}
