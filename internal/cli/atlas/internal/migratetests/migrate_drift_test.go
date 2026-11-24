package migratetests_test

import (
	"fmt"
	"os"
	"path/filepath"
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

// driftProject declares a two-target env, a one-target env and a two-target
// env whose targets name two directories.
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

env "split" {
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

func TestCompatMigrateDrift_HappyPathReportsEveryTargetOfAnEnv(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)

	stdout, stderr, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "tenants")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, `Instance 1 of env "tenants": sqlite://a.db
`+cleanTarget+`
Instance 2 of env "tenants": sqlite://b.db
`+cleanTarget+`
Summary: 2 targets: 2 without drift, 0 with drift, 0 not compared.
`)
	c.Assert(stderr, qt.Equals, "")
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
	values := []struct {
		name, value string
	}{
		{name: "false", value: "false"},
		{name: "zero", value: "0"},
	}
	for _, value := range values {
		for _, spelling := range spellings {
			t.Run(value.name+"/"+spelling.name, func(t *testing.T) {
				c := qt.New(t)
				c.Setenv(driftVariable, value.value)
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

// One --dir is forwarded for every target, so targets naming two
// directories are refused rather than read against the first one's.
func TestCompatMigrateDrift_FailurePathRefusesTargetsThatNameTwoDirectories(t *testing.T) {
	c := qt.New(t)
	c.Setenv(driftVariable, "1")
	config := enterDriftProject(c)

	stdout, _, err := atlastest.RunCompatStreams(c, "migrate", "drift", "-c", config, "--env", "split")

	c.Assert(err, qt.ErrorMatches, `the targets of env "split" do not agree on --dir, which the drift report `+
		`takes once for every target; give them one value or pass --dir`)
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
