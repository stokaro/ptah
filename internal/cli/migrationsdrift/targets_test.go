package migrationsdrift_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrationsdrift"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/errorkind"
	"ptah.run/migration/migrator"
)

// forEachProject writes an atlas.hcl whose env "tenants" expands to one
// target per url, each replayed on shadow, and returns its path.
func forEachProject(c *qt.C, shadow string, urls ...string) string {
	c.Helper()
	return projectWith(c, "", forEachEnv(shadow, "", urls...))
}

// forEachEnv is an env block that expands to one target per url, each
// replayed on shadow. migration is the body of the env's migration block,
// empty for none.
func forEachEnv(shadow, migration string, urls ...string) string {
	quoted := make([]string, len(urls))
	for i, url := range urls {
		quoted[i] = strconv.Quote(url)
	}
	return "env {\n" +
		"  for_each = [" + strings.Join(quoted, ", ") + "]\n" +
		"  name     = atlas.env\n" +
		"  url      = each.value\n" +
		"  dev      = " + strconv.Quote(shadow) + "\n" +
		migration +
		"}\n"
}

// projectWith writes an atlas.hcl into a directory of its own, beside a
// templates directory that holds driftMigrations, and returns its path.
// header comes before env.
func projectWith(c *qt.C, header, env string) string {
	c.Helper()
	dir := c.TempDir()
	templates := filepath.Join(dir, "templates")
	c.Assert(os.Mkdir(templates, 0o750), qt.IsNil)
	for name, content := range driftMigrations {
		c.Assert(os.WriteFile(filepath.Join(templates, name), []byte(content), 0o600), qt.IsNil)
	}
	path := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(path, []byte(header+env), 0o600), qt.IsNil)
	return path
}

// templateDir is the atlas.hcl data source that renders the templates
// directory projectWith writes, and templateMigration the migration block that
// reads it.
const (
	templateDir       = "data \"template_dir\" \"migrations\" {\n  path = \"templates\"\n  vars = {}\n}\n"
	templateMigration = "  migration {\n    dir = data.template_dir.migrations.url\n  }\n"
)

// targetSummary is what a report over an env's targets says about one target.
type targetSummary struct {
	Instance int
	URL      string
	ExitCode int
	Outcome  migrator.DriftOutcome
	Findings []string
	Pending  []string
	Error    string
}

// targetsReport runs the report over the env "tenants" of the project at path
// and returns its document, each target summarized, and the exit status.
func targetsReport(c *qt.C, path string, extra ...string) (migrationsdrift.TargetsDocument, []targetSummary, int) {
	c.Helper()
	stdout, stderr, code := run(c, append([]string{
		"migrations", "drift", "--json", "--config", path, "--env", "tenants",
	}, extra...)...)
	c.Assert(stderr, qt.Equals, "")
	var got migrationsdrift.TargetsDocument
	c.Assert(json.Unmarshal([]byte(stdout), &got), qt.IsNil, qt.Commentf("stdout: %s", stdout))
	summaries := make([]targetSummary, len(got.Targets))
	for i, target := range got.Targets {
		summaries[i] = summarize(c, target)
	}
	return got, summaries, code
}

// summarize reads what a target's entry says: its report's outcome, findings
// and pending migrations, or its error.
func summarize(c *qt.C, target migrationsdrift.Target) targetSummary {
	c.Helper()
	summary := targetSummary{Instance: target.Instance, URL: target.URL, ExitCode: target.ExitCode}
	if target.Error != nil {
		summary.Error = target.Error.Message
	}
	if target.Report == nil {
		return summary
	}
	var report document
	c.Assert(json.Unmarshal(target.Report, &report), qt.IsNil)
	summary.Outcome = report.Check.Outcome
	summary.Pending = report.Pending
	for _, finding := range report.Check.Findings {
		summary.Findings = append(summary.Findings, finding.Message)
	}
	return summary
}

// changedTarget is a target migrated to version 1 from a file that differs
// from the one driftMigrations holds, so a report over driftMigrations cannot
// replay its history.
func changedTarget(c *qt.C) target {
	c.Helper()
	tgt := emptyTarget(c)
	tgt.write(c, "0000000001_users.up.sql", "-- the users\n"+driftMigrations["0000000001_users.up.sql"])
	tgt.up(c, "1")
	return tgt
}

// unreachable is the URL of an SQLite database in a directory that does not
// exist, which no run can open.
func unreachable(c *qt.C) string {
	c.Helper()
	return "sqlite://" + filepath.Join(c.TempDir(), "missing", "target.db")
}

// A report over an env with for_each reports every target, in the order the
// env expands, and changes none of them.
func TestMigrationsDrift_HappyPathReportsEveryTargetOfAForEachEnv(t *testing.T) {
	c := qt.New(t)
	first, second := newTarget(c, "1"), newTarget(c, "2")
	before := [][]string{tables(c, first.url), tables(c, second.url)}
	path := forEachProject(c, first.shadowURL, first.url, second.url)

	got, summaries, code := targetsReport(c, path, "--migrations-dir", first.dir)

	c.Assert(code, qt.Equals, 0)
	c.Assert(got.ContractVersion, qt.Equals, migrationsdrift.ContractVersion)
	c.Assert(got.Command, qt.Equals, migrationsdrift.Command)
	c.Assert(got.Env, qt.Equals, "tenants")
	c.Assert(summaries, qt.DeepEquals, []targetSummary{
		{Instance: 1, URL: first.url, Outcome: migrator.DriftClean, Pending: []string{"2"}},
		{Instance: 2, URL: second.url, Outcome: migrator.DriftClean},
	})
	c.Assert([][]string{tables(c, first.url), tables(c, second.url)}, qt.DeepEquals, before)
	c.Assert(tables(c, first.shadowURL), qt.HasLen, 0)
}

// A target that drifted exits 1 and does not stop the run: the target after
// it is reported too.
func TestMigrationsDrift_FailurePathReportsADriftedTargetAndGoesOn(t *testing.T) {
	c := qt.New(t)
	drifted, clean := newTarget(c, "2", "DROP INDEX users_email"), newTarget(c, "2")
	before := tables(c, drifted.url)
	path := forEachProject(c, clean.shadowURL, drifted.url, clean.url)

	_, summaries, code := targetsReport(c, path, "--migrations-dir", clean.dir)

	c.Assert(code, qt.Equals, 1)
	c.Assert(summaries, qt.DeepEquals, []targetSummary{
		{
			Instance: 1, URL: drifted.url, ExitCode: 1, Outcome: migrator.DriftDrifted,
			Findings: []string{"missing index users.users_email"},
		},
		{Instance: 2, URL: clean.url, Outcome: migrator.DriftClean},
	})
	c.Assert(tables(c, drifted.url), qt.DeepEquals, before)
}

// A target the run cannot open, and one whose comparison cannot decide, exit
// 2, beside a target that drifted, and none of them stops the run.
func TestMigrationsDrift_FailurePathReportsATargetItCannotCompareAndGoesOn(t *testing.T) {
	c := qt.New(t)
	drifted, undecided, clean := newTarget(c, "2", "DROP INDEX users_email"), changedTarget(c), newTarget(c, "1")
	missing := unreachable(c)
	path := forEachProject(c, clean.shadowURL, drifted.url, missing, undecided.url, clean.url)

	_, summaries, code := targetsReport(c, path, "--migrations-dir", clean.dir)

	c.Assert(code, qt.Equals, 2)
	c.Assert(summaries, qt.DeepEquals, []targetSummary{
		{
			Instance: 1, URL: drifted.url, ExitCode: 1, Outcome: migrator.DriftDrifted,
			Findings: []string{"missing index users.users_email"},
		},
		{
			Instance: 2, URL: missing, ExitCode: 2,
			Error: "error connecting to database: failed to ping database: unable to open database file (14)",
		},
		{Instance: 3, URL: undecided.url, ExitCode: 2, Outcome: migrator.DriftUnable, Pending: []string{"2"}},
		{Instance: 4, URL: clean.url, Outcome: migrator.DriftClean, Pending: []string{"2"}},
	})
	_, err := os.Stat(filepath.Dir(strings.TrimPrefix(missing, "sqlite://")))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

// The text report names each target before its report, says why a target
// made none, and closes with a count of what the reports found.
func TestMigrationsDrift_FailurePathReportsEveryTargetText(t *testing.T) {
	c := qt.New(t)
	drifted := newTarget(c, "2", "DROP INDEX users_email")
	missing := unreachable(c)
	path := forEachProject(c, drifted.shadowURL, drifted.url, missing)

	stdout, stderr, code := run(c, "migrations", "drift", "--config", path, "--env", "tenants",
		"--migrations-dir", drifted.dir)

	c.Assert(code, qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "Instance 1 of env \"tenants\": "+drifted.url+"\n"+
		"Dialect: sqlite\nRevision table: \"schema_migrations\"\nCurrent version: 2\nPending migrations: 0\n\n"+
		"Drift: the database differs from the schema its 2 applied migrations produce:\n"+
		"  - missing index users.users_email\n\n"+
		"Statements that would bring the database back to its history (review them; nothing ran):\n"+
		"CREATE INDEX \"users_email\" ON \"users\" (\"email\");\n\n"+
		"Instance 2 of env \"tenants\": "+missing+"\n"+
		"Error: error connecting to database: failed to ping database: unable to open database file (14)\n\n"+
		"Summary: 2 targets: 0 without drift, 1 with drift, 1 not compared.\n")
	c.Assert(stderr, qt.Equals, "")
}

// Two env blocks of one name are two targets, and neither is an instance of a
// for_each, so each is named by its env alone.
func TestMigrationsDrift_HappyPathReportsEveryEnvBlockOfOneName(t *testing.T) {
	c := qt.New(t)
	first, second := newTarget(c, "1"), newTarget(c, "2")
	path := projectWith(c, "", "env \"tenants\" {\n  url = "+strconv.Quote(first.url)+"\n  dev = "+strconv.Quote(first.shadowURL)+"\n}\n"+
		"env \"tenants\" {\n  url = "+strconv.Quote(second.url)+"\n  dev = "+strconv.Quote(first.shadowURL)+"\n}\n")

	stdout, stderr, code := run(c, "migrations", "drift", "--config", path, "--env", "tenants",
		"--migrations-dir", first.dir)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Equals, "Env \"tenants\": "+first.url+"\n"+
		"Dialect: sqlite\nRevision table: \"schema_migrations\"\nCurrent version: 1\nPending migrations: 1\n  - 2\n\n"+
		"No drift: the database matches the schema its 1 applied migration produces.\n\n"+
		"Env \"tenants\": "+second.url+"\n"+
		"Dialect: sqlite\nRevision table: \"schema_migrations\"\nCurrent version: 2\nPending migrations: 0\n\n"+
		"No drift: the database matches the schema its 2 applied migrations produce.\n\n"+
		"Summary: 2 targets: 2 without drift, 0 with drift, 0 not compared.\n")
}

// A for_each env whose name selects one instance is still a collection: the
// report keeps the shape a script reads for several targets, and names the
// instance by its place in the expansion.
func TestMigrationsDrift_HappyPathKeepsTheShapeOfOneSelectedInstance(t *testing.T) {
	c := qt.New(t)
	tgt := newTarget(c, "1")
	path := projectWith(c, "", "env \"tenants\" {\n"+
		"  for_each = { aa = \"sqlite://unused.db\", tenants = "+strconv.Quote(tgt.url)+" }\n"+
		"  name     = each.key\n"+
		"  url      = each.value\n"+
		"  dev      = "+strconv.Quote(tgt.shadowURL)+"\n"+
		"}\n")

	_, summaries, code := targetsReport(c, path, "--migrations-dir", tgt.dir)

	c.Assert(code, qt.Equals, 0)
	c.Assert(summaries, qt.DeepEquals, []targetSummary{
		{Instance: 2, URL: tgt.url, Outcome: migrator.DriftClean, Pending: []string{"2"}},
	})
}

// A migration directory atlas.hcl renders with data.template_dir is the one
// the report replays: the project binds it while it loads, so the report reads
// the directory after the load (stokaro/ptah#6559).
func TestMigrationsDrift_HappyPathReadsATemplateDirectory(t *testing.T) {
	c := qt.New(t)
	tgt := emptyTarget(c)
	path := projectWith(c, templateDir, "env \"local\" {\n"+
		"  url = "+strconv.Quote(tgt.url)+"\n"+
		"  dev = "+strconv.Quote(tgt.shadowURL)+"\n"+
		templateMigration+
		"}\n")
	_, stderr, code := run(c, "migrations", "up", "--config", path, "--env", "local", "--to-version", "1")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))

	stdout, stderr, code := run(c, "migrations", "drift", "--json", "--config", path, "--env", "local")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	var got document
	c.Assert(json.Unmarshal([]byte(stdout), &got), qt.IsNil)
	c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean)
	c.Assert(got.Check.Replayed, qt.DeepEquals, []int64{1})
	c.Assert(got.CurrentVersion, qt.Equals, "1")
	c.Assert(got.Pending, qt.DeepEquals, []string{"2"})
}

// Every target of a for_each env reads the directory data.template_dir
// renders: no migration ran on either database, so the directory is where the
// pending migrations come from.
func TestMigrationsDrift_HappyPathReadsATemplateDirectoryForEveryTarget(t *testing.T) {
	c := qt.New(t)
	first, second := emptyTarget(c), emptyTarget(c)
	path := projectWith(c, templateDir, forEachEnv(first.shadowURL, templateMigration, first.url, second.url))

	_, summaries, code := targetsReport(c, path)

	c.Assert(code, qt.Equals, 0)
	c.Assert(summaries, qt.DeepEquals, []targetSummary{
		{Instance: 1, URL: first.url, Outcome: migrator.DriftClean, Pending: []string{"1", "2"}},
		{Instance: 2, URL: second.url, Outcome: migrator.DriftClean, Pending: []string{"1", "2"}},
	})
	c.Assert(tables(c, first.url), qt.HasLen, 0)
	c.Assert(tables(c, second.url), qt.HasLen, 0)
}

// A name atlas.hcl ignores is reported once for the run, not once per target.
func TestMigrationsDrift_HappyPathWarnsOnceOfAnIgnoredName(t *testing.T) {
	c := qt.New(t)
	first, second := newTarget(c, "1"), newTarget(c, "1")
	path := projectWith(c, "unknown_setting = 1\n", forEachEnv(first.shadowURL, "", first.url, second.url))

	_, stderr, code := run(c, "migrations", "drift", "--json", "--config", path, "--env", "tenants",
		"--migrations-dir", first.dir)

	c.Assert(code, qt.Equals, 0)
	c.Assert(stderr, qt.Equals, `warning: atlas.hcl attribute "unknown_setting" at `+path+":1 "+
		"is ignored for Atlas compatibility and has no effect\n")
}

// An operator who stops the run stops it: the target being reported fails,
// no target after it is tried, and the run prints the failure rather than a
// report over the targets.
func TestMigrationsDrift_FailurePathStopsWhenTheRunIsCanceled(t *testing.T) {
	c := qt.New(t)
	first, second := newTarget(c, "1"), newTarget(c, "1")
	path := forEachProject(c, first.shadowURL, first.url, second.url)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := root.NewRootCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)

	code := root.RunContext(ctx, cmd, "migrations", "drift", "--json", "--config", path, "--env", "tenants",
		"--migrations-dir", first.dir)

	c.Assert(code, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", out.String(), diagnostics.String()))
	var failure errorkind.Failure
	c.Assert(json.Unmarshal(out.Bytes(), &failure), qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(failure.Command, qt.Equals, migrationsdrift.Command)
	c.Assert(failure.Error, qt.IsNotNil)
	c.Assert(failure.Error.Message, qt.Equals, "error connecting to database: failed to ping database: context canceled")
}
