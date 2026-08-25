package agentgate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentgate"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agentworkspace"
	"go.5x5.cz/ptah/internal/migrateops"
	"go.5x5.cz/ptah/migration/migrationfile"
)

// scopeFor builds a workspace holding one class directory with the given files
// and returns that class's scope.
func scopeFor(
	c *qt.C,
	class agentpolicy.ArtifactClass,
	files map[string]string,
) *agentworkspace.Scope {
	c.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, string(class))
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}

	workspace, err := agentworkspace.Open(agentworkspace.Config{
		Root: root,
		Classes: map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig{
			class: {Dir: string(class), Writable: true},
		},
		Dialect: "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = workspace.Close() })

	scope, err := workspace.Scope(class)
	c.Assert(err, qt.IsNil)
	return scope
}

// runner builds the gate runner every test uses.
func runner(c *qt.C) *agentgate.Runner {
	c.Helper()
	built, err := agentgate.New(agentgate.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	return built
}

// resultFor returns the named gate's result from a report.
func resultFor(c *qt.C, report agentgate.Report, gate string) agentgate.Result {
	c.Helper()
	found := agentgate.Result{Gate: "<absent>"}
	for _, result := range report.Results {
		matches := map[bool]agentgate.Result{true: result, false: found}
		found = matches[result.Gate == gate]
	}
	c.Assert(found.Gate, qt.Equals, gate)
	return found
}

// hashMigrations writes the integrity file for a migration directory, which is
// what every Ptah verb that touches one leaves behind.
func hashMigrations(c *qt.C, scope *agentworkspace.Scope) {
	c.Helper()
	_, err := migrateops.Rehash(scope.Path(), migrationfile.DirFormatAuto)
	c.Assert(err, qt.IsNil)
}

func TestNew_RefusesAGateWithNoTarget(t *testing.T) {
	// A lint run without a dialect either guesses or checks nothing, and a gate
	// that checks nothing reports the same "ok" as one that checked everything.
	c := qt.New(t)

	built, err := agentgate.New(agentgate.Options{})

	c.Assert(err, qt.ErrorMatches, "gate dialect is required: a check without a target checks nothing")
	c.Assert(built, qt.IsNil)
}

func TestRun_MigrationsHappyPath(t *testing.T) {
	c := qt.New(t)
	scope := scopeFor(c, agentpolicy.ClassMigrations, map[string]string{
		"1700000000_init.up.sql":   "CREATE TABLE users (id BIGINT PRIMARY KEY);\n",
		"1700000000_init.down.sql": "DROP TABLE users;\n",
	})
	hashMigrations(c, scope)

	report, err := runner(c).Run(context.Background(), scope)

	c.Assert(err, qt.IsNil)
	c.Assert(report.OK, qt.IsTrue)
	c.Assert(report.Skipped, qt.Equals, 0)
	c.Assert(resultFor(c, report, agentgate.GateMigrationIntegrity).OK, qt.IsTrue)
	c.Assert(resultFor(c, report, agentgate.GateMigrationSQL).OK, qt.IsTrue)
}

func TestRun_MigrationsCatchAnEditThatTheChecksumDoesNotCover(t *testing.T) {
	// This is the property the whole write path depends on: a file changed
	// without its checksum is a directory every executing Ptah verb refuses, so
	// a patch that skipped the refresh would produce a repository that looks
	// written and cannot be used.
	c := qt.New(t)
	scope := scopeFor(c, agentpolicy.ClassMigrations, map[string]string{
		"1700000000_init.up.sql":   "CREATE TABLE users (id BIGINT PRIMARY KEY);\n",
		"1700000000_init.down.sql": "DROP TABLE users;\n",
	})
	hashMigrations(c, scope)
	c.Assert(os.WriteFile(
		filepath.Join(scope.Path(), "1700000000_init.up.sql"),
		[]byte("CREATE TABLE users (id BIGINT PRIMARY KEY, stolen TEXT);\n"), 0o600), qt.IsNil)

	report, err := runner(c).Run(context.Background(), scope)

	c.Assert(err, qt.IsNil)
	c.Assert(report.OK, qt.IsFalse)
	integrity := resultFor(c, report, agentgate.GateMigrationIntegrity)
	c.Assert(integrity.OK, qt.IsFalse)
	c.Assert(integrity.Diagnostics, qt.HasLen, 1)
	c.Assert(integrity.Diagnostics[0].Path, qt.Equals, "1700000000_init.up.sql")
	c.Assert(integrity.Diagnostics[0].Severity, qt.Equals, agentgate.SeverityError)
}

func TestRun_MigrationsReportUnparsableSQL(t *testing.T) {
	c := qt.New(t)
	scope := scopeFor(c, agentpolicy.ClassMigrations, map[string]string{
		"1700000000_init.up.sql":   "CRATE TABLE users (id BIGINT);\n",
		"1700000000_init.down.sql": "DROP TABLE users;\n",
	})
	hashMigrations(c, scope)

	report, err := runner(c).Run(context.Background(), scope)

	c.Assert(err, qt.IsNil)
	c.Assert(report.OK, qt.IsFalse)
	sql := resultFor(c, report, agentgate.GateMigrationSQL)
	c.Assert(sql.OK, qt.IsFalse)
	c.Assert(sql.Diagnostics[0].Path, qt.Equals, "1700000000_init.up.sql")
	c.Assert(sql.Diagnostics[0].Severity, qt.Equals, agentgate.SeverityError)
}

const goodSchema = `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="email" type="TEXT" not_null="true"
	Email string
}
`

const unparsableSchema = `package models

type User struct {
`

func TestRun_SchemaHappyPath(t *testing.T) {
	c := qt.New(t)
	scope := scopeFor(c, agentpolicy.ClassSchema, map[string]string{"models.go": goodSchema})

	report, err := runner(c).Run(context.Background(), scope)

	c.Assert(err, qt.IsNil)
	c.Assert(report.OK, qt.IsTrue)
	c.Assert(report.Skipped, qt.Equals, 0)
	c.Assert(resultFor(c, report, agentgate.GateSchemaLoad).OK, qt.IsTrue)
	c.Assert(resultFor(c, report, agentgate.GateSchemaValidate).OK, qt.IsTrue)
	c.Assert(resultFor(c, report, agentgate.GateSchemaRender).OK, qt.IsTrue)
}

func TestRun_SchemaThatWillNotLoadSkipsTheGatesThatNeedIt(t *testing.T) {
	// A gate that could not run is reported as skipped rather than as passed,
	// because a check that quietly does not run reads as a check that agreed.
	c := qt.New(t)
	scope := scopeFor(c, agentpolicy.ClassSchema, map[string]string{"models.go": unparsableSchema})

	report, err := runner(c).Run(context.Background(), scope)

	c.Assert(err, qt.IsNil)
	c.Assert(report.OK, qt.IsFalse)
	c.Assert(report.Skipped, qt.Equals, 2)
	c.Assert(resultFor(c, report, agentgate.GateSchemaLoad).OK, qt.IsFalse)
	validate := resultFor(c, report, agentgate.GateSchemaValidate)
	c.Assert(validate.Skipped, qt.IsTrue)
	c.Assert(validate.SkipReason, qt.Equals, "the schema did not load")
	c.Assert(resultFor(c, report, agentgate.GateSchemaRender).Skipped, qt.IsTrue)
}

func TestRun_TestsHappyPath(t *testing.T) {
	c := qt.New(t)
	scope := scopeFor(c, agentpolicy.ClassTests, map[string]string{
		"users.yaml": "cases:\n  - name: creates the users table\n    steps:\n      - migrate_to: latest\n",
	})

	report, err := runner(c).Run(context.Background(), scope)

	c.Assert(err, qt.IsNil)
	c.Assert(resultFor(c, report, agentgate.GateTestParse).OK, qt.IsTrue)
}

func TestRun_TestsReportAFileThatDoesNotParse(t *testing.T) {
	c := qt.New(t)
	scope := scopeFor(c, agentpolicy.ClassTests, map[string]string{
		"users.yaml": "cases:\n  - name: [unterminated\n",
	})

	report, err := runner(c).Run(context.Background(), scope)

	c.Assert(err, qt.IsNil)
	c.Assert(report.OK, qt.IsFalse)
	parse := resultFor(c, report, agentgate.GateTestParse)
	c.Assert(parse.OK, qt.IsFalse)
	c.Assert(parse.Diagnostics[0].Path, qt.Equals, "users.yaml")
}

func TestReportComparison_SeparatesWhatAPatchIntroducedFromWhatWasAlreadyThere(t *testing.T) {
	c := qt.New(t)
	old := agentgate.Diagnostic{
		Gate: "g", Severity: agentgate.SeverityError, Path: "a.sql", Message: "already broken",
	}
	fresh := agentgate.Diagnostic{
		Gate: "g", Severity: agentgate.SeverityError, Path: "b.sql", Message: "newly broken",
	}
	baseline := agentgate.Report{Results: []agentgate.Result{{Gate: "g", Diagnostics: []agentgate.Diagnostic{old}}}}
	after := agentgate.Report{Results: []agentgate.Result{{Gate: "g", Diagnostics: []agentgate.Diagnostic{old, fresh}}}}

	c.Assert(after.Introduced(baseline), qt.DeepEquals, []agentgate.Diagnostic{fresh})
	c.Assert(after.Resolved(baseline), qt.HasLen, 0)
}

func TestReportComparison_AMovedDiagnosticIsNotANewOne(t *testing.T) {
	// An inserted line moves every diagnostic below it. A comparison keyed on
	// the line number would report a patch as introducing problems it shifted.
	c := qt.New(t)
	before := agentgate.Diagnostic{
		Gate: "g", Severity: agentgate.SeverityError, Path: "a.sql", Line: 3, Message: "same problem",
	}
	moved := before
	moved.Line = 9
	baseline := agentgate.Report{Results: []agentgate.Result{{Gate: "g", Diagnostics: []agentgate.Diagnostic{before}}}}
	after := agentgate.Report{Results: []agentgate.Result{{Gate: "g", Diagnostics: []agentgate.Diagnostic{moved}}}}

	c.Assert(after.Introduced(baseline), qt.HasLen, 0)
	c.Assert(after.Resolved(baseline), qt.HasLen, 0)
}

func TestReportComparison_ARepairIsReported(t *testing.T) {
	c := qt.New(t)
	fixed := agentgate.Diagnostic{
		Gate: "g", Severity: agentgate.SeverityError, Path: "a.sql", Message: "was broken",
	}
	baseline := agentgate.Report{Results: []agentgate.Result{{Gate: "g", Diagnostics: []agentgate.Diagnostic{fixed}}}}
	after := agentgate.Report{Results: []agentgate.Result{{Gate: "g", Diagnostics: make([]agentgate.Diagnostic, 0)}}}

	c.Assert(after.Resolved(baseline), qt.DeepEquals, []agentgate.Diagnostic{fixed})
	c.Assert(after.Introduced(baseline), qt.HasLen, 0)
}
