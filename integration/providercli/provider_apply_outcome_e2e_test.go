//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/exeext"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// noTransactionMessage is what a refusal of a provider without
// sessions.transactions ends with. It names the mode the run can use.
const noTransactionMessage = "does not support transactions for the selected engine; rerun with --tx-mode none, which commits each statement as it runs"

// databaseTables reads which of the named tables exist, from the database file
// and with nothing of Ptah's or the provider's in between. It returns nil when
// none does.
func databaseTables(c *qt.C, path string, names ...string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT name FROM sqlite_master WHERE type = 'table'`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var present []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		present = append(present, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	var found []string
	for _, name := range names {
		if slices.Contains(present, name) {
			found = append(found, name)
		}
	}
	return found
}

// applyReport decodes the document `schema apply --json` printed.
func applyReport(c *qt.C, out string) atlasschema.ApplyReport {
	c.Helper()
	var report atlasschema.ApplyReport
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
	c.Assert(report.ContractVersion, qt.Equals, atlasschema.ApplyReportContractVersion)
	return report
}

// refusalCode is the code of the report's refusal, and empty without one.
func refusalCode(report atlasschema.ApplyReport) atlasschema.RefusalCode {
	if report.Refusal == nil {
		return ""
	}
	return report.Refusal.Code
}

// computedArgs and savedArgs run the fixture's schema, computed from the desired
// schema and read from the plan saved earlier: the command takes two paths to
// the statements, and each marks the dispatch on its own.
func computedArgs(f providerCLI, args ...string) []string {
	return append([]string{"--schema-file", f.schemaPath, "--auto-approve", "--json"}, args...)
}

func savedArgs(f providerCLI, args ...string) []string {
	return append([]string{"--plan", f.planPath, "--auto-approve", "--json"}, args...)
}

// computedUnapprovedArgs and savedUnapprovedArgs leave approval to a prompt whose
// standard input is empty. A refusal that comes after the prompt reads as a
// canceled run, so these rows hold that the refusal comes first.
func computedUnapprovedArgs(f providerCLI, args ...string) []string {
	return append([]string{"--schema-file", f.schemaPath, "--json"}, args...)
}

func savedUnapprovedArgs(f providerCLI, args ...string) []string {
	return append([]string{"--plan", f.planPath, "--json"}, args...)
}

// TestProviderSchemaApply_ReportsWhereTheRunStopped reads the document against
// the database. An outcome that says nothing reached the database holds only if
// the tables are absent, and one that does not say so holds even where the
// provider took the statements and the table is there.
func TestProviderSchemaApply_ReportsWhereTheRunStopped(t *testing.T) {
	parent := qt.New(t)
	withoutTransactions := fixtureReleaseSpec(parent, "provider", testutils.FixtureRelease{})
	withTransactions := fixtureReleaseSpec(parent, "transactions", testutils.FixtureRelease{})
	withLocks := fixtureReleaseSpec(parent, "locks", testutils.FixtureRelease{})
	tests := []struct {
		name        string
		spec        providerhost.Spec
		fault       string
		args        func(providerCLI, ...string) []string
		mode        []string
		wantOutcome atlasschema.ApplyOutcome
		wantRefusal atlasschema.RefusalCode
		wantError   string
		wantTables  []string
		// wantFailure is where the record says the run stopped. A provider confirms
		// a prefix and gives no code, so the position is the first unit it did not
		// confirm, whatever became of that unit.
		wantFailure *atlasschema.ApplyFailure
	}{
		{
			name: "no transactions, a computed plan in the default mode", spec: withoutTransactions, args: computedArgs,
			wantOutcome: atlasschema.ApplyOutcomeRefused, wantRefusal: atlasschema.RefusalTransactionUnavailable,
			wantError: noTransactionMessage,
		},
		{
			name: "no transactions, a saved plan in the default mode", spec: withoutTransactions, args: savedArgs,
			wantOutcome: atlasschema.ApplyOutcomeRefused, wantRefusal: atlasschema.RefusalTransactionUnavailable,
			wantError: noTransactionMessage,
		},
		{
			name: "no transactions, a computed plan nobody approved", spec: withoutTransactions, args: computedUnapprovedArgs,
			wantOutcome: atlasschema.ApplyOutcomeRefused, wantRefusal: atlasschema.RefusalTransactionUnavailable,
			wantError: noTransactionMessage,
		},
		{
			name: "no transactions, a saved plan nobody approved", spec: withoutTransactions, args: savedUnapprovedArgs,
			wantOutcome: atlasschema.ApplyOutcomeRefused, wantRefusal: atlasschema.RefusalTransactionUnavailable,
			wantError: noTransactionMessage,
		},
		{
			name: "no transactions, one transaction asked for by name", spec: withoutTransactions, args: computedArgs, mode: []string{"--tx-mode", "all"},
			wantOutcome: atlasschema.ApplyOutcomeRefused, wantRefusal: atlasschema.RefusalTransactionUnavailable,
			wantError: `transaction mode "all"`,
		},
		{
			name: "no transactions, the mode the refusal names", spec: withoutTransactions, args: computedArgs, mode: []string{"--tx-mode", "none"},
			wantOutcome: atlasschema.ApplyOutcomeApplied, wantTables: []string{"items", "second"},
		},
		{
			name: "the provider refuses to begin, a computed plan", spec: withTransactions, fault: "refuse-begin", args: computedArgs,
			wantOutcome: atlasschema.ApplyOutcomeFailed, wantError: "begin schema apply transaction: ",
		},
		{
			name: "the provider refuses to begin, a saved plan", spec: withTransactions, fault: "refuse-begin", args: savedArgs,
			wantOutcome: atlasschema.ApplyOutcomeFailed, wantError: "begin schema apply transaction: ",
		},
		{
			name: "the provider dies while a transaction begins", spec: withTransactions, fault: "crash-begin", args: savedArgs,
			wantOutcome: atlasschema.ApplyOutcomeFailed, wantError: "begin schema apply transaction: ",
		},
		{
			name: "the lock is lost before the first statement", spec: withLocks, fault: "lose-lock-before-write", args: savedArgs,
			wantOutcome: atlasschema.ApplyOutcomeFailed, wantError: "begin schema apply transaction: ",
		},
		{
			name: "the lock is lost after every statement", spec: withLocks, fault: "lose-lock-on-check", args: savedArgs,
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantError: "was lost while it was held", wantTables: []string{"items", "second"},
		},
		{
			name: "the provider dies after a statement, a computed plan", spec: withoutTransactions, fault: "crash-after", args: computedArgs, mode: []string{"--tx-mode", "none"},
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantError: "UNKNOWN", wantTables: []string{"items"},
			wantFailure: &atlasschema.ApplyFailure{Statement: 1, Statements: 2},
		},
		{
			name: "the provider dies after a statement, a saved plan", spec: withoutTransactions, fault: "crash-after", args: savedArgs, mode: []string{"--tx-mode", "none"},
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantError: "UNKNOWN", wantTables: []string{"items"},
			wantFailure: &atlasschema.ApplyFailure{Statement: 1, Statements: 2},
		},
		{
			name: "the first unit applied and the next did not", spec: withoutTransactions, fault: "partial", args: savedArgs, mode: []string{"--tx-mode", "none"},
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantError: "PARTIAL", wantTables: []string{"items"},
			wantFailure: &atlasschema.ApplyFailure{Statement: 2, Statements: 2},
		},
		{
			name: "a commit whose reply was lost", spec: withTransactions, fault: "lost-commit", args: savedArgs,
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantError: "UNKNOWN", wantTables: []string{"items", "second"},
			wantFailure: &atlasschema.ApplyFailure{Statements: 2, Commit: true},
		},
		{
			name: "a commit that never ran", spec: withTransactions, fault: "lost-before-commit", args: savedArgs,
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantError: "UNKNOWN",
			wantFailure: &atlasschema.ApplyFailure{Statements: 2, Commit: true},
		},
		// The units left the host, and only the provider's answer says they did
		// not run. The report does not take a provider's word over its own
		// evidence, whatever the migrator does with the same answer.
		{
			name: "the provider answers that it did not start", spec: withoutTransactions, fault: "reject", args: savedArgs, mode: []string{"--tx-mode", "none"},
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantError: "NOT_STARTED",
			wantFailure: &atlasschema.ApplyFailure{Statement: 1, Statements: 2},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newProviderCLI(c, test.spec)
			f.plan(c)
			f.address += "?fault=" + test.fault

			out, diagnostics, _ := f.run(c, c.Context(), "apply", test.args(f, test.mode...)...)

			report := applyReport(c, out)
			c.Assert(report.Outcome, qt.Equals, test.wantOutcome, qt.Commentf("%s", diagnostics))
			c.Assert(refusalCode(report), qt.Equals, test.wantRefusal)
			c.Assert(diagnostics, qt.Contains, test.wantError)
			c.Assert(report.Failure, qt.DeepEquals, test.wantFailure)
			c.Assert(report.Units, qt.HasLen, 2)
			c.Assert(databaseTables(c, f.databasePath, "items", "second"), qt.DeepEquals, test.wantTables)
		})
	}
}

// A lock the provider will not give stops the run before it has a plan, and so
// does a session it will not open. Neither is a report of statements sent.
func TestProviderSchemaApply_ReportsARefusalBeforeThePlanAsFailed(t *testing.T) {
	t.Run("the lock", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, fixtureReleaseSpec(c, "locks", testutils.FixtureRelease{}))
		f.address += "?fault=lost-lock-reply"

		out, diagnostics, code := f.run(c, c.Context(), "apply", computedArgs(f)...)

		c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
		report := applyReport(c, out)
		c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
		c.Assert(report.Error.Message, qt.Contains, "acquire schema apply lock: ")
		c.Assert(report.Units, qt.HasLen, 0)
		c.Assert(databaseTables(c, f.databasePath, "items", "second"), qt.HasLen, 0)
	})
	t.Run("the session", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, fixtureReleaseSpec(c, "locks", testutils.FixtureRelease{}))
		f.address = strings.Replace(f.address, "database.db", "missing/database.db", 1)

		out, diagnostics, code := f.run(c, c.Context(), "apply", computedArgs(f)...)

		c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
		report := applyReport(c, out)
		c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
		c.Assert(report.Error.Message, qt.Contains, "connect to --db-url: ")
		c.Assert(report.Units, qt.HasLen, 0)
	})
}

// executionInputSpec serves one SQL statement that reads the fixture's execution
// input, which sends the command down its path for statements approved with
// their inputs. The provider offers no transactions.
func executionInputSpec(c *qt.C) providerhost.Spec {
	c.Helper()
	spec := fixtureReleaseSpec(c, "operation-models", testutils.FixtureRelease{})
	spec.Environment = []string{"FIXTURE_RENDER_MODE=native-cli", "FIXTURE_INPUT_MODE=stable"}
	return spec
}

func executionInputFixture(c *qt.C, spec providerhost.Spec) providerCLI {
	c.Helper()
	f := newProviderCLI(c, spec)
	c.Assert(os.WriteFile(f.schemaPath, []byte("tables:\n  needs_input:\n    columns:\n      id: {type: INTEGER, not_null: true}\n"), 0o600), qt.IsNil)
	f.plan(c)
	return f
}

// TestProviderSchemaApply_ReportsAnUnsetInputAsFailed is a refusal the provider
// session makes on the way to the provider: the variable is read only once the
// plan is approved, so no preflight can see it, and nothing has left the host.
func TestProviderSchemaApply_ReportsAnUnsetInputAsFailed(t *testing.T) {
	parent := qt.New(t)
	t.Setenv(executionInputName, "restore-after-test")
	parent.Assert(os.Unsetenv(executionInputName), qt.IsNil)
	spec := executionInputSpec(parent)
	tests := []struct {
		name string
		args func(providerCLI, ...string) []string
	}{
		{name: "a computed plan", args: computedArgs},
		{name: "a saved plan", args: savedArgs},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := executionInputFixture(c, spec)

			out, diagnostics, code := f.run(c, c.Context(), "apply", test.args(f, "--tx-mode", "none")...)

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
			report := applyReport(c, out)
			c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
			c.Assert(report.Refusal, qt.IsNil)
			c.Assert(report.Error.Message, qt.Contains, `input environment variable "`+executionInputName+`" is not set`)
			c.Assert(databaseTables(c, f.databasePath, "needs_input"), qt.HasLen, 0)
		})
	}
}

// TestProviderSchemaApply_ApprovedInputPathKeepsItsReport drives the two
// remaining places the command starts execution, with the input set: the
// refusal of the default mode comes before them, and a provider that dies after
// a statement is unknown.
func TestProviderSchemaApply_ApprovedInputPathKeepsItsReport(t *testing.T) {
	t.Setenv(executionInputName, "fixture-private-value")
	spec := executionInputSpec(qt.New(t))
	tests := []struct {
		name        string
		fault       string
		args        func(providerCLI, ...string) []string
		mode        []string
		wantOutcome atlasschema.ApplyOutcome
		wantRefusal atlasschema.RefusalCode
		wantTables  []string
		wantFailure *atlasschema.ApplyFailure
	}{
		{
			name: "the default mode, a computed plan", args: computedArgs,
			wantOutcome: atlasschema.ApplyOutcomeRefused, wantRefusal: atlasschema.RefusalTransactionUnavailable,
		},
		{
			name: "the default mode, a saved plan", args: savedArgs,
			wantOutcome: atlasschema.ApplyOutcomeRefused, wantRefusal: atlasschema.RefusalTransactionUnavailable,
		},
		{
			name: "the provider dies after the statement, a computed plan", fault: "crash-after", args: computedArgs, mode: []string{"--tx-mode", "none"},
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantTables: []string{"needs_input"},
			wantFailure: &atlasschema.ApplyFailure{Statement: 1, Statements: 1},
		},
		{
			name: "the provider dies after the statement, a saved plan", fault: "crash-after", args: savedArgs, mode: []string{"--tx-mode", "none"},
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantTables: []string{"needs_input"},
			wantFailure: &atlasschema.ApplyFailure{Statement: 1, Statements: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := executionInputFixture(c, spec)
			f.address += "?fault=" + test.fault

			out, diagnostics, _ := f.run(c, c.Context(), "apply", test.args(f, test.mode...)...)

			report := applyReport(c, out)
			c.Assert(report.Outcome, qt.Equals, test.wantOutcome, qt.Commentf("%s", diagnostics))
			c.Assert(refusalCode(report), qt.Equals, test.wantRefusal)
			c.Assert(report.Failure, qt.DeepEquals, test.wantFailure)
			c.Assert(databaseTables(c, f.databasePath, "needs_input"), qt.DeepEquals, test.wantTables)
		})
	}
}

// minimalProviderBuild builds examples/minimal-provider, which offers locks and
// no transactions, and returns its manifest. The module stands alone, so it
// builds with the workspace off, as its README does.
func minimalProviderBuild(c *qt.C) (manifest string) {
	c.Helper()
	repo, err := exec.CommandContext(c.Context(), "go", "list", "-m", "-f", "{{.Dir}}", "ptah.run").Output() // #nosec G204 -- the test asks the toolchain for this checkout's root.
	c.Assert(err, qt.IsNil)
	dir := c.TempDir()
	executable := filepath.Join(dir, "ptah-tutorial-provider"+exeext.Suffix)
	build := exec.CommandContext(c.Context(), "go", "build", "-o", executable, ".") // #nosec G204 -- the test builds this checkout's own example.
	build.Dir = filepath.Join(strings.TrimSpace(string(repo)), "examples", "minimal-provider")
	build.Env = append(os.Environ(), "GOWORK=off")
	output, err := build.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go build:\n%s", output))
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	content, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion, Providers: []providerconfig.Installation{{
		Spec:    providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.com/ptah-tutorial-provider"},
		Engines: []providerconfig.Engine{{ID: "tutorial"}},
	}}})
	c.Assert(err, qt.IsNil)
	manifest = filepath.Join(dir, "providers.json")
	c.Assert(os.WriteFile(manifest, content, 0o600), qt.IsNil)
	return manifest
}

// TestShippedBinaryRefusesATransactionTheMinimalProviderCannotOpen is the run an
// operator makes: the ptah binary, a provider built on its own, and the database
// file read back without either. The statement the refused runs planned is the
// one the last run applies.
func TestShippedBinaryRefusesATransactionTheMinimalProviderCannotOpen(t *testing.T) {
	c := qt.New(t)
	binary := buildPtah(c)
	manifest := minimalProviderBuild(c)
	dir := c.TempDir()
	database := filepath.Join(dir, "app.db")
	address := "tutorial://" + filepath.ToSlash(database)
	schema := filepath.Join(dir, "add.yaml")
	plan := filepath.Join(dir, "add.plan.json")
	run := func(args ...string) (stdout, stderr string, exitCode int) {
		command := exec.CommandContext(c.Context(), binary, args...) // #nosec G204 -- the test runs its own build.
		var out, diagnostics strings.Builder
		command.Stdout, command.Stderr = &out, &diagnostics
		err := command.Run()
		code := command.ProcessState.ExitCode()
		c.Assert(code >= 0, qt.IsTrue, qt.Commentf("%v: %s", err, diagnostics.String()))
		return out.String(), diagnostics.String(), code
	}
	connection := []string{"--provider-config", manifest, "--db-url", address}
	columnAdded := func() int {
		db, err := sql.Open("sqlite", database)
		c.Assert(err, qt.IsNil)
		defer func() { c.Check(db.Close(), qt.IsNil) }()
		var count int
		c.Assert(db.QueryRowContext(c.Context(), `SELECT count(*) FROM pragma_table_info('notes') WHERE name = 'created_at'`).Scan(&count), qt.IsNil)
		return count
	}

	c.Assert(os.WriteFile(schema, []byte("tables:\n  notes:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n      body: {type: TEXT}\n"), 0o600), qt.IsNil)
	_, diagnostics, code := run(append(append([]string{"schema", "apply"}, connection...), "--schema-file", schema, "--tx-mode", "none", "--auto-approve")...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(columnAdded(), qt.Equals, 0)

	c.Assert(os.WriteFile(schema, []byte("tables:\n  notes:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n      body: {type: TEXT}\n      created_at: {type: INTEGER}\n"), 0o600), qt.IsNil)
	_, diagnostics, code = run(append(append([]string{"schema", "plan"}, connection...), "--schema-file", schema, "--output", plan)...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))

	for _, source := range [][]string{{"--schema-file", schema}, {"--plan", plan}} {
		out, diagnostics, code := run(append(append(append([]string{"schema", "apply"}, connection...), source...), "--auto-approve", "--json")...)
		c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
		report := applyReport(c, out)
		c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
		c.Assert(refusalCode(report), qt.Equals, atlasschema.RefusalTransactionUnavailable)
		c.Assert(report.Error.Message, qt.Contains, noTransactionMessage)
		c.Assert(columnAdded(), qt.Equals, 0)
	}

	out, diagnostics, code := run(append(append([]string{"schema", "apply"}, connection...), "--plan", plan, "--tx-mode", "none", "--auto-approve", "--json")...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(applyReport(c, out).Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
	c.Assert(columnAdded(), qt.Equals, 1)
}
