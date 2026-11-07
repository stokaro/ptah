//go:build integration

package integration_test

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/drift"
	"ptah.run/internal/clirun"
	"ptah.run/internal/errorkind"
)

// Another tool's history tables in the selected schema are objects the desired
// schema does not declare, so `schema drift` and `schema compare` read them as a
// destructive removal. These tests build the two tables Liquibase creates in
// public with plain SQL and run both verbs through the shipped binary, with and
// without the selectors `schema diff` takes.

// historyApplication is the desired schema: the application's objects, and none of
// the other tool's.
const historyApplication = `CREATE TABLE public.subscriptions (id bigint PRIMARY KEY, plan text NOT NULL);
CREATE INDEX subscriptions_plan_idx ON public.subscriptions (plan);
`

const (
	historyChangeLog = "public.databasechangelog"
	historyLock      = "public.databasechangeloglock"
	historyNoObject  = "public.nothing_here"
	historySynced    = "Schemas are synced, no changes to be made.\n"
	historyNoDrift   = "No schema drift detected.\n"
	historyNoDiffs   = "No schema differences detected.\n"
	historyFindings  = "- constraints_removed: 1 (destructive)\n- tables_removed: 2 (destructive)\n"
	historyDropOne   = `DROP TABLE "databasechangelog";`
	historyDropTwo   = `DROP TABLE "databasechangeloglock";`
)

// historyTables are the tables Liquibase creates at its default settings.
var historyTables = []string{
	`CREATE TABLE public.databasechangeloglock (
		id integer NOT NULL, locked boolean NOT NULL, lockgranted timestamp, lockedby varchar(255),
		CONSTRAINT databasechangeloglock_pkey PRIMARY KEY (id))`,
	`CREATE TABLE public.databasechangelog (
		id varchar(255) NOT NULL, author varchar(255) NOT NULL, filename varchar(255) NOT NULL,
		dateexecuted timestamp NOT NULL, orderexecuted integer NOT NULL, exectype varchar(10) NOT NULL,
		md5sum varchar(35), description varchar(255), comments varchar(255), tag varchar(255),
		liquibase varchar(20), contexts varchar(255), labels varchar(255), deployment_id varchar(10))`,
}

// historyDatabase is a database holding the application's objects, then statements.
func historyDatabase(c *qt.C, statements ...string) string {
	c.Helper()
	database := newEmptyReadScopeDatabase(c, "history")
	execPostgresFamilySQL(c, c.Context(), database, historyApplication)
	for _, statement := range statements {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	return database
}

// historySchemaFile writes a desired schema and returns its path.
func historySchemaFile(c *qt.C, schema string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
	return path
}

func historyDrift(c *qt.C, database, schema string, flags ...string) clirun.Result {
	c.Helper()
	return runPtahIn(c, append([]string{"schema", "drift", "--db-url", database, "--schema-file", schema}, flags...)...)
}

func historyCompare(c *qt.C, database, schema string, flags ...string) clirun.Result {
	c.Helper()
	return runPtahIn(c, append([]string{"schema", "compare", "--db-url", database, "--schema-file", schema, "--exit-code"}, flags...)...)
}

func historyDiff(c *qt.C, database, schema string, flags ...string) clirun.Result {
	c.Helper()
	return runPtahIn(c, append([]string{"schema", "diff", "--from", database, "--to", schema}, flags...)...)
}

// historyComment prints both streams when an assertion fails.
func historyComment(got clirun.Result) qt.Comment {
	return qt.Commentf("exit %d\nstdout:\n%s\nstderr:\n%s", got.ExitCode, got.Stdout, got.Stderr)
}

// comparisonBody is what `schema compare` prints under its heading. The heading
// names the URL, so it is not part of what two runs are compared on.
func comparisonBody(c *qt.C, stdout string) string {
	c.Helper()
	_, body, found := strings.Cut(stdout, "=== SCHEMA COMPARISON ===\n\n")
	c.Assert(found, qt.IsTrue, qt.Commentf("stdout:\n%s", stdout))
	return body
}

// reconcilingSQL is the lines of the SQL section of a comparison.
func reconcilingSQL(stdout string) []string {
	_, sql, _ := strings.Cut(stdout, "Reconciling SQL:\n")
	return sqlLines(sql)
}

// sqlLines drops the blank lines between statements, which the two verbs space
// differently.
func sqlLines(sql string) []string {
	return slices.DeleteFunc(strings.Split(sql, "\n"), func(line string) bool { return line == "" })
}

// TestHistoryTablesAreDriftWithoutASelectorE2E is the issue's measurement, and the
// control for every other test here: the application's objects match, and both
// verbs fail on the two tables they were never told about.
func TestHistoryTablesAreDriftWithoutASelectorE2E(t *testing.T) {
	c := qt.New(t)
	database, schema := historyDatabase(c, historyTables...), historySchemaFile(c, historyApplication)

	drifted := historyDrift(c, database, schema, "--schemas", "public")
	c.Assert(drifted.ExitCode, qt.Equals, 1, historyComment(drifted))
	c.Assert(drifted.Stdout, qt.Contains, historyFindings)

	compared := historyCompare(c, database, schema, "--schemas", "public")
	c.Assert(compared.ExitCode, qt.Equals, 1, historyComment(compared))
	c.Assert(compared.Stdout, qt.Contains, historyDropOne)
	c.Assert(compared.Stdout, qt.Contains, historyDropTwo)
}

// TestSelectorsLeaveTheHistoryTablesOutE2E runs each spelling a reader might write
// on the issue's database. Both verbs answer as they answer a database that never
// held the tables, and `schema diff` given the same selectors is the reference for
// which spellings select them.
func TestSelectorsLeaveTheHistoryTablesOutE2E(t *testing.T) {
	tests := []struct {
		name  string
		flags []string
	}{
		{
			name:  "the issue's command line",
			flags: []string{"--schemas", "public", "--exclude", historyChangeLog, "--exclude", historyLock},
		},
		{
			name:  "no --schemas",
			flags: []string{"--exclude", historyChangeLog, "--exclude", historyLock},
		},
		{
			name:  "one value listing both",
			flags: []string{"--exclude", historyChangeLog + "," + historyLock},
		},
		{
			name:  "a glob",
			flags: []string{"--schemas", "public", "--exclude", "public.databasechangelog*[type=table]"},
		},
		{
			name:  "the application included",
			flags: []string{"--include", "public.subscriptions"},
		},
	}
	c := qt.New(t)
	database, schema := historyDatabase(c, historyTables...), historySchemaFile(c, historyApplication)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reference := historyDiff(c, database, schema, test.flags...)
			c.Assert(reference.ExitCode, qt.Equals, 0, historyComment(reference))
			c.Assert(reference.Stdout, qt.Equals, historySynced)

			drifted := historyDrift(c, database, schema, test.flags...)
			c.Assert(drifted.ExitCode, qt.Equals, 0, historyComment(drifted))
			c.Assert(drifted.Stdout, qt.Equals, historyNoDrift)
			c.Assert(drifted.Stderr, qt.Equals, "")

			compared := historyCompare(c, database, schema, test.flags...)
			c.Assert(compared.ExitCode, qt.Equals, 0, historyComment(compared))
			c.Assert(comparisonBody(c, compared.Stdout), qt.Equals, historyNoDiffs)
			c.Assert(compared.Stderr, qt.Equals, "")
		})
	}
}

// TestAnIncludeCarriesAnExtensionAsSupportE2E covers the object a positive selection
// leaves unnamed and a comparison would otherwise remove. Under --include an
// extension the desired schema does not declare rides along, as on `schema diff`.
// Under --exclude alone nothing selects, so the extension is what it is without a
// selector, and --ignore-extension is what leaves it alone.
func TestAnIncludeCarriesAnExtensionAsSupportE2E(t *testing.T) {
	const removal = `feature_changes:ptah.run/pgextension/extension-change: 1 (destructive)`
	c := qt.New(t)
	database := historyDatabase(c, append([]string{"CREATE EXTENSION pg_trgm"}, historyTables...)...)
	schema := historySchemaFile(c, historyApplication)

	included := historyDrift(c, database, schema, "--include", "public.subscriptions")
	c.Assert(included.ExitCode, qt.Equals, 0, historyComment(included))
	c.Assert(included.Stdout, qt.Equals, historyNoDrift)

	reference := historyDiff(c, database, schema, "--include", "public.subscriptions")
	c.Assert(reference.Stdout, qt.Equals, historySynced)

	excluded := historyDrift(c, database, schema, "--exclude", historyChangeLog, "--exclude", historyLock)
	c.Assert(excluded.ExitCode, qt.Equals, 1, historyComment(excluded))
	c.Assert(excluded.Stdout, qt.Contains, removal)

	ignored := historyDrift(c, database, schema, "--exclude", historyChangeLog, "--exclude", historyLock, "--ignore-extension", "pg_trgm")
	c.Assert(ignored.ExitCode, qt.Equals, 0, historyComment(ignored))
	c.Assert(ignored.Stdout, qt.Equals, historyNoDrift)
}

// TestSchemasAloneSelectsNothingE2E covers how --schemas joins a selection. Alone it
// says where the database is read, so a table the desired schema declares elsewhere
// is still planned. Beside a selector it selects, as on `schema diff`, so that table
// leaves both sides.
func TestSchemasAloneSelectsNothingE2E(t *testing.T) {
	desired := historyApplication + "CREATE SCHEMA reporting;\nCREATE TABLE reporting.daily (id bigint PRIMARY KEY);\n"
	selectors := []string{"--schemas", "public", "--exclude", historyChangeLog, "--exclude", historyLock}
	c := qt.New(t)
	database, schema := historyDatabase(c, historyTables...), historySchemaFile(c, desired)

	alone := historyDrift(c, database, schema, "--schemas", "public")
	c.Assert(alone.ExitCode, qt.Equals, 1, historyComment(alone))
	c.Assert(alone.Stdout, qt.Contains, "- tables_added: 1 (safe)\n")

	selected := historyDrift(c, database, schema, selectors...)
	c.Assert(selected.ExitCode, qt.Equals, 0, historyComment(selected))
	c.Assert(selected.Stdout, qt.Equals, historyNoDrift)

	reference := historyDiff(c, database, schema, selectors...)
	c.Assert(reference.Stdout, qt.Equals, historySynced)
}

// TestAPtahExcludeEnvironmentVariableSelectsE2E covers the binding every flag of
// the native tree takes, on both verbs.
func TestAPtahExcludeEnvironmentVariableSelectsE2E(t *testing.T) {
	c := qt.New(t)
	database, schema := historyDatabase(c, historyTables...), historySchemaFile(c, historyApplication)
	environment := append(os.Environ(), "PTAH_EXCLUDE="+historyChangeLog+","+historyLock)
	options := clirun.Options{Dir: c.TempDir(), Env: environment}

	drifted := clirun.Run(c, clirun.Ptah, options, "schema", "drift", "--db-url", database, "--schema-file", schema)
	c.Assert(drifted.ExitCode, qt.Equals, 0, historyComment(drifted))
	c.Assert(drifted.Stdout, qt.Equals, historyNoDrift)

	compared := clirun.Run(c, clirun.Ptah, options, "schema", "compare", "--db-url", database, "--schema-file", schema, "--exit-code")
	c.Assert(compared.ExitCode, qt.Equals, 0, historyComment(compared))
	c.Assert(comparisonBody(c, compared.Stdout), qt.Equals, historyNoDiffs)
}

// TestARealDifferenceBesideExcludedHistoryTablesIsReportedE2E holds each selector to
// what it names. The database has a column and an index the desired schema does
// not declare and lacks a table it does, and every one of them still fails the
// check, while no line names the other tool's tables. `schema diff` given the same
// selectors is the reference for the SQL.
func TestARealDifferenceBesideExcludedHistoryTablesIsReportedE2E(t *testing.T) {
	desired := historyApplication + "CREATE TABLE public.invoices (id bigint PRIMARY KEY);\n"
	selectors := []string{"--exclude", historyChangeLog, "--exclude", historyLock}
	c := qt.New(t)
	database := historyDatabase(c, append([]string{
		"ALTER TABLE public.subscriptions ADD COLUMN legacy text",
		"CREATE INDEX subscriptions_legacy_idx ON public.subscriptions (legacy)",
	}, historyTables...)...)
	schema := historySchemaFile(c, desired)

	drifted := historyDrift(c, database, schema, selectors...)
	c.Assert(drifted.ExitCode, qt.Equals, 1, historyComment(drifted))
	c.Assert(drifted.Stdout, qt.Contains,
		"- columns_removed: 1 (destructive)\n- indexes_removed: 1 (warning)\n- tables_added: 1 (safe)\n")
	c.Assert(drifted.Stdout, qt.Not(qt.Contains), "tables_removed")
	c.Assert(drifted.Stdout, qt.Not(qt.Contains), "constraints_removed")

	compared := historyCompare(c, database, schema, selectors...)
	c.Assert(compared.ExitCode, qt.Equals, 1, historyComment(compared))
	c.Assert(compared.Stdout, qt.Contains, "tables_added (1): public.invoices\n")
	c.Assert(compared.Stdout, qt.Not(qt.Contains), "databasechangelog")

	reference := historyDiff(c, database, schema, selectors...)
	c.Assert(reference.ExitCode, qt.Equals, 0, historyComment(reference))
	c.Assert(reference.Stdout, qt.Not(qt.Equals), historySynced)
	c.Assert(reconcilingSQL(compared.Stdout), qt.DeepEquals, sqlLines(reference.Stdout))
}

// TestAnExcludeThatMatchesNothingOnlyWarnsE2E covers a selector naming no object:
// each verb says so on standard error in the words `schema diff` uses, and decides
// nothing with it. A clean database stays clean and a drifted one stays drifted.
func TestAnExcludeThatMatchesNothingOnlyWarnsE2E(t *testing.T) {
	const warning = `Warning: the --exclude selection matched no objects: "` + historyNoObject + `".` + "\n"
	c := qt.New(t)
	drifted, clean := historyDatabase(c, historyTables...), historyDatabase(c)
	schema := historySchemaFile(c, historyApplication)

	reference := historyDiff(c, clean, schema, "--exclude", historyNoObject)
	c.Assert(reference.ExitCode, qt.Equals, 0, historyComment(reference))
	c.Assert(reference.Stderr, qt.Equals, warning)

	tests := []struct {
		name     string
		database string
		exitCode int
	}{
		{name: "a clean database", database: clean, exitCode: 0},
		{name: "a drifted database", database: drifted, exitCode: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			checked := historyDrift(c, test.database, schema, "--exclude", historyNoObject)
			unselected := historyDrift(c, test.database, schema)
			c.Assert(checked.ExitCode, qt.Equals, test.exitCode, historyComment(checked))
			c.Assert(checked.Stdout, qt.Equals, unselected.Stdout)
			c.Assert(checked.Stderr, qt.Equals, warning)

			compared := historyCompare(c, test.database, schema, "--exclude", historyNoObject)
			alone := historyCompare(c, test.database, schema)
			c.Assert(compared.ExitCode, qt.Equals, test.exitCode, historyComment(compared))
			c.Assert(comparisonBody(c, compared.Stdout), qt.Equals, comparisonBody(c, alone.Stdout))
			c.Assert(compared.Stderr, qt.Equals, warning)
		})
	}
}

// TestARefusedSelectionIsRefusedAsOnSchemaDiffE2E covers the selections no answer
// can be given for. `schema diff` refuses each one, and the reason it gives has to
// reach the reader of both verbs. A selector naming no object protects nothing
// under --exclude, and under --include it would report a comparison of nothing as
// a synced database.
func TestARefusedSelectionIsRefusedAsOnSchemaDiffE2E(t *testing.T) {
	const (
		declared = `CREATE TABLE public.users (id bigint PRIMARY KEY);
CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES public.users (id));
`
		dependency = `(?s).*selection drops objects that selected objects depend on:\n  - table "(public\.)?orders" depends on table "(public\.)?users" via a foreign key.*`
		nothing    = `(?s).*the --include selection matched no objects: "public\.nothing_here"\n`
	)
	tests := []struct {
		name  string
		flags []string
		want  string
	}{
		{
			name:  "--schemas and an exclude leaving out what a selected table needs",
			flags: []string{"--schemas", "public", "--exclude", "public.users"},
			want:  dependency,
		},
		{
			name:  "an include leaving out what a selected table needs",
			flags: []string{"--include", "public.orders"},
			want:  dependency,
		},
		{
			name:  "an include matching nothing on either side",
			flags: []string{"--include", historyNoObject},
			want:  nothing,
		},
	}
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "history_refused")
	execPostgresFamilySQL(c, c.Context(), database, declared)
	schema := historySchemaFile(c, declared)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reference := historyDiff(c, database, schema, test.flags...)
			c.Assert(reference.ExitCode, qt.Equals, 2, historyComment(reference))
			c.Assert(reference.Stderr, qt.Matches, test.want)

			drifted := historyDrift(c, database, schema, test.flags...)
			c.Assert(drifted.ExitCode, qt.Equals, 2, historyComment(drifted))
			c.Assert(drifted.Stderr, qt.Matches, test.want)
			c.Assert(drifted.Stdout, qt.Not(qt.Contains), "No schema drift")

			compared := historyCompare(c, database, schema, test.flags...)
			c.Assert(compared.ExitCode, qt.Equals, 2, historyComment(compared))
			c.Assert(compared.Stderr, qt.Matches, test.want)
			c.Assert(compared.Stdout, qt.Not(qt.Contains), "No schema differences")

			document := runPtahIn(c, append([]string{
				"schema", "drift", "--db-url", database, "--schema-file", schema, "--format", "json",
			}, test.flags...)...)
			c.Assert(document.ExitCode, qt.Equals, 2, historyComment(document))
			failure := decodeHistoryFailure(c, document.Stdout)
			c.Assert(failure.ContractVersion, qt.Equals, drift.ContractVersion)
			c.Assert(failure.Command, qt.Equals, drift.Command)
			c.Assert(failure.Error, qt.IsNotNil)
			c.Assert(failure.Error.Message+"\n", qt.Matches, test.want)
		})
	}
}

// decodeHistoryFailure holds a document to the failure contract: exactly the
// fields it declares, and nothing after it.
func decodeHistoryFailure(c *qt.C, stdout string) errorkind.Failure {
	c.Helper()
	decoder := json.NewDecoder(strings.NewReader(stdout))
	decoder.DisallowUnknownFields()
	var failure errorkind.Failure
	c.Assert(decoder.Decode(&failure), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(decoder.More(), qt.IsFalse, qt.Commentf("stdout:\n%s", stdout))
	return failure
}

// TestTheDriftDocumentStaysInItsContractE2E compares a selected run's document with
// the document of a database that never held the tables. The two have the same
// fields, the same inventory and the same findings, so nothing of the excluded
// tables reaches a reader, and a reader of the contract meets no new field. Only
// the address differs, because the databases differ.
func TestTheDriftDocumentStaysInItsContractE2E(t *testing.T) {
	selectors := []string{"--exclude", historyChangeLog, "--exclude", historyLock, "--format", "json"}
	c := qt.New(t)
	selected, clean := historyDatabase(c, historyTables...), historyDatabase(c)
	schema := historySchemaFile(c, historyApplication)

	got := historyDrift(c, selected, schema, selectors...)
	want := historyDrift(c, clean, schema, "--format", "json")

	c.Assert(got.ExitCode, qt.Equals, 0, historyComment(got))
	c.Assert(want.ExitCode, qt.Equals, 0, historyComment(want))
	document, reference := decodeHistoryDocument(c, got.Stdout), decodeHistoryDocument(c, want.Stdout)
	c.Assert(document["contract_version"], qt.Equals, float64(drift.ContractVersion))
	c.Assert(document["command"], qt.Equals, drift.Command)
	c.Assert(slices.Sorted(maps.Keys(document)), qt.DeepEquals, slices.Sorted(maps.Keys(reference)))
	delete(document, "database_url")
	delete(reference, "database_url")
	c.Assert(document, qt.DeepEquals, reference)
	c.Assert(document["inventory"], qt.Not(qt.HasLen), 0)
}

func decodeHistoryDocument(c *qt.C, stdout string) map[string]any {
	c.Helper()
	var document map[string]any
	c.Assert(json.Unmarshal([]byte(stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	return document
}
