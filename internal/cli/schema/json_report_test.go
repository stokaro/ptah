package schema_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	digest "github.com/opencontainers/go-digest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/cli/schema"
	"ptah.run/migration/safety"
)

// runSchemaStreamsWithInput is runSchemaStreams with stdin, for the one row
// that answers the confirmation prompt. A --json caller parses one stream and
// a person reads the other, so the prompt has to be found on the second.
func runSchemaStreamsWithInput(stdin string, args ...string) (stdout, stderr string, err error) {
	cmd := schema.NewSchemaCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// decodeOneDocument reads stdout as exactly one JSON document of type T.
//
// Unknown fields are refused, so a field the command writes and the contract
// type does not declare fails here rather than reaching a consumer unnoticed,
// and a second value on the stream -- a human line, a second document -- fails
// too.
func decodeOneDocument[T any](c *qt.C, stdout string) T {
	c.Helper()
	decoder := json.NewDecoder(strings.NewReader(stdout))
	decoder.DisallowUnknownFields()
	var document T
	c.Assert(decoder.Decode(&document), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	var trailing json.RawMessage
	c.Assert(decoder.Decode(&trailing), qt.Equals, io.EOF, qt.Commentf("stdout:\n%s", stdout))
	return document
}

// planFixture is a SQLite target holding one table and a desired schema that
// adds a second, so a plan against it has exactly one statement.
type planFixture struct {
	dir        string
	dbPath     string
	dbURL      string
	schemaPath string
}

func newPlanFixture(c *qt.C) planFixture {
	c.Helper()
	dir := c.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	return planFixture{
		dir:    dir,
		dbPath: dbPath,
		dbURL:  "sqlite://" + dbPath,
		schemaPath: writeSchemaSQLFile(c, dir, "schema.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n"),
	}
}

// savePlan saves the fixture's plan to a file and returns its path.
func (f planFixture) savePlan(c *qt.C) string {
	c.Helper()
	planPath := filepath.Join(f.dir, "add-orders.plan.json")
	out, err := runSchema("", "plan", "--db-url", f.dbURL, "--schema-file", f.schemaPath, "--output", planPath)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return planPath
}

// regionRowsEntities declares one reference table and the file that carries
// its rows, which is what a protected table and a rows fingerprint need.
const regionRowsEntities = `package entities

//ptah:schema:data table="regions" key="code" file="regions.yaml"
//ptah:schema:table name="regions"
type Region struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`

// rowsFixture is a SQLite target whose regions table holds the row the
// entities declared, and whose declaration then moved to a different value.
type rowsFixture struct {
	dir      string
	dbPath   string
	dbURL    string
	entities string
}

func newRowsFixture(c *qt.C) rowsFixture {
	c.Helper()
	dir := c.TempDir()
	entities := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entities, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(entities, "schema.go"), []byte(regionRowsEntities), 0o600), qt.IsNil)
	rowsFile := filepath.Join(entities, "regions.yaml")
	c.Assert(os.WriteFile(rowsFile, []byte("- code: NO\n  name: Norway\n"), 0o600), qt.IsNil)
	dbPath := filepath.Join(dir, "target.db")
	f := rowsFixture{dir: dir, dbPath: dbPath, dbURL: "sqlite://" + dbPath, entities: entities}
	out, err := runSchema("", "apply", "--db-url", f.dbURL, "--root-dir", entities, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(os.WriteFile(rowsFile, []byte("- code: NO\n  name: Norge\n"), 0o600), qt.IsNil)
	return f
}

func fileDigest(c *qt.C, path string) string {
	c.Helper()
	contents, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	return digest.FromBytes(contents).String()
}

func TestSchemaPlanJSONReportsTheSavedPlan(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	planPath := filepath.Join(f.dir, "add-orders.plan.json")

	stdout, stderr, err := runSchemaStreams("plan",
		"--db-url", f.dbURL, "--schema-file", f.schemaPath, "--output", planPath, "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.PlanReport](c, stdout)
	saved, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(report.ContractVersion, qt.Equals, atlasschema.PlanReportContractVersion)
	c.Assert(report.Outcome, qt.Equals, atlasschema.PlanOutcomeChanges)
	c.Assert(report.PlanPath, qt.Equals, planPath)
	c.Assert(report.PlanDigest, qt.Equals, fileDigest(c, planPath))
	c.Assert(report.Plan, qt.DeepEquals, &saved)
	c.Assert(report.Plan.Statements, qt.HasLen, 1)
	c.Assert(report.Plan.Statements[0].SQL, qt.Contains, `CREATE TABLE "orders"`)
	c.Assert(report.Plan.Statements[0].Severity, qt.Equals, safety.Safe)
	c.Assert(report.Refusal, qt.IsNil)
	c.Assert(stderr, qt.Contains, "Plan saved to file://"+planPath)
}

// TestSchemaPlanJSONDigestNamesTheDocumentDryRunPrints holds the digest to the
// bytes a caller can obtain without --json, which is what makes it an identity
// rather than a checksum of an encoding only the report ever held.
func TestSchemaPlanJSONDigestNamesTheDocumentDryRunPrints(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	document, stderr, err := runSchemaStreams("plan",
		"--db-url", f.dbURL, "--schema-file", f.schemaPath, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))

	stdout, stderr, err := runSchemaStreams("plan",
		"--db-url", f.dbURL, "--schema-file", f.schemaPath, "--dry-run", "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.PlanReport](c, stdout)
	previewed := decodeOneDocument[atlasschema.PlanFile](c, document)
	c.Assert(report.Outcome, qt.Equals, atlasschema.PlanOutcomeChanges)
	c.Assert(report.PlanDigest, qt.Equals, digest.FromString(document).String())
	c.Assert(report.Plan, qt.DeepEquals, &previewed)
	c.Assert(report.PlanPath, qt.Equals, "")
}

// TestSchemaPlanJSONIsByteStableAcrossReads is what a caller that plans twice
// and compares the two answers relies on: nothing in the document depends on
// when it was written.
func TestSchemaPlanJSONIsByteStableAcrossReads(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	args := []string{"plan", "--db-url", f.dbURL, "--schema-file", f.schemaPath, "--dry-run", "--json"}

	first, _, err := runSchemaStreams(args...)
	c.Assert(err, qt.IsNil)
	second, _, err := runSchemaStreams(args...)
	c.Assert(err, qt.IsNil)

	c.Assert(second, qt.Equals, first)
	c.Assert(decodeOneDocument[atlasschema.PlanReport](c, first).Outcome, qt.Equals, atlasschema.PlanOutcomeChanges)
}

func TestSchemaPlanJSONReportsNoChanges(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	synced := writeSchemaSQLFile(c, f.dir, "synced.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	t.Chdir(f.dir)

	stdout, stderr, err := runSchemaStreams("plan",
		"--db-url", f.dbURL, "--schema-file", synced, "--save", "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(decodeOneDocument[atlasschema.PlanReport](c, stdout), qt.DeepEquals, atlasschema.PlanReport{
		ContractVersion: atlasschema.PlanReportContractVersion,
		Outcome:         atlasschema.PlanOutcomeNoChanges,
	})
	c.Assert(stderr, qt.Contains, "Schema is synced, no changes to be made.")
	saved, err := filepath.Glob(filepath.Join(f.dir, "*.plan.json"))
	c.Assert(err, qt.IsNil)
	c.Assert(saved, qt.HasLen, 0)
}

// TestSchemaPlanJSONReportsAProtectedTableRefusal carries its own control: the
// same change without the fence plans, so the refusal is the fence's and not
// a failure the fixture caused.
func TestSchemaPlanJSONReportsAProtectedTableRefusal(t *testing.T) {
	c := qt.New(t)
	f := newRowsFixture(c)
	args := []string{"plan", "--db-url", f.dbURL, "--root-dir", f.entities, "--dry-run", "--json"}

	stdout, stderr, err := runSchemaStreams(append(args, "--protected-table", "regions")...)

	c.Assert(err, qt.ErrorMatches, `refusing to change protected table\(s\) regions: .*`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	report := decodeOneDocument[atlasschema.PlanReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.PlanOutcomeRefused)
	c.Assert(report.Refusal, qt.DeepEquals, &atlasschema.Refusal{
		Code:   atlasschema.RefusalProtectedTable,
		Tables: []string{"regions"},
	})
	c.Assert(report.Error, qt.Equals, err.Error())
	c.Assert(report.Plan, qt.IsNil)
	c.Assert(stderr, qt.Contains, "error: refusing to change protected table(s) regions")

	control, controlStderr, err := runSchemaStreams(args...)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", controlStderr))
	c.Assert(decodeOneDocument[atlasschema.PlanReport](c, control).Outcome, qt.Equals, atlasschema.PlanOutcomeChanges)
}

func TestSchemaPlanJSONReportsAFailure(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)

	stdout, stderr, err := runSchemaStreams("plan",
		"--db-url", f.dbURL, "--schema-file", filepath.Join(f.dir, "missing.sql"), "--dry-run", "--json")

	c.Assert(err, qt.ErrorMatches, `error parsing schema file: schema file does not exist: .*missing\.sql`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(decodeOneDocument[atlasschema.PlanReport](c, stdout), qt.DeepEquals, atlasschema.PlanReport{
		ContractVersion: atlasschema.PlanReportContractVersion,
		Outcome:         atlasschema.PlanOutcomeFailed,
		Error:           err.Error(),
	})
	c.Assert(stderr, qt.Equals, "error: "+err.Error()+"\n")
}

// TestSchemaPlanWithoutJSONKeepsItsOutput pins the human output byte for byte,
// on the stream it has always used. Callers that read it exist, and --json is
// an addition beside it rather than a replacement for it.
func TestSchemaPlanWithoutJSONKeepsItsOutput(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	synced := writeSchemaSQLFile(c, f.dir, "synced.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	planPath := filepath.Join(f.dir, "add-orders.plan.json")

	syncedOut, syncedErr, err := runSchemaStreams("plan",
		"--db-url", f.dbURL, "--schema-file", synced, "--dry-run")
	c.Assert(err, qt.IsNil)
	savedOut, savedErr, err := runSchemaStreams("plan",
		"--db-url", f.dbURL, "--schema-file", f.schemaPath, "--output", planPath)
	c.Assert(err, qt.IsNil)

	c.Assert(syncedOut, qt.Equals, "Schema is synced, no changes to be made.\n")
	c.Assert(syncedErr, qt.Equals, "")
	c.Assert(savedOut, qt.Equals, "Planned schema changes:\n"+
		"CREATE TABLE \"orders\" (\n  \"id\" INTEGER PRIMARY KEY\n);\n"+
		"Plan saved to file://"+planPath+"\n")
	c.Assert(savedErr, qt.Equals, "")
}

func TestSchemaApplyJSONReportsAnAppliedPlan(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)

	stdout, stderr, err := runSchemaStreams("apply",
		"--db-url", f.dbURL, "--plan", planPath, "--auto-approve", "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout), qt.DeepEquals, atlasschema.ApplyReport{
		ContractVersion: atlasschema.ApplyReportContractVersion,
		Outcome:         atlasschema.ApplyOutcomeApplied,
		PlanName:        plan.Name,
		PlanDigest:      fileDigest(c, planPath),
		Statements:      plan.StatementSQL(),
	})
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"orders", "users"})
	c.Assert(stderr, qt.Contains, "Schema apply completed successfully.")
}

// TestSchemaApplyJSONDryRunVerifiesThePlanAndChangesNothing is the rehearsal a
// caller runs before it asks for approval: the plan was read, its fingerprint
// held, and nothing ran.
func TestSchemaApplyJSONDryRunVerifiesThePlanAndChangesNothing(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)

	stdout, stderr, err := runSchemaStreams("apply",
		"--db-url", f.dbURL, "--plan", planPath, "--dry-run", "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout), qt.DeepEquals, atlasschema.ApplyReport{
		ContractVersion: atlasschema.ApplyReportContractVersion,
		Outcome:         atlasschema.ApplyOutcomeDryRun,
		PlanName:        plan.Name,
		PlanDigest:      fileDigest(c, planPath),
		Statements:      plan.StatementSQL(),
	})
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"users"})
}

func TestSchemaApplyJSONReportsAStalePlan(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	seedSQLite(c, f.dbPath, "CREATE TABLE drifted (id INTEGER PRIMARY KEY);")

	stdout, stderr, err := runSchemaStreams("apply",
		"--db-url", f.dbURL, "--plan", planPath, "--auto-approve", "--json")

	c.Assert(err, qt.ErrorMatches, "pre-planned migration is stale: .*")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
	c.Assert(report.Refusal, qt.IsNotNil)
	c.Assert(report.Refusal.Code, qt.Equals, atlasschema.RefusalStalePlan)
	c.Assert(report.Refusal.Changed, qt.Equals, "schema")
	c.Assert(report.Refusal.PlanFingerprint, qt.Equals, plan.FromFingerprint)
	c.Assert(report.Refusal.DatabaseFingerprint, qt.Matches, `sha256:[0-9a-f]{64}`)
	c.Assert(report.Refusal.DatabaseFingerprint, qt.Not(qt.Equals), plan.FromFingerprint)
	c.Assert(report.PlanDigest, qt.Equals, fileDigest(c, planPath))
	c.Assert(report.Statements, qt.IsNil)
	c.Assert(report.Error, qt.Equals, err.Error())
	c.Assert(stderr, qt.Contains, "error: pre-planned migration is stale")
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"drifted", "users"})
}

// TestSchemaApplyJSONReportsStaleRows is the stale plan whose structure still
// matches: a declared row moved after planning, and the refusal says so with
// the rows fingerprints rather than the schema ones.
func TestSchemaApplyJSONReportsStaleRows(t *testing.T) {
	c := qt.New(t)
	f := newRowsFixture(c)
	planPath := filepath.Join(f.dir, "rename-region.plan.json")
	out, err := runSchema("", "plan", "--db-url", f.dbURL, "--root-dir", f.entities, "--output", planPath)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	seedSQLite(c, f.dbPath, "UPDATE regions SET name = 'Noreg' WHERE code = 'NO';")

	stdout, stderr, err := runSchemaStreams("apply",
		"--db-url", f.dbURL, "--plan", planPath, "--auto-approve", "--json")

	c.Assert(err, qt.ErrorMatches, "pre-planned migration is stale: the declared rows .*", qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
	c.Assert(report.Refusal, qt.IsNotNil)
	c.Assert(report.Refusal.Code, qt.Equals, atlasschema.RefusalStalePlan)
	c.Assert(report.Refusal.Changed, qt.Equals, "rows")
	c.Assert(report.Refusal.PlanFingerprint, qt.Equals, plan.RowsFingerprint)
	c.Assert(report.Refusal.DatabaseFingerprint, qt.Not(qt.Equals), plan.RowsFingerprint)
}

func TestSchemaApplyJSONReportsAProtectedTableRefusal(t *testing.T) {
	c := qt.New(t)
	f := newRowsFixture(c)

	stdout, _, err := runSchemaStreams("apply",
		"--db-url", f.dbURL, "--root-dir", f.entities, "--protected-table", "regions", "--auto-approve", "--json")

	c.Assert(err, qt.ErrorMatches, `refusing to change protected table\(s\) regions: .*`)
	c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout), qt.DeepEquals, atlasschema.ApplyReport{
		ContractVersion: atlasschema.ApplyReportContractVersion,
		Outcome:         atlasschema.ApplyOutcomeRefused,
		Refusal:         &atlasschema.Refusal{Code: atlasschema.RefusalProtectedTable, Tables: []string{"regions"}},
		Error:           err.Error(),
	})
}

func TestSchemaApplyJSONReportsNoChanges(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	synced := writeSchemaSQLFile(c, f.dir, "synced.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	stdout, stderr, err := runSchemaStreams("apply",
		"--db-url", f.dbURL, "--schema-file", synced, "--auto-approve", "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout), qt.DeepEquals, atlasschema.ApplyReport{
		ContractVersion: atlasschema.ApplyReportContractVersion,
		Outcome:         atlasschema.ApplyOutcomeNoChanges,
	})
	c.Assert(stderr, qt.Contains, "Schema is synced, no changes to be made.")
}

// TestSchemaApplyJSONReportsADeclinedConfirmation keeps the prompt usable
// under --json: it is written for a person, so it goes where a person reads.
func TestSchemaApplyJSONReportsADeclinedConfirmation(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)

	stdout, stderr, err := runSchemaStreamsWithInput("no\n", "apply",
		"--db-url", f.dbURL, "--schema-file", f.schemaPath, "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeCanceled)
	c.Assert(report.Statements, qt.HasLen, 1)
	c.Assert(report.Statements[0], qt.Contains, `CREATE TABLE "orders"`)
	c.Assert(stderr, qt.Contains, "Apply these schema changes? Type 'YES' to confirm: Schema apply canceled.")
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"users"})
}

// TestSchemaApplyJSONReportsAnExecutionFailureAsUnknown is the outcome a
// caller must not retry from. The second statement fails once the first has
// been sent, and the report does not guess what the database kept.
func TestSchemaApplyJSONReportsAnExecutionFailureAsUnknown(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	plan.Statements = append(plan.Statements, plan.Statements[0])
	document, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(planPath, document, 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaStreams("apply",
		"--db-url", f.dbURL, "--plan", planPath, "--auto-approve", "--json")

	c.Assert(err, qt.ErrorMatches, "(?s)apply schema changes: .*", qt.Commentf("stderr:\n%s", stderr))
	c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout), qt.DeepEquals, atlasschema.ApplyReport{
		ContractVersion: atlasschema.ApplyReportContractVersion,
		Outcome:         atlasschema.ApplyOutcomeUnknown,
		PlanName:        plan.Name,
		PlanDigest:      digest.FromBytes(document).String(),
		Statements:      plan.StatementSQL(),
		Error:           err.Error(),
	})
	c.Assert(stderr, qt.Not(qt.Contains), "Schema apply completed successfully.")
}

func TestSchemaApplyJSONReportsAFailureBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)

	stdout, stderr, err := runSchemaStreams("apply",
		"--db-url", f.dbURL, "--plan", filepath.Join(f.dir, "missing.plan.json"), "--auto-approve", "--json")

	c.Assert(err, qt.ErrorMatches, "read plan file: .*")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout), qt.DeepEquals, atlasschema.ApplyReport{
		ContractVersion: atlasschema.ApplyReportContractVersion,
		Outcome:         atlasschema.ApplyOutcomeFailed,
		Error:           err.Error(),
	})
	c.Assert(stderr, qt.Equals, "error: "+err.Error()+"\n")
}

// TestSchemaApplyWithoutJSONKeepsItsTranscript pins the transcript an apply of
// a saved plan prints, byte for byte and on the stream it has always used.
func TestSchemaApplyWithoutJSONKeepsItsTranscript(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	planPath := f.savePlan(c)

	stdout, stderr, err := runSchemaStreams("apply", "--db-url", f.dbURL, "--plan", planPath, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Equals, "Planned schema changes:\n"+
		"CREATE TABLE \"orders\" (\n  \"id\" INTEGER PRIMARY KEY\n);\n"+
		"Auto-approval enabled; applying schema changes.\n"+
		"Schema apply completed successfully.\n")
	c.Assert(stderr, qt.Equals, "")
}
