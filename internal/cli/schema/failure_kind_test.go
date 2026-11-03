package schema_test

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/drift"
	"ptah.run/internal/errorkind"
	"ptah.run/migration/safety"
)

// refusedPostgresURL is a PostgreSQL URL whose port nothing listens on.
func refusedPostgresURL(c *qt.C) string {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	address := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return "postgres://ptah:secret@" + address + "/app?sslmode=disable"
}

// postgresPlanFile writes a PostgreSQL plan with one statement, which apply
// reads and validates before it opens the target.
func postgresPlanFile(c *qt.C) string {
	c.Helper()
	document, err := atlasschema.MarshalPlanFile(atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "add_orders",
		Dialect:         platform.Postgres,
		FromFingerprint: "sha256:" + fmt.Sprintf("%064x", 1),
		ToFingerprint:   "sha256:" + fmt.Sprintf("%064x", 2),
		Statements: []atlasschema.PlanStatement{{
			SQL: `CREATE TABLE "orders" ("id" integer)`, Severity: safety.Safe, Reason: "create table orders",
		}},
	})
	c.Assert(err, qt.IsNil)
	path := filepath.Join(c.TempDir(), "add_orders.plan.json")
	c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)
	return path
}

// TestSchemaPlanJSONNamesAnUnreachableTarget is the failure the operator's
// plan Job reports when the database cannot be reached: the document carries
// the kind, and standard error carries the line a person reads.
func TestSchemaPlanJSONNamesAnUnreachableTarget(t *testing.T) {
	c := qt.New(t)
	schemaPath := writeSchemaSQLFile(c, c.TempDir(), "schema.sql", "CREATE TABLE orders (id integer);\n")

	stdout, stderr, err := runSchemaStreams("plan", "--db-url", refusedPostgresURL(c),
		"--schema-file", schemaPath, "--output", filepath.Join(c.TempDir(), "p.plan.json"), "--json")

	c.Assert(err, qt.IsNotNil)
	report := decodeOneDocument[atlasschema.PlanReport](c, stdout)
	c.Assert(report, qt.DeepEquals, atlasschema.PlanReport{
		ContractVersion: atlasschema.PlanReportContractVersion,
		Command:         atlasschema.PlanReportCommand,
		Outcome:         atlasschema.PlanOutcomeFailed,
		Error:           &errorkind.Report{Kind: errorkind.DatabaseUnreachable, Message: err.Error()},
	})
	c.Assert(stderr, qt.Equals, "error: "+err.Error()+"\n")
}

// TestSchemaPlanJSONGivesAnUnreachableRegistryNoKind is the control: the
// desired schema's registry refuses the dial before the target is opened, and
// that refusal says nothing about the database.
func TestSchemaPlanJSONGivesAnUnreachableRegistryNoKind(t *testing.T) {
	c := qt.New(t)
	listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(listenErr, qt.IsNil)
	registry := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)

	stdout, _, err := runSchemaStreams("plan", "--db-url", refusedPostgresURL(c),
		"--schema-file", "oci://"+registry+"/schemas/app:v1", "--plain-http", "--dry-run", "--json")

	c.Assert(err, qt.ErrorAs, new(*net.OpError))
	report := decodeOneDocument[atlasschema.PlanReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.PlanOutcomeFailed)
	c.Assert(report.Error, qt.DeepEquals, &errorkind.Report{Message: err.Error()})
}

// TestSchemaApplyJSONNamesAnUnreachableTarget covers the three ways the
// operator and its users run an apply: the approved plan, its dry run, and a
// plan computed from a desired schema.
func TestSchemaApplyJSONNamesAnUnreachableTarget(t *testing.T) {
	tests := []struct {
		name string
		// source names what the run reads: "plan" for a plan file, "schema"
		// for a desired schema.
		source string
		extra  []string
	}{
		{name: "an approved plan", source: "plan", extra: []string{"--auto-approve"}},
		{name: "a dry run of an approved plan", source: "plan", extra: []string{"--dry-run"}},
		{name: "a desired schema", source: "schema", extra: []string{"--auto-approve"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sources := map[string][]string{
				"plan":   {"--plan", postgresPlanFile(c)},
				"schema": {"--schema-file", writeSchemaSQLFile(c, c.TempDir(), "schema.sql", "CREATE TABLE orders (id integer);\n")},
			}
			args := append([]string{"apply", "--db-url", refusedPostgresURL(c), "--json"}, sources[test.source]...)
			args = append(args, test.extra...)

			stdout, stderr, err := runSchemaStreams(args...)

			c.Assert(err, qt.IsNotNil)
			report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
			c.Assert(report.ContractVersion, qt.Equals, atlasschema.ApplyReportContractVersion)
			c.Assert(report.Command, qt.Equals, atlasschema.ApplyReportCommand)
			c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
			c.Assert(report.Error, qt.DeepEquals, &errorkind.Report{Kind: errorkind.DatabaseUnreachable, Message: err.Error()})
			c.Assert(stderr, qt.Equals, "error: "+err.Error()+"\n")
		})
	}
}

// TestSchemaDriftProviderSetupFailureIsTheFailureDocument holds a provider
// setup failure, which stops drift before it runs, to the same document a
// failed run prints, beside the provider's own diagnostic.
func TestSchemaDriftProviderSetupFailureIsTheFailureDocument(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runSchemaStreams("drift", "--provider-engine=", "--format", "json")

	c.Assert(err, qt.IsNotNil)
	var document struct {
		errorkind.Failure
		ProviderDiagnostic json.RawMessage `json:"provider_diagnostic"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(document.ContractVersion, qt.Equals, drift.ContractVersion)
	c.Assert(document.Command, qt.Equals, "schema drift")
	c.Assert(document.Error, qt.DeepEquals, &errorkind.Report{Message: err.Error()})
}
