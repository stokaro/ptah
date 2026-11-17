package atlasschema_test

import (
	"encoding/json"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/errorkind"
	"ptah.run/migration/migrator"
)

// ownerRefusals are the three refusals the owner check raises before a plan
// sends a statement, each with a hint the refusal does not carry, since the
// message says it.
func ownerRefusals() (missing *atlasschema.MissingOwnerRoleError, transfer *atlasschema.OwnerTransferError, schema *atlasschema.OwnerSchemaError) {
	missing = &atlasschema.MissingOwnerRoleError{
		Statement: `ALTER VIEW "public"."my_docs" OWNER TO "reporting"`, Kind: "VIEW", Role: "reporting", Hint: "h",
	}
	transfer = &atlasschema.OwnerTransferError{
		Statement: `ALTER VIEW "public"."my_docs" OWNER TO "rv_a"`, Kind: "VIEW", Role: "rv_a",
		Connected: "app", Privilege: "SET", Holds: true, Hint: "h",
	}
	schema = &atlasschema.OwnerSchemaError{
		Statement: `ALTER MATERIALIZED VIEW "Sales"."Doc Totals" OWNER TO "Audit Team"`, Kind: "MATERIALIZED VIEW",
		Role: "Audit Team", Schema: "Sales", Holds: true, Hint: "h",
	}
	return missing, transfer, schema
}

// asStatementFailure wraps err the way a run wraps a statement it stopped at,
// and the command wraps the run.
func asStatementFailure(err error) error {
	return fmt.Errorf("apply schema changes: %w", &migrator.MigrationExecutionError{
		Err:            fmt.Errorf("failed to execute SQL statement: %w", err),
		Statement:      `ALTER VIEW "public"."my_docs" OWNER TO "rv_a"`,
		StatementIndex: 1,
		Total:          1,
	})
}

// TestNewApplyReportOwnerRefusal_HappyPath holds an apply that stopped at the
// owner check, before it sent a statement, to the refusal code of the check
// that stopped it and the statement, kind, role and server facts that code
// describes. The error is classified bare, as the command wraps it, and under
// a statement failure, so the code does not depend on where the run wrapped
// it.
func TestNewApplyReportOwnerRefusal_HappyPath(t *testing.T) {
	missing, transfer, schema := ownerRefusals()
	missingWant := &atlasschema.Refusal{Code: atlasschema.RefusalMissingOwnerRole, Owner: &atlasschema.OwnerRefusal{
		Statement: missing.Statement, Kind: "VIEW", Role: "reporting",
	}}
	transferWant := &atlasschema.Refusal{Code: atlasschema.RefusalOwnerTransfer, Owner: &atlasschema.OwnerRefusal{
		Statement: transfer.Statement, Kind: "VIEW", Role: "rv_a", Connected: "app", Privilege: "SET", Holds: true,
	}}
	schemaWant := &atlasschema.Refusal{Code: atlasschema.RefusalOwnerSchemaPrivilege, Owner: &atlasschema.OwnerRefusal{
		Statement: schema.Statement, Kind: "MATERIALIZED VIEW", Role: "Audit Team", Schema: "Sales", Holds: true,
	}}
	tests := []struct {
		name string
		err  error
		want *atlasschema.Refusal
	}{
		{name: "a role the server does not hold", err: missing, want: missingWant},
		{name: "a role the server does not hold, as the command wraps it", err: fmt.Errorf("apply schema changes: %w", missing), want: missingWant},
		{name: "a role the server does not hold, under a statement failure", err: asStatementFailure(missing), want: missingWant},
		{name: "an owner the connection may not give", err: transfer, want: transferWant},
		{name: "an owner the connection may not give, as the command wraps it", err: fmt.Errorf("apply schema changes: %w", transfer), want: transferWant},
		{name: "an owner the connection may not give, under a statement failure", err: asStatementFailure(transfer), want: transferWant},
		{name: "an owner without CREATE on the schema", err: schema, want: schemaWant},
		{name: "an owner without CREATE on the schema, as the command wraps it", err: fmt.Errorf("apply schema changes: %w", schema), want: schemaWant},
		{name: "an owner without CREATE on the schema, under a statement failure", err: asStatementFailure(schema), want: schemaWant},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			units := atlasschema.SQLApplyUnitReports([]string{`ALTER VIEW "public"."my_docs" OWNER TO "rv_a"`})

			got := atlasschema.NewApplyReport(atlasschema.ApplyEvidence{Units: units, Err: test.err})

			c.Assert(got, qt.DeepEquals, atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Command:         atlasschema.ApplyReportCommand,
				Outcome:         atlasschema.ApplyOutcomeRefused,
				Units:           units,
				Refusal:         test.want,
				Error:           &errorkind.Report{Message: test.err.Error()},
			})
		})
	}
}

// TestNewApplyReportOwnerRefusal_FailurePath holds the owner refusals to no
// owner code where the target's owner check is not what stopped the run. Once
// a statement was sent the outcome is unknown and names no refusal, because a
// refusal promises that nothing reached the database. A refusal raised by the
// rehearsal on the dev database is the dev server's answer about its own
// roles, so it is simulation-failed and carries no owner.
func TestNewApplyReportOwnerRefusal_FailurePath(t *testing.T) {
	missing, transfer, schema := ownerRefusals()
	tests := []struct {
		name        string
		evidence    atlasschema.ApplyEvidence
		wantOutcome atlasschema.ApplyOutcome
		wantRefusal *atlasschema.Refusal
	}{
		{
			name:        "a role the server does not hold, after a statement was sent",
			evidence:    atlasschema.ApplyEvidence{Dispatched: true, Err: missing},
			wantOutcome: atlasschema.ApplyOutcomeUnknown,
		},
		{
			name:        "an owner the connection may not give, after a statement was sent",
			evidence:    atlasschema.ApplyEvidence{Dispatched: true, Err: transfer},
			wantOutcome: atlasschema.ApplyOutcomeUnknown,
		},
		{
			name:        "an owner without CREATE on the schema, after a statement was sent",
			evidence:    atlasschema.ApplyEvidence{Dispatched: true, Err: schema},
			wantOutcome: atlasschema.ApplyOutcomeUnknown,
		},
		{
			name:        "a role the dev server does not hold",
			evidence:    atlasschema.ApplyEvidence{Err: &atlasschema.SimulationError{Stage: "plan", Err: missing}},
			wantOutcome: atlasschema.ApplyOutcomeRefused,
			wantRefusal: &atlasschema.Refusal{Code: atlasschema.RefusalSimulationFailed},
		},
		{
			name:        "an owner the dev connection may not give",
			evidence:    atlasschema.ApplyEvidence{Err: &atlasschema.SimulationError{Stage: "plan", Err: transfer}},
			wantOutcome: atlasschema.ApplyOutcomeRefused,
			wantRefusal: &atlasschema.Refusal{Code: atlasschema.RefusalSimulationFailed},
		},
		{
			name:        "an owner without CREATE on the dev schema",
			evidence:    atlasschema.ApplyEvidence{Err: &atlasschema.SimulationError{Stage: "plan", Err: schema}},
			wantOutcome: atlasschema.ApplyOutcomeRefused,
			wantRefusal: &atlasschema.Refusal{Code: atlasschema.RefusalSimulationFailed},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasschema.NewApplyReport(test.evidence)

			c.Assert(got.Outcome, qt.Equals, test.wantOutcome)
			c.Assert(got.Refusal, qt.DeepEquals, test.wantRefusal)
			c.Assert(got.Error, qt.DeepEquals, &errorkind.Report{Message: test.evidence.Err.Error()})
		})
	}
}

// TestRefusal_OwnerWireNames pins the field names a consumer parses, as
// TestReports_WireNames does for the fields of the other codes. A refusal
// that describes fewer facts leaves the others out rather than writing them
// empty, so the second row is the shape of a missing-owner-role refusal.
func TestRefusal_OwnerWireNames(t *testing.T) {
	tests := []struct {
		name    string
		refusal atlasschema.Refusal
		want    string
	}{
		{
			name: "every field",
			refusal: atlasschema.Refusal{Code: atlasschema.RefusalOwnerTransfer, Owner: &atlasschema.OwnerRefusal{
				Statement: "S", Kind: "VIEW", Role: "r", Connected: "a", Privilege: "SET", Schema: "s", Holds: true,
			}},
			want: `{"code":"owner-transfer","owner":{"statement":"S","kind":"VIEW","role":"r",` +
				`"connected":"a","privilege":"SET","schema":"s","holds":true}}`,
		},
		{
			name: "the statement, the kind and the role",
			refusal: atlasschema.Refusal{Code: atlasschema.RefusalMissingOwnerRole, Owner: &atlasschema.OwnerRefusal{
				Statement: "S", Kind: "VIEW", Role: "r",
			}},
			want: `{"code":"missing-owner-role","owner":{"statement":"S","kind":"VIEW","role":"r"}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := json.Marshal(test.refusal)

			c.Assert(err, qt.IsNil)
			c.Assert(string(got), qt.Equals, test.want)
		})
	}
}
