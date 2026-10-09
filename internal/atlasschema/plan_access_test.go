package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/safety"
)

const editedOwnerReason = "the edit changed a statement that carried an owner verdict; " +
	"its effect, access included, is unknown and manual review is required"

// An edited plan re-reads every statement from its text, and SQL text cannot
// say what an owner operation does. A statement the edit left alone keeps the
// owner verdict it was planned with, whichever verdict its text earns; a
// statement the edit introduced while every owned statement stayed has only
// its text verdict.
func TestPlanFileWithStatementsFromSQLKeepsTheOwnersVerdict(t *testing.T) {
	tests := []struct {
		name         string
		recorded     atlasschema.PlanStatement
		wantSeverity safety.Severity
		wantReason   string
	}{
		{
			name: "the recorded verdict is higher than the text",
			recorded: atlasschema.PlanStatement{
				SQL: "CREATE POLICY p ON t USING (true)", Severity: safety.Destructive, Owned: true,
				Reason: "can widen access: admits every row", Access: schemaext.AccessWidens, AccessReason: "admits every row",
			},
			wantSeverity: safety.Destructive, wantReason: "can widen access: admits every row",
		},
		{
			name: "the text is at least as high as the recorded verdict",
			recorded: atlasschema.PlanStatement{
				SQL: "DROP POLICY p ON t", Severity: safety.Warning, Owned: true,
				Reason: "can narrow access: a permissive policy goes away", Access: schemaext.AccessNarrows, AccessReason: "a permissive policy goes away",
			},
			wantSeverity: safety.Destructive, wantReason: "DROP POLICY removes an access-control protection",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			plan := atlasschema.PlanFile{Dialect: "postgres", Statements: []atlasschema.PlanStatement{tc.recorded}}

			edited := plan.WithStatementsFromSQL(tc.recorded.SQL + ";\nCREATE TABLE fresh (id integer);\n")

			c.Assert(edited.Statements, qt.HasLen, 2)
			c.Assert(edited.Statements[0].Severity, qt.Equals, tc.wantSeverity)
			c.Assert(edited.Statements[0].Reason, qt.Equals, tc.wantReason)
			c.Assert(edited.Statements[0].Owned, qt.IsTrue)
			c.Assert(edited.Statements[0].Access, qt.Equals, tc.recorded.Access)
			c.Assert(edited.Statements[0].AccessReason, qt.Equals, tc.recorded.AccessReason)
			c.Assert(edited.Statements[1], qt.DeepEquals, atlasschema.PlanStatement{
				SQL: "CREATE TABLE fresh (id integer)", Severity: safety.Safe, Reason: "does not remove data or tighten constraints",
			})
		})
	}
}

var (
	openPolicy = atlasschema.PlanStatement{
		SQL: "CREATE POLICY open ON t USING (true)", Severity: safety.Safe, Owned: true,
		Reason: "does not remove data or tighten constraints", Access: schemaext.AccessUnchanged, AccessReason: "a restrictive policy still limits it",
	}
	restrictivePolicy = atlasschema.PlanStatement{
		SQL: "CREATE POLICY tenant ON t AS RESTRICTIVE USING (tenant = current_user)", Severity: safety.Warning, Owned: true,
		Reason: "can narrow access: hides other tenants' rows", Access: schemaext.AccessNarrows, AccessReason: "hides other tenants' rows",
	}
	rowTTL = atlasschema.PlanStatement{
		SQL: "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')", Severity: safety.Warning, Owned: true,
		Reason: "row-level TTL decides which rows a background job deletes",
	}
	commonTable = atlasschema.PlanStatement{
		SQL: "CREATE TABLE kept (id integer)", Severity: safety.Safe, Reason: "does not remove data or tighten constraints",
	}
)

// An owner verdict cannot be read back out of SQL text, and an access verdict
// was made beside sibling statements. Once an edit changes or removes a
// statement that carried an owner verdict, every remaining owned statement and
// every statement the edit introduced is raised to Destructive with an unknown
// access effect. The raise keeps the reason the statement had.
func TestPlanFileWithStatementsFromSQLFailsClosedWhenAnOwnedStatementIsEdited(t *testing.T) {
	tests := []struct {
		name     string
		recorded []atlasschema.PlanStatement
		edited   string
		want     []atlasschema.PlanStatement
	}{
		{
			name:     "an owned statement rewritten",
			recorded: []atlasschema.PlanStatement{restrictivePolicy, commonTable},
			edited:   "CREATE POLICY tenant ON t AS RESTRICTIVE USING (true);\nCREATE TABLE kept (id integer);\n",
			want: []atlasschema.PlanStatement{
				{SQL: "CREATE POLICY tenant ON t AS RESTRICTIVE USING (true)", Severity: safety.Destructive, Owned: true,
					Reason: "does not remove data or tighten constraints; " + editedOwnerReason,
					Access: schemaext.AccessUnknown, AccessReason: editedOwnerReason},
				commonTable,
			},
		},
		{
			name:     "a sibling policy removed",
			recorded: []atlasschema.PlanStatement{openPolicy, restrictivePolicy},
			edited:   openPolicy.SQL + ";\n",
			want: []atlasschema.PlanStatement{
				{SQL: openPolicy.SQL, Severity: safety.Destructive, Owned: true,
					Reason: openPolicy.Reason + "; " + editedOwnerReason,
					Access: schemaext.AccessUnknown, AccessReason: editedOwnerReason},
			},
		},
		{
			name:     "a lifecycle owner verdict rewritten",
			recorded: []atlasschema.PlanStatement{rowTTL},
			edited:   "ALTER TABLE sessions SET (ttl_expiration_expression = 'created_at');\n",
			want: []atlasschema.PlanStatement{
				{SQL: "ALTER TABLE sessions SET (ttl_expiration_expression = 'created_at')", Severity: safety.Destructive, Owned: true,
					Reason: "does not remove data or tighten constraints; " + editedOwnerReason,
					Access: schemaext.AccessUnknown, AccessReason: editedOwnerReason},
			},
		},
		{
			name:     "an introduced statement keeps its own reason",
			recorded: []atlasschema.PlanStatement{rowTTL},
			edited:   "DROP TABLE sessions;\n",
			want: []atlasschema.PlanStatement{
				{SQL: "DROP TABLE sessions", Severity: safety.Destructive, Owned: true,
					Reason: "DROP TABLE removes the table and all rows; " + editedOwnerReason,
					Access: schemaext.AccessUnknown, AccessReason: editedOwnerReason},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			plan := atlasschema.PlanFile{Dialect: "postgres", Statements: tc.recorded}

			edited := plan.WithStatementsFromSQL(tc.edited)

			c.Assert(edited.Statements, qt.DeepEquals, tc.want)
			c.Assert(edited.Destructive, qt.IsTrue)
		})
	}
}

// Two statements with the same text pair with their own recorded statements,
// in order, rather than both taking whichever was recorded last.
func TestPlanFileWithStatementsFromSQLPairsDuplicatesInOrder(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{Dialect: "postgres", Statements: []atlasschema.PlanStatement{
		{SQL: "SELECT 1", Severity: safety.Warning, Owned: true, Reason: "can narrow access: r",
			Access: schemaext.AccessNarrows, AccessReason: "r"},
		{SQL: "SELECT 1", Severity: safety.Safe, Reason: "does not remove data or tighten constraints"},
	}}

	edited := plan.WithStatementsFromSQL("SELECT 1;\nSELECT 1;\n")

	c.Assert(edited.Statements, qt.DeepEquals, plan.Statements)
}

func TestDecodePlanFile_FailurePath_RefusesAnAccessAssessmentThePlannerCouldNotWrite(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{name: "unrecognized access", statement: `{"sql":"CREATE POLICY p ON t","severity":"destructive","reason":"r","owned":true,"access":"safe","access_reason":"r"}`,
			want: `.*unrecognized access assessment "safe".*`},
		{name: "access without a reason", statement: `{"sql":"CREATE POLICY p ON t","severity":"destructive","reason":"r","owned":true,"access":"widens"}`,
			want: `.*needs a reason on one trimmed line.*`},
		{name: "a reason without access", statement: `{"sql":"CREATE POLICY p ON t","severity":"safe","reason":"r","access_reason":"r"}`,
			want: `.*access_reason is recorded without access.*`},
		{name: "access on a statement no owner rendered", statement: `{"sql":"CREATE POLICY p ON t","severity":"destructive","reason":"r","access":"widens","access_reason":"r"}`,
			want: `.*access "widens" is recorded on a statement no owner operation rendered.*`},
		{name: "a widening recorded as safe", statement: `{"sql":"CREATE POLICY p ON t","severity":"safe","reason":"r","owned":true,"access":"widens","access_reason":"r"}`,
			want: `.*access "widens" requires severity destructive or higher, and the statement records safe.*`},
		{name: "a narrowing recorded as safe", statement: `{"sql":"CREATE POLICY p ON t","severity":"safe","reason":"r","owned":true,"access":"narrows","access_reason":"r"}`,
			want: `.*access "narrows" requires severity warning or higher, and the statement records safe.*`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			document := `{"format_version":1,"name":"p","dialect":"postgres","from_fingerprint":"sha256:` + fingerprintHex('a') +
				`","to_fingerprint":"sha256:` + fingerprintHex('b') + `","destructive":false,"statements":[` + tc.statement + `]}`
			plan, digest, err := atlasschema.DecodePlanFile([]byte(document), "p.plan.json")
			c.Assert(err, qt.ErrorMatches, `(?s)invalid plan file p\.plan\.json: plan statement 1: `+tc.want)
			c.Assert(plan.Statements, qt.HasLen, 0)
			c.Assert(digest, qt.Equals, "")
		})
	}
}

func TestDecodePlanFile_HappyPath_ReadsARecordedAccessAssessment(t *testing.T) {
	c := qt.New(t)
	document := `{"format_version":1,"name":"p","dialect":"postgres","from_fingerprint":"sha256:` + fingerprintHex('a') +
		`","to_fingerprint":"sha256:` + fingerprintHex('b') + `","destructive":true,"statements":[` +
		`{"sql":"CREATE POLICY p ON t","severity":"destructive","reason":"can widen access: r","owned":true,"access":"widens","access_reason":"r"}]}`
	plan, _, err := atlasschema.DecodePlanFile([]byte(document), "p.plan.json")
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.DeepEquals, []atlasschema.PlanStatement{{
		SQL: "CREATE POLICY p ON t", Severity: safety.Destructive, Reason: "can widen access: r",
		Owned: true, Access: schemaext.AccessWidens, AccessReason: "r",
	}})
}
