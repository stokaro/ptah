package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/safety"
)

// An edited plan re-reads every statement from its text, and SQL text cannot
// say what an owned operation does to access. A statement the edit left alone
// keeps the owner's access assessment it was planned with, whichever verdict
// its text earns; a statement the edit introduced has none.
func TestPlanFileWithStatementsFromSQLKeepsTheOwnersAccessAssessment(t *testing.T) {
	tests := []struct {
		name         string
		recorded     atlasschema.PlanStatement
		wantSeverity safety.Severity
		wantReason   string
	}{
		{
			name: "the recorded verdict is higher than the text",
			recorded: atlasschema.PlanStatement{
				SQL: "CREATE POLICY p ON t USING (true)", Severity: safety.Destructive,
				Reason: "can widen access: admits every row", Access: schemaext.AccessWidens, AccessReason: "admits every row",
			},
			wantSeverity: safety.Destructive, wantReason: "can widen access: admits every row",
		},
		{
			name: "the text is at least as high as the recorded verdict",
			recorded: atlasschema.PlanStatement{
				SQL: "DROP POLICY p ON t", Severity: safety.Warning,
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
			c.Assert(edited.Statements[0].Access, qt.Equals, tc.recorded.Access)
			c.Assert(edited.Statements[0].AccessReason, qt.Equals, tc.recorded.AccessReason)
			c.Assert(edited.Statements[1].Access, qt.Equals, schemaext.Access(""))
			c.Assert(edited.Statements[1].AccessReason, qt.Equals, "")
		})
	}
}

// A statement the edit introduced may be an owned statement rewritten, and its
// text cannot say what it does to access. When the edit changed or removed a
// statement that carried an access assessment, an introduced statement is
// destructive with an unknown access effect, not the verdict its text earns.
func TestPlanFileWithStatementsFromSQLFailsClosedWhenAnOwnedStatementIsEdited(t *testing.T) {
	tests := []struct {
		name   string
		edited string
	}{
		{name: "the owned statement rewritten", edited: "CREATE POLICY p ON t USING (true);\n"},
		{name: "the owned statement removed and another added", edited: "CREATE TABLE fresh (id integer);\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			plan := atlasschema.PlanFile{Dialect: "postgres", Statements: []atlasschema.PlanStatement{{
				SQL: "CREATE POLICY p ON t USING (tenant = current_user)", Severity: safety.Warning,
				Reason: "can narrow access: hides other tenants' rows", Access: schemaext.AccessNarrows, AccessReason: "hides other tenants' rows",
			}}}

			edited := plan.WithStatementsFromSQL(tc.edited)

			c.Assert(edited.Statements, qt.HasLen, 1)
			c.Assert(edited.Statements[0].Severity, qt.Equals, safety.Destructive)
			c.Assert(edited.Statements[0].Access, qt.Equals, schemaext.AccessUnknown)
			c.Assert(edited.Statements[0].Reason, qt.Equals,
				"the edit changed a statement that carried an access assessment; its access effect is unknown and manual review is required")
			c.Assert(edited.Destructive, qt.IsTrue)
		})
	}
}

func TestDecodePlanFile_FailurePath_RefusesAnAccessAssessmentThePlannerCouldNotWrite(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{name: "unrecognized access", statement: `{"sql":"CREATE POLICY p ON t","severity":"safe","reason":"r","access":"safe","access_reason":"r"}`,
			want: `.*unrecognized access assessment "safe".*`},
		{name: "access without a reason", statement: `{"sql":"CREATE POLICY p ON t","severity":"safe","reason":"r","access":"widens"}`,
			want: `.*needs a reason on one trimmed line.*`},
		{name: "a reason without access", statement: `{"sql":"CREATE POLICY p ON t","severity":"safe","reason":"r","access_reason":"r"}`,
			want: `.*access_reason is recorded without access.*`},
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
		`{"sql":"CREATE POLICY p ON t","severity":"destructive","reason":"can widen access: r","access":"widens","access_reason":"r"}]}`
	plan, _, err := atlasschema.DecodePlanFile([]byte(document), "p.plan.json")
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].Access, qt.Equals, schemaext.AccessWidens)
	c.Assert(plan.Statements[0].AccessReason, qt.Equals, "r")
}
