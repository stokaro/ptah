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
