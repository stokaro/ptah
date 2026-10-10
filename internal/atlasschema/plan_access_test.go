package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/safety"
)

const editedOwnerReason = "the edit changed a plan that carried an owner verdict; " +
	"this statement's effect, access included, is unknown and manual review is required"

var (
	openPolicy = atlasschema.PlanStatement{
		SQL: "CREATE POLICY open ON t USING (true)", Severity: safety.Safe, Owned: true,
		Reason: "does not remove data or tighten constraints", Access: schemaext.AccessUnchanged, AccessReason: "a restrictive policy still limits it",
	}
	restrictivePolicy = atlasschema.PlanStatement{
		SQL: "CREATE POLICY tenant ON t AS RESTRICTIVE USING (tenant = current_user)", Severity: safety.Warning, Owned: true,
		Reason: "can narrow access: hides other tenants' rows", Access: schemaext.AccessNarrows, AccessReason: "hides other tenants' rows",
	}
	wideningPolicy = atlasschema.PlanStatement{
		SQL: "CREATE POLICY everyone ON t USING (true)", Severity: safety.Destructive, Owned: true,
		Reason: "can widen access: admits every row", Access: schemaext.AccessWidens, AccessReason: "admits every row",
	}
	rowTTL = atlasschema.PlanStatement{
		SQL: "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')", Severity: safety.Warning, Owned: true,
		Reason: "row-level TTL decides which rows a background job deletes",
	}
	commonTable = atlasschema.PlanStatement{
		SQL: "CREATE TABLE kept (id integer)", Severity: safety.Safe, Reason: "does not remove data or tighten constraints",
	}
	declaredRowDelete = atlasschema.PlanStatement{
		SQL: "DELETE FROM ref WHERE id = 1", Severity: safety.Destructive, Reason: "the declaration no longer holds this row",
	}
)

// failedClosed is statement after an edit changed a plan that carried an
// owner verdict: Destructive, its reason kept, with an unknown access effect.
// A widening is stronger than unknown and stays; that row spells its result
// out.
func failedClosed(statement atlasschema.PlanStatement) atlasschema.PlanStatement {
	statement.Severity, statement.Owned = safety.Destructive, true
	statement.Reason += "; " + editedOwnerReason
	statement.Access, statement.AccessReason = schemaext.AccessUnknown, editedOwnerReason
	return statement
}

// An edit that leaves the statement sequence as it was keeps every recorded
// verdict position for position, even where its text rates lower and where two
// statements share a text but not a verdict. Comments and whitespace are not
// part of the sequence, which is what lets a directive header be spliced in.
func TestPlanFileWithStatementsFromSQLKeepsEveryVerdictWhenTheSequenceIsUnchanged(t *testing.T) {
	sameText := []atlasschema.PlanStatement{
		{SQL: "SELECT 1", Severity: safety.Warning, Owned: true, Reason: "can narrow access: r",
			Access: schemaext.AccessNarrows, AccessReason: "r"},
		{SQL: "SELECT 1", Severity: safety.Safe, Reason: "does not remove data or tighten constraints"},
	}
	tests := []struct {
		name            string
		recorded        []atlasschema.PlanStatement
		edited          string
		wantDestructive bool
	}{
		{name: "owner verdicts above their text", recorded: []atlasschema.PlanStatement{wideningPolicy, rowTTL, commonTable},
			edited: wideningPolicy.SQL + ";\n" + rowTTL.SQL + ";\n" + commonTable.SQL + ";\n", wantDestructive: true},
		{name: "a data-stage verdict above its text", recorded: []atlasschema.PlanStatement{declaredRowDelete},
			edited: declaredRowDelete.SQL + ";\n", wantDestructive: true},
		{name: "two statements with one text", recorded: sameText, edited: "SELECT 1;\nSELECT 1;\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			plan := atlasschema.PlanFile{Dialect: "postgres", Statements: tc.recorded}

			edited := plan.WithStatementsFromSQL(tc.edited)

			c.Assert(edited.Statements, qt.DeepEquals, tc.recorded)
			c.Assert(edited.Destructive, qt.Equals, tc.wantDestructive)
		})
	}
}

func TestPlanFileWithStatementsFromSQLKeepsVerdictsAcrossCommentsAndWhitespace(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{Dialect: "postgres", Statements: []atlasschema.PlanStatement{wideningPolicy, commonTable}}

	edited := plan.WithStatementsFromSQL("-- reviewed\nCREATE POLICY everyone\n  ON t USING (true);\n" + commonTable.SQL + ";\n")

	reviewed := wideningPolicy
	reviewed.SQL = "-- reviewed\nCREATE POLICY everyone\n  ON t USING (true)"
	c.Assert(edited.Statements, qt.DeepEquals, []atlasschema.PlanStatement{reviewed, commonTable})
	c.Assert(edited.Destructive, qt.IsTrue)
}

// Once an edit changes a plan that carried an owner verdict in any way, each
// statement that carries an owner verdict and each statement the edit
// introduced is raised to Destructive with an unknown access effect. Deciding
// which edits leave an owner's verdict valid is not attempted: an owner judged
// its statement beside the statements the plan held. A statement without an
// owner verdict keeps the verdict recorded for its text.
func TestPlanFileWithStatementsFromSQLFailsClosedWhenAPlanWithAnOwnerVerdictChanges(t *testing.T) {
	enableRLS := atlasschema.PlanStatement{
		SQL: "ALTER TABLE t ENABLE ROW LEVEL SECURITY", Severity: safety.Safe, Reason: "does not remove data or tighten constraints",
	}
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
				failedClosed(atlasschema.PlanStatement{SQL: "CREATE POLICY tenant ON t AS RESTRICTIVE USING (true)",
					Reason: "does not remove data or tighten constraints"}),
				commonTable,
			},
		},
		{
			name:     "a statement added beside an unchanged owned one",
			recorded: []atlasschema.PlanStatement{openPolicy},
			edited:   openPolicy.SQL + ";\n" + enableRLS.SQL + ";\n",
			want:     []atlasschema.PlanStatement{failedClosed(openPolicy), failedClosed(enableRLS)},
		},
		{
			name:     "a sibling policy removed",
			recorded: []atlasschema.PlanStatement{openPolicy, restrictivePolicy},
			edited:   openPolicy.SQL + ";\n",
			want:     []atlasschema.PlanStatement{failedClosed(openPolicy)},
		},
		{
			name:     "a statement no owner rendered removed",
			recorded: []atlasschema.PlanStatement{openPolicy, commonTable},
			edited:   openPolicy.SQL + ";\n",
			want:     []atlasschema.PlanStatement{failedClosed(openPolicy)},
		},
		{
			name:     "owned statements reordered",
			recorded: []atlasschema.PlanStatement{openPolicy, restrictivePolicy},
			edited:   restrictivePolicy.SQL + ";\n" + openPolicy.SQL + ";\n",
			want:     []atlasschema.PlanStatement{failedClosed(restrictivePolicy), failedClosed(openPolicy)},
		},
		{
			name:     "a widening stays a widening",
			recorded: []atlasschema.PlanStatement{wideningPolicy, commonTable},
			edited:   wideningPolicy.SQL + ";\n",
			want: []atlasschema.PlanStatement{{
				SQL: wideningPolicy.SQL, Severity: safety.Destructive, Owned: true,
				Reason: wideningPolicy.Reason + "; " + editedOwnerReason,
				Access: schemaext.AccessWidens, AccessReason: wideningPolicy.AccessReason,
			}},
		},
		{
			name:     "a lifecycle owner verdict rewritten",
			recorded: []atlasschema.PlanStatement{rowTTL},
			edited:   "ALTER TABLE sessions SET (ttl_expiration_expression = 'created_at');\n",
			want: []atlasschema.PlanStatement{
				failedClosed(atlasschema.PlanStatement{SQL: "ALTER TABLE sessions SET (ttl_expiration_expression = 'created_at')",
					Reason: "does not remove data or tighten constraints"}),
			},
		},
		{
			name:     "an introduced statement keeps its own reason",
			recorded: []atlasschema.PlanStatement{rowTTL},
			edited:   "DROP TABLE sessions;\n",
			want: []atlasschema.PlanStatement{
				failedClosed(atlasschema.PlanStatement{SQL: "DROP TABLE sessions", Reason: "DROP TABLE removes the table and all rows"}),
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

// In a plan without an owner verdict, a statement whose text the plan recorded
// takes the strongest verdict recorded for that text, so a copy of a
// destructive statement is destructive too. The edit introduces nothing an
// owner judged, so nothing fails closed.
func TestPlanFileWithStatementsFromSQLGivesACopyTheStrongestRecordedVerdict(t *testing.T) {
	warned := atlasschema.PlanStatement{SQL: "SELECT 1", Severity: safety.Warning, Reason: "warned"}
	plain := atlasschema.PlanStatement{SQL: "SELECT 1", Severity: safety.Safe, Reason: "does not remove data or tighten constraints"}
	tests := []struct {
		name            string
		recorded        []atlasschema.PlanStatement
		edited          string
		want            []atlasschema.PlanStatement
		wantDestructive bool
	}{
		{
			name:            "a destructive data statement copied",
			recorded:        []atlasschema.PlanStatement{declaredRowDelete},
			edited:          declaredRowDelete.SQL + ";\n" + declaredRowDelete.SQL + ";\n",
			want:            []atlasschema.PlanStatement{declaredRowDelete, declaredRowDelete},
			wantDestructive: true,
		},
		{
			name:     "one of two statements with one text removed",
			recorded: []atlasschema.PlanStatement{plain, warned},
			edited:   "SELECT 1;\n",
			want:     []atlasschema.PlanStatement{warned},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			plan := atlasschema.PlanFile{Dialect: "postgres", Statements: tc.recorded}

			edited := plan.WithStatementsFromSQL(tc.edited)

			c.Assert(edited.Statements, qt.DeepEquals, tc.want)
			c.Assert(edited.Destructive, qt.Equals, tc.wantDestructive)
		})
	}
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
