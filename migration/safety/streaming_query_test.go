package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
)

func TestStreamingQueryCreateGuardsAgreeAcrossAssessments(t *testing.T) {
	for _, test := range []struct {
		name     string
		options  ydbstreaming.CreateOptions
		severity safety.Severity
	}{
		{name: "create", severity: safety.Safe},
		{name: "replace", options: ydbstreaming.CreateOptions{OrReplace: true}, severity: safety.Destructive},
		{name: "guard", options: ydbstreaming.CreateOptions{IfNotExists: true}, severity: safety.Safe},
		{name: "guarded replacement", options: ydbstreaming.CreateOptions{OrReplace: true, IfNotExists: true}, severity: safety.Safe},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			payload := &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Name: "copy", Creation: ydbast.StreamingCreation{OrReplace: test.options.OrReplace, IfNotExists: test.options.IfNotExists},
				Spec: ydbstreaming.Spec{Text: "INSERT INTO sink SELECT * FROM source;"}}
			node := &ast.ExtensionStatement{Payload: payload}
			assessed := safety.Assess([]ast.Node{node})
			c.Assert(assessed[0].Severity, qt.Equals, test.severity)
			rendered, err := safety.AssessRenderedWithCapabilities(c.Context(), must.Must(builtin.New()), []ast.Node{node}, "ydb", capability.YDB262().With(capability.StreamingQueries, true))
			c.Assert(err, qt.IsNil)
			c.Assert(rendered[0].Severity, qt.Equals, test.severity)
			statement := ydbstreaming.Create(payload.QualifiedName(), payload.Spec, test.options)
			c.Assert(assessText(c, platform.YDB, statement).Severity, qt.Equals, test.severity)
			c.Assert(assessText(c, "", statement).Severity, qt.Equals, test.severity)
		})
	}
}

// A change of a streaming query read as text needs review, and one that
// changes its body resets its checkpoint state and is destructive, as is a
// dropped query, with the ydb target named or no target at all. With no target
// the body's semicolons end statements for a split that knows no dialect, so
// the YDB owner declines the statement it reads whole, which leaves it
// destructive.
func TestAssessSQL_StreamingQueries(t *testing.T) {
	const body = "ALTER STREAMING QUERY `copy` SET (FORCE = TRUE) AS DO BEGIN INSERT INTO sink SELECT * FROM source; END DO"
	tests := []struct {
		target    string
		statement string
		severity  safety.Severity
		reason    string
	}{
		{target: platform.YDB, statement: "ALTER STREAMING QUERY `copy` SET (RUN = FALSE)", severity: safety.Warning, reason: ydbstreaming.ExecutionChange},
		{target: "", statement: "ALTER STREAMING QUERY `copy` SET (RUN = FALSE)", severity: safety.Warning, reason: ydbstreaming.ExecutionChange},
		{target: platform.YDB, statement: body, severity: safety.Destructive, reason: ydbstreaming.CheckpointLoss},
		{target: "", statement: body, severity: safety.Destructive,
			reason: "the ydb owner reads this statement otherwise and did not judge it; manual review is required"},
		{target: platform.YDB, statement: "DROP STREAMING QUERY `copy`", severity: safety.Destructive, reason: ydbstreaming.CheckpointLoss},
		{target: "", statement: "DROP STREAMING QUERY `copy`", severity: safety.Destructive, reason: ydbstreaming.CheckpointLoss},
	}
	for _, test := range tests {
		t.Run(test.target+"/"+test.statement, func(t *testing.T) {
			c := qt.New(t)
			got := assessText(c, test.target, test.statement)
			c.Assert(got.Severity, qt.Equals, test.severity)
			c.Assert(got.Reason, qt.Equals, test.reason)
		})
	}
}
