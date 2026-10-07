package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/migration/safety"
)

// TestClassify_Changefeed judges what a YDB changefeed statement loses: a
// drop takes the stream with every record nobody read, a change of its topic
// can end a consumer's position or keep records for less time, and an
// addition loses nothing.
func TestClassify_Changefeed(t *testing.T) {
	feed := ast.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	alter := func(operation ast.AlterOperation) ast.Node {
		return &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{operation}}
	}
	tests := []struct {
		name         string
		node         ast.Node
		wantSeverity safety.Severity
		wantReason   string
	}{
		{
			name: "a changefeed dropped", node: alter(&ast.ExtensionAlterOperation{Payload: &ydbast.DropChangefeed{Name: "updates"}}),
			wantSeverity: safety.Destructive,
			wantReason:   "DROP CHANGEFEED removes the change stream with every record nobody read, and its consumers",
		},
		{
			name: "a changefeed's topic changed", node: alter(&ast.ExtensionAlterOperation{Payload: &ydbast.AlterChangefeedTopic{Changefeed: feed, Previous: feed}}),
			wantSeverity: safety.Warning,
			wantReason:   "ALTER TOPIC can drop a consumer's position or shorten how long the stream keeps records",
		},
		{
			name: "a changefeed added", node: alter(&ast.ExtensionAlterOperation{Payload: &ydbast.AddChangefeed{Changefeed: feed}}),
			wantSeverity: safety.Safe,
			wantReason:   "does not remove data or tighten constraints",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			assessments := safety.Assess([]ast.Node{test.node})

			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.wantSeverity)
			c.Assert(assessments[0].Reason, qt.Equals, test.wantReason)
		})
	}
}
