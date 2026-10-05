package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbstream"
	"ptah.run/migration/safety"
)

func TestStreamingQueryCheckpointRisk(t *testing.T) {
	for _, test := range []struct {
		name     string
		node     *ydbstream.Node
		severity safety.Severity
	}{
		{name: "drop", node: &ydbstream.Node{Operation: ydbstream.DropOperation, Name: "q"}, severity: safety.Destructive},
		{name: "body reset", node: &ydbstream.Node{Operation: ydbstream.AlterOperation, Name: "q", Spec: ast.StreamingQuerySpec{Text: "SELECT 2;"}, Previous: ast.StreamingQuerySpec{Text: "SELECT 1;"}, AllowStateReset: true}, severity: safety.Destructive},
		{name: "stop", node: &ydbstream.Node{Operation: ydbstream.AlterOperation, Name: "q", Spec: ast.StreamingQuerySpec{Text: "SELECT 1;", Run: new(false)}, Previous: ast.StreamingQuerySpec{Text: "SELECT 1;"}}, severity: safety.Warning},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := safety.Assess([]ast.Node{test.node})
			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Severity, qt.Equals, test.severity)
			sql, err := test.node.Statement(capability.YDB262().With(capability.StreamingQueries, true))
			c.Assert(err, qt.IsNil)
			c.Assert(safety.Assess([]ast.Node{ast.NewRawSQL(sql)})[0].Severity, qt.Equals, test.severity)

		})
	}
}
