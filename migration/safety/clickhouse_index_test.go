package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/migration/safety"
)

func TestSkippingIndexAdditionRetainsWorkloadWarning(t *testing.T) {
	c := qt.New(t)
	operation := &ast.ExtensionAlterOperation{Payload: &chast.AddSkippingIndex{Name: "idx", Expression: "c"}}
	for _, node := range []ast.Node{operation, &ast.AlterTableNode{Name: "events", Operations: []ast.AlterOperation{operation}}} {
		assessments := safety.Assess([]ast.Node{node})
		c.Assert(assessments, qt.HasLen, 1)
		c.Assert(assessments[0].Severity, qt.Equals, safety.Warning)
		c.Assert(assessments[0].Reason, qt.Contains, "existing data is not materialized")
	}
}
