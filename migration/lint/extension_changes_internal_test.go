package lint

// White-box testing required: AnalyzeFS accepts SQL, so caller-defined payload
// reports and malformed interface values cannot reach it. This pins the
// neutral reporting boundary; clickhouse_index_test.go drives the public path.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

type reportedChange struct {
	action ast.ExtensionChangeAction
	name   string
}

func (*reportedChange) Kind() schemaext.Kind                   { return "example.org/report" }
func (p *reportedChange) CloneExtension() ast.ExtensionPayload { return new(*p) }
func (p *reportedChange) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: p.action, Name: p.name}
}

func TestExtensionChangeReportsStayScopedAndConservative(t *testing.T) {
	for _, test := range []struct {
		name     string
		payload  ast.ExtensionPayload
		wantKind SchemaChangeKind
		wantName string
	}{
		{name: "add", payload: &reportedChange{action: ast.ExtensionAdd, name: "child"}, wantKind: SchemaChangeAdd, wantName: "child"},
		{name: "drop", payload: &reportedChange{action: ast.ExtensionDrop, name: "child"}, wantKind: SchemaChangeDrop, wantName: "child"},
		{name: "modify", payload: &reportedChange{action: ast.ExtensionModify, name: "child"}, wantKind: SchemaChangeModify, wantName: "child"},
		{name: "rename", payload: &reportedChange{action: ast.ExtensionRename, name: "child"}, wantKind: SchemaChangeRename, wantName: "child"},
		{name: "unknown action", payload: &reportedChange{action: "ignored", name: "child"}, wantKind: SchemaChangeModify, wantName: "parent"},
		{name: "empty name", payload: &reportedChange{action: ast.ExtensionAdd}, wantKind: SchemaChangeModify, wantName: "parent"},
		{name: "nil", wantKind: SchemaChangeModify, wantName: "parent"},
		{name: "typed nil", payload: (*reportedChange)(nil), wantKind: SchemaChangeModify, wantName: "parent"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			kind, name := alterOperationChange(&ast.AlterTableNode{Name: "parent"}, &ast.ExtensionAlterOperation{Payload: test.payload})
			c.Assert(kind, qt.Equals, test.wantKind)
			c.Assert(name, qt.Equals, test.wantName)
		})
	}
}
