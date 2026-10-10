package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
)

const mixedReason = "an owner operation shares its statement with other operations; manual review is required"

// plannedVerdicts renders nodes once with service, splits the plan the way an
// apply splits it, and asks for the owners' verdicts of its statements.
func plannedVerdicts(c *qt.C, service renderer.Service, dialect string, nodes []ast.Node) ([]string, []safety.StatementAssessment) {
	c.Helper()
	request := renderer.Request{Target: dialect, Capabilities: capability.ForDialect(dialect), Nodes: nodes}
	result, err := renderer.Render(c.Context(), service, request)
	c.Assert(err, qt.IsNil)
	plan := planner.RenderedPlan{Request: request, Result: result, Dialect: dialect}
	var statements []string
	var statementNodes []int
	for _, statement := range plan.PlannedStatements() {
		statements = append(statements, statement.SQL)
		statementNodes = append(statementNodes, statement.Node)
	}
	verdicts, err := safety.OwnerVerdicts(nodes, statementNodes)
	c.Assert(err, qt.IsNil)
	c.Assert(verdicts, qt.HasLen, len(statements))
	return statements, verdicts
}

// Each statement takes the verdict of the node whose fragment rendered it. An
// owner's note renders as a comment the plan joins to the statement after it,
// which is still the one the owner operation rendered, and a comment no
// statement follows belongs to no node.
func TestOwnerVerdicts_AttributesEachVerdictToTheNodeThatRenderedIt(t *testing.T) {
	c := qt.New(t)
	statements, verdicts := plannedVerdicts(c, &policyRenderer{terminated: true}, platform.Postgres, []ast.Node{
		&ast.CreateTableNode{Name: "fresh"}, ast.NewComment("owner note"), policy(narrowsRows), policy(widensRows),
		&ast.DropTableNode{Name: "old"}, ast.NewComment("tail"),
	})
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TABLE fresh (id integer)", "-- owner note\nCREATE POLICY p ON t USING (true)",
		"CREATE POLICY p ON t USING (true)", "DROP TABLE old", "-- tail",
	})
	c.Assert(verdicts, qt.DeepEquals, []safety.StatementAssessment{
		{},
		{NodeType: "*ast.ExtensionStatement", Severity: safety.Warning,
			Reason: "can narrow access: " + narrowsRows.Reason, Access: schemaext.AccessNarrows, AccessReason: narrowsRows.Reason},
		{NodeType: "*ast.ExtensionStatement", Severity: safety.Destructive,
			Reason: "can widen access: " + widensRows.Reason, Access: schemaext.AccessWidens, AccessReason: widensRows.Reason},
		{},
		{},
	})
}

// A fragment that leaves its last statement unterminated is still one
// fragment: its statements are not run into the next node's, so the owner's
// verdict stays on the owner's statement.
func TestOwnerVerdicts_KeepsAnUnterminatedFragmentApart(t *testing.T) {
	c := qt.New(t)
	statements, verdicts := plannedVerdicts(c, &policyRenderer{terminated: false}, platform.Postgres,
		[]ast.Node{&ast.CreateTableNode{Name: "fresh"}, policy(widensRows), &ast.DropTableNode{Name: "old"}})
	c.Assert(statements, qt.DeepEquals, []string{"CREATE TABLE fresh (id integer)", "CREATE POLICY p ON t USING (true)", "DROP TABLE old"})
	c.Assert(verdicts[0], qt.DeepEquals, safety.StatementAssessment{})
	c.Assert(verdicts[1].Access, qt.Equals, schemaext.AccessWidens)
	c.Assert(verdicts[1].Severity, qt.Equals, safety.Destructive)
	c.Assert(verdicts[2], qt.DeepEquals, safety.StatementAssessment{})
}

// Every statement of a node holding an owner operation beside other work
// fails closed, and only those. Failing closed raises the node's own verdict:
// an access effect it established and that is stronger than unknown stays.
func TestOwnerVerdicts_FailsClosedOnAMixedNodeOnly(t *testing.T) {
	tests := []struct {
		name             string
		access           schemaext.AccessEffect
		wantAccess       schemaext.Access
		wantAccessReason string
	}{
		{name: "an unchanged access effect", access: unchangedACL, wantAccess: schemaext.AccessUnknown, wantAccessReason: mixedReason},
		{name: "a widening", access: widensRows, wantAccess: schemaext.AccessWidens, wantAccessReason: widensRows.Reason},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			mixed := &ast.StatementList{Statements: []ast.Node{&ast.DropTableNode{Name: "old"}, policy(tc.access)}}
			statements, verdicts := plannedVerdicts(c, &policyRenderer{terminated: true}, platform.Postgres,
				[]ast.Node{&ast.CreateTableNode{Name: "fresh"}, mixed})
			c.Assert(statements, qt.HasLen, 3)
			c.Assert(verdicts[0], qt.DeepEquals, safety.StatementAssessment{})
			for _, verdict := range verdicts[1:] {
				c.Assert(verdict.Severity, qt.Equals, safety.Destructive)
				c.Assert(verdict.Access, qt.Equals, tc.wantAccess)
				c.Assert(verdict.AccessReason, qt.Equals, tc.wantAccessReason)
			}
		})
	}
}

// An owner operation that makes no access claim leaves a failed-closed
// statement without an access effect, so the field never suggests the
// statement was assessed for access.
func TestOwnerVerdicts_FailsClosedWithoutAnAccessClaimTheOwnerDidNotMake(t *testing.T) {
	c := qt.New(t)
	mixed := &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
		&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "note", Type: "TEXT", Nullable: true}},
		&ast.ExtensionAlterOperation{Payload: &ydbast.DropChangefeed{Name: "updates"}},
	}}

	verdicts, err := safety.OwnerVerdicts([]ast.Node{mixed}, []int{0, 0})

	c.Assert(err, qt.IsNil)
	c.Assert(verdicts, qt.HasLen, 2)
	for _, verdict := range verdicts {
		c.Assert(verdict.Severity, qt.Equals, safety.Destructive)
		c.Assert(verdict.Access, qt.Equals, schemaext.Access(""))
		c.Assert(verdict.AccessReason, qt.Equals, "")
	}
}

func TestOwnerVerdicts_LeavesAPlanWithoutOwnedOperationsUnmarked(t *testing.T) {
	c := qt.New(t)
	statements, verdicts := plannedVerdicts(c, &policyRenderer{terminated: true}, platform.Postgres,
		[]ast.Node{&ast.CreateTableNode{Name: "fresh"}, &ast.DropTableNode{Name: "old"}})
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(verdicts, qt.DeepEquals, make([]safety.StatementAssessment, 2))
}

// A typed nil node is classified, not dereferenced, at the top of the plan
// and inside a statement list that also holds an owner operation.
func TestOwnerVerdicts_ToleratesTypedNilNodes(t *testing.T) {
	c := qt.New(t)
	nested := &ast.StatementList{Statements: []ast.Node{(*ast.StatementList)(nil), (*ast.AlterTableNode)(nil), policy(widensRows)}}
	nodes := []ast.Node{(*ast.StatementList)(nil), (*ast.AlterTableNode)(nil), policy(widensRows), nested}

	verdicts, err := safety.OwnerVerdicts(nodes, []int{2, 3})

	c.Assert(err, qt.IsNil)
	c.Assert(verdicts, qt.HasLen, 2)
	c.Assert(verdicts[0].Access, qt.Equals, schemaext.AccessWidens)
	c.Assert(verdicts[1].Severity, qt.Equals, safety.Destructive)
	c.Assert(verdicts[1].Reason, qt.Equals, "can widen access: "+widensRows.Reason, qt.Commentf("the nested list is a mixed node"))
}

func TestOwnerVerdicts_FailurePath_RefusesProvenanceOutsideThePlannedNodes(t *testing.T) {
	tests := []struct {
		name  string
		nodes []int
		want  string
	}{
		{name: "past the last node", nodes: []int{0, 2}, want: `.*statement provenance 2 is outside the 2 planned nodes`},
		{name: "below no node", nodes: []int{-2}, want: `.*statement provenance -2 is outside the 2 planned nodes`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			verdicts, err := safety.OwnerVerdicts([]ast.Node{policy(widensRows), &ast.DropTableNode{Name: "old"}}, tc.nodes)
			c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
			c.Assert(err, qt.ErrorMatches, tc.want)
			c.Assert(verdicts, qt.IsNil)
		})
	}
}

// A saved plan records the owner's verdict of every owner operation, and the
// statement's text alone reads each of these as safe. Before owner verdicts
// reached saved plans, only a CockroachDB row-level TTL change was raised;
// these statements were saved as safe. This is a change in behavior, and
// pre-v1 no compatibility with the earlier severity is owed.
func TestOwnerVerdicts_RaiseFeatureOwnerStatementsTheirTextReadsAsSafe(t *testing.T) {
	ttl := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()", TTL: "created_at + INTERVAL 1 DAY"}
	noTTL := ttl.Desired()
	noTTL.TTL.Value = ""
	alter := func(table string, payload ast.ExtensionPayload) ast.Node {
		return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: payload}}}
	}
	tests := []struct {
		name         string
		dialect      string
		node         ast.Node
		wantSQL      string
		wantSeverity safety.Severity
		wantReason   string
	}{
		{
			name: "a YDB changefeed dropped", dialect: platform.YDB,
			node:         alter("items", &ydbast.DropChangefeed{Name: "updates"}),
			wantSQL:      "ALTER TABLE `items` DROP CHANGEFEED `updates`",
			wantSeverity: safety.Destructive,
			wantReason:   "DROP CHANGEFEED removes the change stream with every record nobody read, and its consumers",
		},
		{
			name: "a ClickHouse TTL removed", dialect: platform.ClickHouse,
			node:         alter("items", &chast.AlterTTL{Change: chdiff.Table{Before: ttl, After: noTTL}}),
			wantSQL:      "ALTER TABLE items REMOVE TTL",
			wantSeverity: safety.Warning,
			wantReason:   "TTL changes can expire, move, or aggregate existing data; restoring the rule cannot recover that data",
		},
		{
			name: "a ClickHouse skipping index added", dialect: platform.ClickHouse,
			node:         alter("events", &chast.AddSkippingIndex{Name: "idx", Expression: "c"}),
			wantSQL:      "ALTER TABLE `events` ADD INDEX `idx` c TYPE minmax GRANULARITY 1",
			wantSeverity: safety.Warning,
			wantReason:   "ADD INDEX changes future write work and query plans; existing data is not materialized by this operation",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			statements, verdicts := plannedVerdicts(c, must.Must(builtin.New()), tc.dialect, []ast.Node{tc.node})
			c.Assert(statements, qt.DeepEquals, []string{tc.wantSQL})
			textOnly := safety.AssessSQL(statements[0])
			c.Assert(textOnly.Severity, qt.Equals, safety.Safe)

			saved := textOnly
			safety.Fold(&saved, verdicts[0])

			c.Assert(saved.Severity, qt.Equals, tc.wantSeverity)
			c.Assert(saved.Reason, qt.Equals, tc.wantReason)
		})
	}
}
