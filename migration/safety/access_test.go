package safety_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// accessPayload is an owned operation that reports its lifecycle effect and
// its access assessment separately, as an access-control owner does.
type accessPayload struct {
	effect schemaext.Effect
	access schemaext.AccessEffect
}

func (*accessPayload) Kind() schemaext.Kind { return "example.org/access-operation" }
func (p *accessPayload) CloneExtension() ast.ExtensionPayload {
	return &accessPayload{effect: p.effect, access: p.access}
}
func (p *accessPayload) Effect() schemaext.Effect             { return p.effect }
func (p *accessPayload) AccessEffect() schemaext.AccessEffect { return p.access }

// accessChange is the change payload an access-control owner reports before
// planning.
type accessChange struct {
	effect schemaext.Effect
	access schemaext.AccessEffect
}

func (*accessChange) Kind() schemaext.Kind { return "example.org/access-change" }
func (v *accessChange) CloneChange() schemaext.ChangeValue {
	return &accessChange{effect: v.effect, access: v.access}
}
func (v *accessChange) Effect() schemaext.Effect             { return v.effect }
func (v *accessChange) AccessEffect() schemaext.AccessEffect { return v.access }

var addsObject = schemaext.Effect{Impact: schemaext.Additive, Reason: "adds an access-control object"}

// accessNodeShapes places one owned operation in every node shape the
// classifier reads, so each verdict is checked standalone, nested, and inside
// an ALTER TABLE.
func accessNodeShapes(payload ast.ExtensionPayload) []ast.Node {
	return []ast.Node{
		&ast.ExtensionStatement{Payload: payload},
		&ast.StatementList{Statements: []ast.Node{&ast.ExtensionStatement{Payload: payload}}},
		&ast.ExtensionAlterOperation{Payload: payload},
		&ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "note", Type: "TEXT", Nullable: true}},
			&ast.ExtensionAlterOperation{Payload: payload},
		}},
	}
}

func TestAssess_ReportsTheOwnersAccessEffect(t *testing.T) {
	tests := []struct {
		name         string
		effect       schemaext.Effect
		access       schemaext.AccessEffect
		wantSeverity safety.Severity
		wantReason   string
	}{
		{
			name: "widening is destructive", effect: addsObject,
			access:       schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "a permissive policy admits more rows"},
			wantSeverity: safety.Destructive, wantReason: "can widen access: a permissive policy admits more rows",
		},
		{
			name: "narrowing is a warning", effect: addsObject,
			access:       schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "a restrictive policy hides rows"},
			wantSeverity: safety.Warning, wantReason: "can narrow access: a restrictive policy hides rows",
		},
		{
			name:         "unchanged access keeps the lifecycle verdict",
			effect:       schemaext.Effect{Impact: schemaext.Behavioral, Reason: "renames the policy"},
			access:       schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "only the name changes"},
			wantSeverity: safety.Warning, wantReason: "renames the policy",
		},
		{
			name: "an unknown effect is destructive", effect: addsObject,
			access:       schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "the predicate changed"},
			wantSeverity: safety.Destructive, wantReason: "access effect is unknown: the predicate changed",
		},
		{
			name:         "a stronger lifecycle verdict keeps its reason",
			effect:       schemaext.Effect{Impact: schemaext.Destructive, Reason: "drops the policy"},
			access:       schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "a permissive policy goes away"},
			wantSeverity: safety.Destructive, wantReason: "drops the policy",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			assessments := safety.Assess(accessNodeShapes(&accessPayload{effect: tc.effect, access: tc.access}))
			c.Assert(assessments, qt.HasLen, 4)
			for _, assessment := range assessments {
				c.Assert(assessment.Severity, qt.Equals, tc.wantSeverity, qt.Commentf("%s", assessment.NodeType))
				c.Assert(assessment.Reason, qt.Equals, tc.wantReason, qt.Commentf("%s", assessment.NodeType))
				c.Assert(assessment.Access, qt.Equals, tc.access.Access, qt.Commentf("%s", assessment.NodeType))
				c.Assert(assessment.AccessReason, qt.Equals, tc.access.Reason, qt.Commentf("%s", assessment.NodeType))
			}
		})
	}
}

// An assessment the owner did not establish is never read as unchanged or
// narrowing; it takes the review an unknown effect takes.
func TestAssess_UnestablishedAccessEffectsRequireReview(t *testing.T) {
	tests := []struct {
		name    string
		payload ast.ExtensionPayload
	}{
		{name: "missing assessment", payload: &accessPayload{effect: addsObject}},
		{name: "unrecognized assessment", payload: &accessPayload{effect: addsObject, access: schemaext.AccessEffect{Access: "safe", Reason: "not a declared value"}}},
		{name: "missing reason", payload: &accessPayload{effect: addsObject, access: schemaext.AccessEffect{Access: schemaext.AccessUnchanged}}},
		{name: "typed nil", payload: (*accessPayload)(nil)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			assessments := safety.Assess(accessNodeShapes(tc.payload))
			c.Assert(assessments, qt.HasLen, 4)
			for _, assessment := range assessments {
				c.Assert(assessment.Severity, qt.Equals, safety.Destructive, qt.Commentf("%s", assessment.NodeType))
				c.Assert(assessment.Access, qt.Equals, schemaext.AccessUnknown, qt.Commentf("%s", assessment.NodeType))
				c.Assert(assessment.AccessReason, qt.Equals, "the access effect is not established; manual review is required")
			}
		})
	}
}

func TestAssess_AStatementReportsItsStrongestAccessEffect(t *testing.T) {
	narrows := schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "narrows"}
	tests := []struct {
		name  string
		other schemaext.AccessEffect
		want  schemaext.AccessEffect
	}{
		{name: "unchanged then narrows", other: schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "unchanged"}, want: narrows},
		{name: "unknown outranks narrows", other: schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "unknown"},
			want: schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "unknown"}},
		{name: "widens outranks narrows", other: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "widens"},
			want: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "widens"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			first := &accessPayload{effect: addsObject, access: narrows}
			second := &accessPayload{effect: addsObject, access: tc.other}
			nodes := []ast.Node{
				&ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
					&ast.ExtensionAlterOperation{Payload: first}, &ast.ExtensionAlterOperation{Payload: second},
				}},
				&ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
					&ast.ExtensionAlterOperation{Payload: second}, &ast.ExtensionAlterOperation{Payload: first},
				}},
				&ast.StatementList{Statements: []ast.Node{&ast.ExtensionStatement{Payload: first}, &ast.ExtensionStatement{Payload: second}}},
				&ast.StatementList{Statements: []ast.Node{&ast.ExtensionStatement{Payload: second}, &ast.ExtensionStatement{Payload: first}}},
			}
			assessments := safety.Assess(nodes)
			c.Assert(assessments, qt.HasLen, 4)
			for _, assessment := range assessments {
				c.Assert(assessment.Access, qt.Equals, tc.want.Access)
				c.Assert(assessment.AccessReason, qt.Equals, tc.want.Reason)
			}
		})
	}
}

// A statement whose operations make no access claim reports none, so the
// field never suggests that a common change or another owner's operation was
// assessed for access.
func TestAssess_StatementsWithoutAnAccessClaimReportNone(t *testing.T) {
	c := qt.New(t)
	assessments := safety.Assess([]ast.Node{
		&ast.DropTableNode{Name: "items"},
		&ast.ExtensionStatement{Payload: &unclassifiedPayload{effect: addsObject}},
		&ast.ExtensionStatement{Payload: &unknownPayload{}},
	})
	c.Assert(assessments, qt.HasLen, 3)
	for _, assessment := range assessments {
		c.Assert(assessment.Access, qt.Equals, schemaext.Access(""))
		c.Assert(assessment.AccessReason, qt.Equals, "")
	}
}

// Every statement an owned operation renders carries its access assessment,
// including statements whose own text reads as harmless.
func TestAssessRendered_FoldsTheAccessEffectIntoEveryStatementItRendered(t *testing.T) {
	c := qt.New(t)
	access := schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "a permissive policy admits more rows"}
	nodes := []ast.Node{&ast.ExtensionStatement{Payload: &accessPayload{effect: addsObject, access: access}}}
	service := selectedRendering(func(_ context.Context, request renderer.Request) (renderer.Result, error) {
		return renderer.Result{Complete: true, Fragments: []string{"CREATE POLICY p ON t; COMMENT ON POLICY p ON t IS 'x';"}}, nil
	})
	assessments, err := safety.AssessRenderedWithCapabilities(t.Context(), service, nodes, "postgres", capability.ForDialect("postgres"))
	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 2)
	for _, assessment := range assessments {
		c.Assert(assessment.Severity, qt.Equals, safety.Destructive)
		c.Assert(assessment.Access, qt.Equals, schemaext.AccessWidens)
		c.Assert(assessment.AccessReason, qt.Equals, access.Reason)
	}
}

var (
	widensRows   = schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "a permissive policy admits more rows"}
	narrowsRows  = schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "a restrictive policy hides rows"}
	unchangedACL = schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "only the comment changes"}
)

// policyRenderer renders the shapes the tests build, one statement per ALTER
// TABLE operation and per statement-list child, and records every batch it
// receives. terminated false leaves each fragment's last statement without its
// semicolon, as a defective renderer could.
type policyRenderer struct {
	terminated bool
	batches    [][]ast.Node
}

func (r *policyRenderer) Render(_ context.Context, request renderer.Request) (renderer.Result, error) {
	r.batches = append(r.batches, request.Nodes)
	result := renderer.Result{Complete: true}
	for _, node := range request.Nodes {
		result.Fragments = append(result.Fragments, r.fragment(node))
	}
	return result, nil
}

func (r *policyRenderer) fragment(node ast.Node) string {
	var statements []string
	switch typed := node.(type) {
	case *ast.AlterTableNode:
		for _, operation := range typed.Operations {
			switch operation.(type) {
			case *ast.AddColumnOperation:
				statements = append(statements, "ALTER TABLE items ADD COLUMN note TEXT")
			case *ast.ExtensionAlterOperation:
				statements = append(statements, "ALTER TABLE items ENABLE ROW LEVEL SECURITY")
			}
		}
	case *ast.StatementList:
		for _, child := range typed.Statements {
			statements = append(statements, strings.TrimSuffix(strings.TrimSpace(r.fragment(child)), ";"))
		}
	case *ast.DropTableNode:
		statements = append(statements, "DROP TABLE "+typed.Name)
	case *ast.CreateTableNode:
		statements = append(statements, "CREATE TABLE "+typed.Name+" (id integer)")
	case *ast.CommentNode:
		return "-- " + typed.Text + "\n"
	case *ast.ExtensionStatement:
		statements = append(statements, "CREATE POLICY p ON t USING (true)")
	}
	if r.terminated {
		return strings.Join(statements, ";\n") + ";\n"
	}
	return strings.Join(statements, ";\n")
}

func policy(access schemaext.AccessEffect) ast.Node {
	return &ast.ExtensionStatement{Payload: &accessPayload{effect: addsObject, access: access}}
}

// An owner operation that is a node of its own gives its verdict to what it
// rendered, and only its own: the DROP TABLE beside it in the plan does not
// make a harmless policy destructive, and two policies keep two verdicts
// rather than both taking the stronger one.
func TestAssessRendered_EachIsolatedOperationKeepsItsOwnVerdict(t *testing.T) {
	c := qt.New(t)
	service := &policyRenderer{terminated: true}
	nodes := []ast.Node{&ast.DropTableNode{Name: "old"}, policy(unchangedACL), policy(narrowsRows), policy(widensRows)}

	assessments, err := safety.AssessRenderedWithCapabilities(t.Context(), service, nodes, "postgres", capability.ForDialect("postgres"))

	c.Assert(err, qt.IsNil)
	c.Assert(service.batches, qt.HasLen, 1)
	c.Assert(service.batches[0], qt.HasLen, len(nodes), qt.Commentf("no node is rendered twice"))
	c.Assert(assessments, qt.HasLen, 4)
	wantSeverity := []safety.Severity{safety.Destructive, safety.Safe, safety.Warning, safety.Destructive}
	wantAccess := []schemaext.Access{"", schemaext.AccessUnchanged, schemaext.AccessNarrows, schemaext.AccessWidens}
	wantReason := []string{"DROP TABLE removes the table and all rows", "does not remove data or tighten constraints",
		"can narrow access: a restrictive policy hides rows", "can widen access: a permissive policy admits more rows"}
	for i, assessment := range assessments {
		c.Assert(assessment.Severity, qt.Equals, wantSeverity[i], qt.Commentf("%s", assessment.Statement))
		c.Assert(assessment.Access, qt.Equals, wantAccess[i], qt.Commentf("%s", assessment.Statement))
		c.Assert(assessment.Reason, qt.Equals, wantReason[i], qt.Commentf("%s", assessment.Statement))
	}
}

// A node holding an owner operation beside other work cannot say which of its
// statements the operation wrote, so every one of them fails closed. Failing
// closed raises: a widening the node established is stronger than unknown and
// stays, as [safety.Assess] reports it for the same node.
func TestAssessRendered_FailsClosedOnAMixedNode(t *testing.T) {
	alter := func(access schemaext.AccessEffect) ast.Node {
		return &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "note", Type: "TEXT", Nullable: true}},
			&ast.ExtensionAlterOperation{Payload: &accessPayload{effect: addsObject, access: access}},
		}}
	}
	list := func(access schemaext.AccessEffect) ast.Node {
		return &ast.StatementList{Statements: []ast.Node{&ast.DropTableNode{Name: "old"}, policy(access)}}
	}
	tests := []struct {
		name             string
		node             ast.Node
		wantAccess       schemaext.Access
		wantAccessReason string
	}{
		{name: "alter table", node: alter(unchangedACL), wantAccess: schemaext.AccessUnknown, wantAccessReason: mixedReason},
		{name: "statement list", node: list(unchangedACL), wantAccess: schemaext.AccessUnknown, wantAccessReason: mixedReason},
		{name: "a widening alter table", node: alter(widensRows), wantAccess: schemaext.AccessWidens, wantAccessReason: widensRows.Reason},
		{name: "a widening statement list", node: list(widensRows), wantAccess: schemaext.AccessWidens, wantAccessReason: widensRows.Reason},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			assessments, err := safety.AssessRenderedWithCapabilities(t.Context(), &policyRenderer{terminated: true}, []ast.Node{tc.node},
				"postgres", capability.ForDialect("postgres"))
			c.Assert(err, qt.IsNil)
			c.Assert(assessments, qt.HasLen, 2)
			for _, assessment := range assessments {
				c.Assert(assessment.Severity, qt.Equals, safety.Destructive, qt.Commentf("%s", assessment.Statement))
				c.Assert(assessment.Access, qt.Equals, tc.wantAccess, qt.Commentf("%s", assessment.Statement))
				c.Assert(assessment.AccessReason, qt.Equals, tc.wantAccessReason)
			}
		})
	}
}

// Assess judges a node whole and AssessRendered judges each statement it
// rendered; on a mixed node that widens access both report the widening.
func TestAssessRendered_AgreesWithAssessOnAMixedNodeThatWidens(t *testing.T) {
	c := qt.New(t)
	node := &ast.StatementList{Statements: []ast.Node{&ast.DropTableNode{Name: "old"}, policy(widensRows)}}

	whole := safety.Assess([]ast.Node{node})
	rendered, err := safety.AssessRenderedWithCapabilities(t.Context(), &policyRenderer{terminated: true}, []ast.Node{node},
		"postgres", capability.ForDialect("postgres"))

	c.Assert(err, qt.IsNil)
	c.Assert(whole, qt.HasLen, 1)
	c.Assert(whole[0].Access, qt.Equals, schemaext.AccessWidens)
	c.Assert(rendered, qt.HasLen, 2)
	c.Assert(rendered[0].Access, qt.Equals, whole[0].Access)
	c.Assert(rendered[1].Access, qt.Equals, whole[0].Access)
}

func TestClassifySchemaDiff_ReportsAccessEffectsApartFromLifecycle(t *testing.T) {
	tests := []struct {
		name   string
		access schemaext.AccessEffect
		want   []safety.Finding
	}{
		{
			name:   "widened",
			access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "admits more rows"},
			want: []safety.Finding{
				{Category: "feature_access_widened:example.org/access-change", Count: 2, Severity: safety.Destructive},
				{Category: "feature_changes:example.org/access-change", Count: 2, Severity: safety.Safe},
			},
		},
		{
			name:   "narrowed",
			access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "hides rows"},
			want: []safety.Finding{
				{Category: "feature_access_narrowed:example.org/access-change", Count: 2, Severity: safety.Warning},
				{Category: "feature_changes:example.org/access-change", Count: 2, Severity: safety.Safe},
			},
		},
		{
			name:   "unchanged",
			access: schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "comment only"},
			want: []safety.Finding{
				{Category: "feature_access_unchanged:example.org/access-change", Count: 2, Severity: safety.Safe},
				{Category: "feature_changes:example.org/access-change", Count: 2, Severity: safety.Safe},
			},
		},
		{
			name:   "unknown",
			access: schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "predicate changed"},
			want: []safety.Finding{
				{Category: "feature_access_unknown:example.org/access-change", Count: 2, Severity: safety.Destructive},
				{Category: "feature_changes:example.org/access-change", Count: 2, Severity: safety.Safe},
			},
		},
		{
			name: "not established",
			want: []safety.Finding{
				{Category: "feature_access_unknown:example.org/access-change", Count: 2, Severity: safety.Destructive},
				{Category: "feature_changes:example.org/access-change", Count: 2, Severity: safety.Destructive},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts("example.org/access-change", "app", "p")
			record := schemaext.ChangeRecord{Subject: ref, Value: &accessChange{effect: addsObject, access: tc.access}}
			diff := &difftypes.SchemaDiff{
				FeatureChanges: []schemaext.ChangeRecord{record},
				TablesModified: []difftypes.TableDiff{{FeatureChanges: []schemaext.ChangeRecord{record}}},
			}
			c.Assert(safety.ClassifySchemaDiff(diff), qt.DeepEquals, tc.want)
		})
	}
}

// A change that names no valid kind has no kind to be counted under; it is
// still counted, and its access claim still reads as unknown.
func TestClassifySchemaDiff_CountsAChangeWithoutAKindApart(t *testing.T) {
	c := qt.New(t)
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts("example.org/access-change", "app", "p")
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ref, Value: (*accessChange)(nil)}}}

	c.Assert(safety.ClassifySchemaDiff(diff), qt.DeepEquals, []safety.Finding{
		{Category: "feature_access_unknown", Count: 1, Severity: safety.Destructive},
		{Category: "feature_changes", Count: 1, Severity: safety.Destructive},
	})
}

func TestRenderers_ShowTheAccessAssessment(t *testing.T) {
	c := qt.New(t)
	assessments := safety.Assess([]ast.Node{&ast.ExtensionStatement{Payload: &accessPayload{
		effect: addsObject, access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "admits <more> rows"},
	}}})

	var text bytes.Buffer
	c.Assert(safety.RenderText(&text, assessments), qt.IsNil)
	c.Assert(text.String(), qt.Contains, "\n     access widens: admits <more> rows\n")

	var document bytes.Buffer
	c.Assert(safety.RenderJSON(&document, assessments), qt.IsNil)
	c.Assert(document.String(), qt.Contains, `"access": "widens"`)
	var report safety.Report
	c.Assert(json.Unmarshal(document.Bytes(), &report), qt.IsNil)
	c.Assert(report.Assessments, qt.HasLen, 1)
	c.Assert(report.Assessments[0].Access, qt.Equals, schemaext.AccessWidens)
	c.Assert(report.Assessments[0].AccessReason, qt.Equals, "admits <more> rows")

	var page bytes.Buffer
	c.Assert(safety.RenderHTML(&page, assessments), qt.IsNil)
	c.Assert(page.String(), qt.Contains, `<div class="access">access widens: admits &lt;more&gt; rows</div>`)
}

func TestFold_RaisesSeverityAndKeepsTheStrongestAccess(t *testing.T) {
	c := qt.New(t)
	text := safety.AssessSQL("CREATE POLICY p ON t USING (true)")
	c.Assert(text.Severity, qt.Equals, safety.Safe)
	safety.Fold(&text, safety.StatementAssessment{
		Statement: "ignored", Severity: safety.Destructive, Reason: "can widen access: admits every row",
		Access: schemaext.AccessWidens, AccessReason: "admits every row",
	})
	safety.Fold(&text, safety.StatementAssessment{
		Severity: safety.Warning, Reason: "lower", Access: schemaext.AccessNarrows, AccessReason: "weaker",
	})
	c.Assert(text, qt.DeepEquals, safety.StatementAssessment{
		NodeType: "sql", Statement: "CREATE POLICY p ON t USING (true)",
		Severity: safety.Destructive, Reason: "can widen access: admits every row",
		Access: schemaext.AccessWidens, AccessReason: "admits every row",
	})
}
