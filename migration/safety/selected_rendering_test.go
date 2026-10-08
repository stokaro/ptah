package safety_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/migration/safety"
)

type selectedRendering func(context.Context, renderer.Request) (renderer.Result, error)

func (f selectedRendering) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	return f(ctx, request)
}

func TestAssessRenderedUsesOneSelectedBatchAndPreservesNodeRisk(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{
		&ast.StatementList{},
		&ast.ExtensionStatement{Payload: &unknownPayload{}},
		ast.NewRawSQL("DROP TABLE items;"),
	}
	calls := 0
	caps := capability.Capabilities{capability.TransactionalDDL: true}
	service := selectedRendering(func(ctx context.Context, request renderer.Request) (renderer.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "postgres")
		c.Assert(request.Capabilities, qt.DeepEquals, caps)
		c.Assert(request.Nodes, qt.DeepEquals, nodes)
		return renderer.Result{Complete: true, Fragments: []string{"", "CREATE WIDGET first; CREATE WIDGET second;", "DROP TABLE items;"}}, nil
	})
	assessments, err := safety.AssessRenderedWithCapabilities(t.Context(), service, nodes, "pgx", caps)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(assessments, qt.HasLen, 3)
	c.Assert(assessments[0].Index, qt.Equals, 1)
	c.Assert(assessments[1].Index, qt.Equals, 2)
	c.Assert(assessments[2].Index, qt.Equals, 3)
	c.Assert(assessments[0].NodeType, qt.Equals, "*ast.ExtensionStatement")
	c.Assert(assessments[1].NodeType, qt.Equals, "*ast.ExtensionStatement")
	c.Assert(assessments[2].NodeType, qt.Equals, "*ast.RawSQLNode")
	for _, assessment := range assessments[:2] {
		c.Assert(assessment.Severity, qt.Equals, safety.Destructive)
		c.Assert(assessment.Reason, qt.Equals, "extension effects are unknown; manual review is required")
	}
	c.Assert(assessments[2].Severity, qt.Equals, safety.Destructive)
}

func TestAssessRenderedDiscardsFailedOrIncompleteReplies(t *testing.T) {
	failure := errors.New("renderer failed")
	for _, test := range []struct {
		name      string
		fragments []string
		err       error
		want      error
	}{
		{name: "missing", want: renderer.ErrInvalidResult},
		{name: "excess", fragments: []string{"SELECT 1;", "SELECT 2;"}, want: renderer.ErrInvalidResult},
		{name: "failed", fragments: []string{"SELECT 1;"}, err: failure, want: failure},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := selectedRendering(func(context.Context, renderer.Request) (renderer.Result, error) {
				return renderer.Result{Complete: true, Fragments: test.fragments}, test.err
			})
			assessments, err := safety.AssessRendered(t.Context(), service, []ast.Node{ast.NewRawSQL("SELECT 1;")}, "postgres")
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(assessments, qt.IsNil)
		})
	}
}

func TestAssessRenderedDiscardsCanceledReply(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	service := selectedRendering(func(context.Context, renderer.Request) (renderer.Result, error) {
		cancel()
		return renderer.Result{Complete: true, Fragments: []string{"SELECT 1;"}}, nil
	})
	assessments, err := safety.AssessRendered(ctx, service, []ast.Node{ast.NewRawSQL("SELECT 1;")}, "postgres")
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(assessments, qt.IsNil)
}

func TestAssessRenderedRequiresServiceForNoop(t *testing.T) {
	c := qt.New(t)
	assessments, err := safety.AssessRendered(t.Context(), nil, nil, "postgres")
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(assessments, qt.IsNil)
}

func TestAssessRenderedKeepsExtensionRiskThroughContainers(t *testing.T) {
	payload := &unknownPayload{}
	for _, node := range []ast.Node{
		&ast.ExtensionStatement{Payload: payload},
		&ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: payload}}},
		&ast.StatementList{Statements: []ast.Node{&ast.StatementList{Statements: []ast.Node{&ast.ExtensionStatement{Payload: payload}}}}},
	} {
		c := qt.New(t)
		service := selectedRendering(func(context.Context, renderer.Request) (renderer.Result, error) {
			return renderer.Result{Complete: true, Fragments: []string{"CREATE WIDGET first; CREATE WIDGET second;"}}, nil
		})
		assessments, err := safety.AssessRendered(t.Context(), service, []ast.Node{node}, "custom")
		c.Assert(err, qt.IsNil)
		c.Assert(assessments, qt.HasLen, 2)
		for _, assessment := range assessments {
			c.Assert(assessment.Severity, qt.Equals, safety.Destructive)
			c.Assert(assessment.Reason, qt.Equals, "extension effects are unknown; manual review is required")
		}
	}
}
