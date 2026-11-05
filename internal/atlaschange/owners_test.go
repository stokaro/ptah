package atlaschange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgtype"
)

// ownerStatement is a statement an owner renders.
func ownerStatement(payload ast.ExtensionPayload) ast.Node {
	return &ast.ExtensionStatement{Payload: payload}
}

// reportingOperation is an operation of an owner the namer has no words of its
// own for, which says what it changes.
type reportingOperation struct {
	kind, name string
	action     ast.ExtensionChangeAction
}

func (*reportingOperation) Kind() schemaext.Kind                   { return "example.org/feature/op" }
func (o *reportingOperation) CloneExtension() ast.ExtensionPayload { clone := *o; return &clone }
func (o *reportingOperation) OmissionSubject() (kind, name string) { return o.kind, o.name }
func (o *reportingOperation) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: o.action, Name: o.name}
}

// TestOwnerOperations_AreNamedAfterTheirObject_HappyPath pins the comment above
// each statement a built-in feature owner renders, and the comment its rollback
// statement carries. The community binary plans none of these objects, so the
// rows take the pattern of the objects it does plan.
func TestOwnerOperations_AreNamedAfterTheirObject_HappyPath(t *testing.T) {
	domain := pgtype.Target{Kind: pgtype.DomainKind, Schema: "public", Name: "posint"}
	composite := pgtype.Target{Kind: pgtype.CompositeKind, Name: "wrap"}
	span := pgtype.Target{Kind: pgtype.RangeKind, Name: "span"}
	before, after := &pgpolicy.ObservedPolicy{}, &pgpolicy.DesiredPolicy{}
	tests := []struct {
		name       string
		payload    ast.ExtensionPayload
		wantOf     string
		wantUndone string
	}{
		{
			name:       "a new extension",
			payload:    &pgextension.Operation{Name: "hstore", Action: pgextension.Create},
			wantOf:     `create "hstore" extension`,
			wantUndone: `drop "hstore" extension`,
		},
		{
			name:       "a dropped extension",
			payload:    &pgextension.Operation{Name: "hstore", Action: pgextension.Drop},
			wantOf:     `drop "hstore" extension`,
			wantUndone: `create "hstore" extension`,
		},
		{
			name:       "an updated extension",
			payload:    &pgextension.Operation{Name: "hstore", Action: pgextension.UpdateVersion, Version: "1.8"},
			wantOf:     `modify "hstore" extension`,
			wantUndone: `modify "hstore" extension`,
		},
		{
			name:       "an extension comment",
			payload:    &pgextension.CommentOperation{Name: "hstore", Comment: "kv"},
			wantOf:     `set comment to extension: "hstore"`,
			wantUndone: `set comment to extension: "hstore"`,
		},
		{
			name:       "a new policy",
			payload:    &pgpolicy.PolicyOperation{Table: "t", Name: "p", Change: pgpolicy.PolicyChange{After: after}},
			wantOf:     `create "p" policy`,
			wantUndone: `drop "p" policy`,
		},
		{
			name:       "a dropped policy",
			payload:    &pgpolicy.PolicyOperation{Table: "t", Name: "p", Change: pgpolicy.PolicyChange{Before: before}},
			wantOf:     `drop "p" policy`,
			wantUndone: `create "p" policy`,
		},
		{
			name:       "a changed policy",
			payload:    &pgpolicy.PolicyOperation{Table: "t", Name: "p", Change: pgpolicy.PolicyChange{Before: before, After: after}},
			wantOf:     `modify "p" policy`,
			wantUndone: `modify "p" policy`,
		},
		{
			name:       "a policy comment",
			payload:    &pgpolicy.PolicyCommentOperation{Table: "t", Name: "p", Comment: "pp"},
			wantOf:     `set comment to policy: "p"`,
			wantUndone: `set comment to policy: "p"`,
		},
		{
			name:       "row-level security switched on a table",
			payload:    &pgpolicy.TableStateOperation{Table: "t"},
			wantOf:     `modify "t" table`,
			wantUndone: `modify "t" table`,
		},
		{
			name:       "a new domain",
			payload:    &pgtype.CreateDomain{Target: domain},
			wantOf:     `create "posint" domain`,
			wantUndone: `drop "posint" domain`,
		},
		{
			name:       "a changed domain",
			payload:    &pgtype.AlterDomain{Target: domain},
			wantOf:     `modify "posint" domain`,
			wantUndone: `modify "posint" domain`,
		},
		{
			name:       "a dropped domain",
			payload:    &pgtype.Drop{Target: domain},
			wantOf:     `drop "posint" domain`,
			wantUndone: `create "posint" domain`,
		},
		{
			name:       "a domain comment",
			payload:    &pgtype.Comment{Target: domain, Comment: "pos"},
			wantOf:     `set comment to domain: "posint"`,
			wantUndone: `set comment to domain: "posint"`,
		},
		{
			name:       "a new composite type",
			payload:    &pgtype.CreateComposite{Target: composite},
			wantOf:     `create "wrap" composite type`,
			wantUndone: `drop "wrap" composite type`,
		},
		{
			name:       "a changed composite type",
			payload:    &pgtype.AlterComposite{Target: composite},
			wantOf:     `modify "wrap" composite type`,
			wantUndone: `modify "wrap" composite type`,
		},
		{
			name:       "a dropped composite type",
			payload:    &pgtype.Drop{Target: composite},
			wantOf:     `drop "wrap" composite type`,
			wantUndone: `create "wrap" composite type`,
		},
		{
			name:       "a new range type",
			payload:    &pgtype.CreateRange{Target: span},
			wantOf:     `create "span" range type`,
			wantUndone: `drop "span" range type`,
		},
		{
			name:       "a dropped range type",
			payload:    &pgtype.Drop{Target: span},
			wantOf:     `drop "span" range type`,
			wantUndone: `create "span" range type`,
		},
		{
			name:       "another owner's new object",
			payload:    &reportingOperation{kind: "hypertable", name: "public.metrics", action: ast.ExtensionAdd},
			wantOf:     `create "metrics" hypertable`,
			wantUndone: `drop "metrics" hypertable`,
		},
		{
			name:       "another owner's changed object",
			payload:    &reportingOperation{kind: "continuous aggregate", name: "hourly", action: ast.ExtensionModify},
			wantOf:     `modify "hourly" continuous aggregate`,
			wantUndone: `modify "hourly" continuous aggregate`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			of, ok := nameOf(ownerStatement(test.payload))
			c.Assert(ok, qt.IsTrue)
			c.Assert(of, qt.Equals, test.wantOf)

			undone, ok := nameUndone(ownerStatement(test.payload))
			c.Assert(ok, qt.IsTrue)
			c.Assert(undone, qt.Equals, test.wantUndone)
		})
	}
}

// TestRollback_AnOwnerObjectTheForwardPlanModifies_HappyPath pins a rollback
// that drops and creates an owner's object the forward plan only changed: both
// statements undo the forward `modify`. A policy of the same name on another
// table, and a domain of the same name in another schema, are other objects.
func TestRollback_AnOwnerObjectTheForwardPlanModifies_HappyPath(t *testing.T) {
	before, after := &pgpolicy.ObservedPolicy{}, &pgpolicy.DesiredPolicy{}
	changedPolicy := ownerStatement(&pgpolicy.PolicyOperation{Table: "t", Name: "p", Change: pgpolicy.PolicyChange{Before: before, After: after}})
	changedDomain := ownerStatement(&pgtype.AlterDomain{Target: pgtype.Target{Kind: pgtype.DomainKind, Schema: "app", Name: "d"}})
	tests := []struct {
		name    string
		forward ast.Node
		node    ast.Node
		want    string
	}{
		{
			name:    "a policy dropped on its table",
			forward: changedPolicy,
			node:    ownerStatement(&pgpolicy.PolicyOperation{Table: "t", Name: "p", Change: pgpolicy.PolicyChange{Before: before}}),
			want:    `modify "p" policy`,
		},
		{
			name:    "a policy of the same name on another table",
			forward: changedPolicy,
			node:    ownerStatement(&pgpolicy.PolicyOperation{Table: "u", Name: "p", Change: pgpolicy.PolicyChange{Before: before}}),
			want:    `create "p" policy`,
		},
		{
			name:    "a domain created in its schema",
			forward: changedDomain,
			node:    ownerStatement(&pgtype.CreateDomain{Target: pgtype.Target{Kind: pgtype.DomainKind, Schema: "app", Name: "d"}}),
			want:    `modify "d" domain`,
		},
		{
			name:    "a domain of the same name in another schema",
			forward: changedDomain,
			node:    ownerStatement(&pgtype.CreateDomain{Target: pgtype.Target{Kind: pgtype.DomainKind, Schema: "other", Name: "d"}}),
			want:    `drop "d" domain`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := nameRollback([]ast.Node{test.forward}, test.node)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestOwnerOperations_UnnamedWithoutASubject_FailurePath is an operation that
// reports nothing, or a report with no object in it.
func TestOwnerOperations_UnnamedWithoutASubject_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		payload ast.ExtensionPayload
	}{
		{name: "an operation that reports nothing", payload: opaqueOperation{}},
		{name: "a report with no kind", payload: &reportingOperation{name: "metrics", action: ast.ExtensionAdd}},
		{name: "a report with no name", payload: &reportingOperation{kind: "hypertable", action: ast.ExtensionAdd}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			of, ok := nameOf(ownerStatement(test.payload))

			c.Assert(ok, qt.IsFalse)
			c.Assert(of, qt.Equals, "")
		})
	}
}
