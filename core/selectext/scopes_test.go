package selectext_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
)

type scopeAnswer struct {
	requests *[]selectext.ScopeRequest
	result   selectext.ScopeResult
	err      error
	cancel   context.CancelFunc
}

func (a scopeAnswer) DescribeScopes(_ context.Context, request selectext.ScopeRequest) (selectext.ScopeResult, error) {
	if a.requests != nil {
		*a.requests = append(*a.requests, request)
	}
	if a.cancel != nil {
		a.cancel()
	}
	return a.result, a.err
}

func scopeExtension(owner string, service selectext.ScopeService, kinds ...schemaext.Kind) selectext.Extension {
	extension := selectext.Extension{Owner: owner, Scopes: service}
	for _, kind := range kinds {
		extension.Selections = append(extension.Selections, selectext.Selection{Kind: kind, Naming: selectext.SchemaWide})
	}
	return extension
}

func scopeSubject(kind schemaext.Kind, name string) selectext.ScopeSubject {
	return selectext.ScopeSubject{Kind: kind, Subject: ref(kind, name)}
}

func TestSet_DescribeScopesRoutesObjectsAndCoverage(t *testing.T) {
	c := qt.New(t)
	const a, b, foreign schemaext.Kind = "example.org/a/x", "example.org/b/x", "example.org/c/x"
	held := scopeSubject(a, "held")
	unknown := selectext.ScopeSubject{Kind: a, Subject: ref("example.org/a/class", "unknown")}
	other := scopeSubject(b, "other")
	alias := held.Subject
	alias.Name.Source = "HELD"
	coverage := must.Must(schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{
		{Model: schemaext.CodecIdentity{Owner: "example.org/a", Kind: a, Representation: schemaext.Desired, Version: 1, Definition: "definition"}, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})},
	}, []schemaext.SubjectCoverage{
		{Kind: a, Subject: alias, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}, {Kind: a, Subject: unknown.Subject, Knowledge: schemaext.Knowledge{Reason: "class is unknown"}},
	}))
	var first, second, idle []selectext.ScopeRequest
	roles := []string{"writer", "reader"}
	set := must.Must(selectext.NewSet(
		scopeExtension("example.org/b", scopeAnswer{requests: &second, result: selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: other}}}}, b),
		scopeExtension("example.org/a", scopeAnswer{requests: &first, result: selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{
			{ScopeSubject: unknown, Schema: "app"}, {ScopeSubject: held, Schema: " app ", Roles: roles},
		}}}, a),
		scopeExtension("example.org/idle", scopeAnswer{requests: &idle}, "example.org/idle/x"),
	))
	objects := must.Must(schemaext.NewObjects(object(a, "held", "label"), object(b, "other", ""), object(foreign, "foreign", "")))
	result, err := set.DescribeScopes(c.Context(), selectext.ScopeRequest{Objects: objects, Coverage: coverage})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Subjects, qt.DeepEquals, []selectext.SubjectScope{
		{ScopeSubject: unknown, Schema: "app"}, {ScopeSubject: held, Schema: " app ", Roles: []string{"reader", "writer"}}, {ScopeSubject: other},
	})
	c.Assert(first, qt.HasLen, 1)
	c.Assert(second, qt.HasLen, 1)
	c.Assert(idle, qt.HasLen, 0)
	c.Assert(first[0].Objects.Refs(), qt.DeepEquals, []objectidentity.ID{held.Subject})
	c.Assert(first[0].Subjects(), qt.DeepEquals, []selectext.ScopeSubject{unknown, held})
	c.Assert(second[0].Coverage.SubjectRecords(), qt.HasLen, 0)
	c.Assert(set.ForType(""), qt.HasLen, 0)
	roles[0] = "changed"
	c.Assert(result.Subjects[1].Roles, qt.DeepEquals, []string{"reader", "writer"})
	result.Subjects[1].Roles[0] = "changed"
	c.Assert(roles[1], qt.Equals, "reader")
}

func TestSet_DescribeScopesRejectsMalformedReply(t *testing.T) {
	const kind schemaext.Kind = "example.org/a/x"
	subject := scopeSubject(kind, "held")
	renamed := subject
	renamed.Subject.Name.Source = "HELD"
	tests := []struct {
		name   string
		result selectext.ScopeResult
	}{
		{name: "zero reply"},
		{name: "missing subject", result: selectext.ScopeResult{Complete: true}},
		{name: "unknown subject", result: selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: scopeSubject(kind, "other")}}}},
		{name: "changed source identity", result: selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: renamed}}}},
		{name: "duplicate subject", result: selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: subject}, {ScopeSubject: subject}}}},
		{name: "empty role", result: selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: subject, Roles: []string{""}}}}},
		{name: "duplicate role", result: selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: subject, Roles: []string{"reader", "reader"}}}}},
		{name: "invalid role", result: selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: subject, Roles: []string{"\x00"}}}}},
		{name: "invalid schema", result: selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: subject, Schema: "\xff"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := must.Must(selectext.NewSet(scopeExtension("example.org/a", scopeAnswer{result: test.result}, kind)))
			result, err := set.DescribeScopes(c.Context(), selectext.ScopeRequest{Objects: must.Must(schemaext.NewObjects(object(kind, "held", "")))})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, selectext.ScopeResult{})
		})
	}
}

// scopeFailureFixture varies dispatch conditions, without varying assertions.
func scopeFailureFixture(parent context.Context, mode string, refused error) (selectext.Set, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	answer := scopeAnswer{}
	switch mode {
	case "unselected":
		return selectext.Set{}, ctx, cancel
	case "nil context":
		ctx = nil
	case "canceled before dispatch":
		cancel()
	case "canceled during dispatch":
		answer.cancel = cancel
	case "owner failure":
		answer.err = refused
	}
	set := must.Must(selectext.NewSet(
		scopeExtension("example.org/first", scopeAnswer{result: selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: scopeSubject("example.org/first/x", "one")}}}}, "example.org/first/x"),
		scopeExtension("example.org/a", answer, "example.org/a/x"),
	))
	return set, ctx, cancel
}

func TestSet_DescribeScopesFailureIsAtomic(t *testing.T) {
	refused := errors.New("owner refused")
	tests := []struct {
		name string
		want error
	}{
		{name: "unselected", want: selectext.ErrUnselected},
		{name: "nil context", want: schemaext.ErrInvalidValue},
		{name: "canceled before dispatch", want: context.Canceled},
		{name: "canceled during dispatch", want: context.Canceled},
		{name: "owner failure", want: refused},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, ctx, cancel := scopeFailureFixture(c.Context(), test.name, refused)
			defer cancel()
			objects := must.Must(schemaext.NewObjects(object("example.org/first/x", "one", ""), object("example.org/a/x", "held", "")))
			result, err := set.DescribeScopes(ctx, selectext.ScopeRequest{Objects: objects})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, selectext.ScopeResult{})
		})
	}
}

func TestNewSet_RefusesInvalidSchemaWideSelection(t *testing.T) {
	tests := []struct {
		name      string
		selection selectext.Selection
		service   selectext.ScopeService
	}{
		{name: "no service", selection: selectext.Selection{Kind: "example.org/a/x", Naming: selectext.SchemaWide}},
		{name: "selector type", selection: selectext.Selection{Kind: "example.org/a/x", Naming: selectext.SchemaWide, Type: "acl"}, service: scopeAnswer{}},
		{name: "relation", selection: selectext.Selection{Kind: "example.org/a/x", Naming: selectext.SchemaWide, Relation: true}, service: scopeAnswer{}},
		{name: "field", selection: selectext.Selection{Kind: "example.org/a/x", Naming: selectext.SchemaWide, Fields: []string{"comment"}}, service: scopeAnswer{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			extension := selectext.Extension{Owner: "example.org/a", Scopes: test.service, Selections: []selectext.Selection{test.selection}}
			set, err := selectext.NewSet(extension)
			c.Assert(err, qt.ErrorMatches, "scope selection of example.org/a requires a schema-wide service and no selector type, relation or fields")
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

func TestNewSet_RefusesMultipleScopeServicesForOneOwner(t *testing.T) {
	c := qt.New(t)
	set, err := selectext.NewSet(
		scopeExtension("example.org/a", scopeAnswer{}, "example.org/a/x"),
		scopeExtension("example.org/a", scopeAnswer{}, "example.org/a/y"),
	)
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(set.Selected(), qt.IsFalse)
}
