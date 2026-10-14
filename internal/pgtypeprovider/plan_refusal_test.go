package pgtypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/feature/pgtype"
	"ptah.run/feature/pgtype/typeplan"
)

// TestPlanTypes_Receipts pins the strategy each change is accounted for
// with.
func TestPlanTypes_Receipts(t *testing.T) {
	text := &pgtype.ObservedDomain{BaseType: "text"}
	checked := &pgtype.ObservedDomain{BaseType: "text", Check: "(VALUE <> ''::text)",
		Checks: []pgtype.DomainCheck{{Name: "t_check", Expression: "(VALUE <> ''::text)"}}}
	tests := []struct {
		name   string
		change *pgtype.DomainChange
		want   string
		steps  int
	}{
		{name: "a creation", change: &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text"}}, steps: 1,
			want: "create the type before the columns, routines and types that name it"},
		{name: "a drop", change: &pgtype.DomainChange{Before: text}, steps: 1, want: "drop the type with CASCADE after the objects that use it"},
		{name: "a rebuild", change: &pgtype.DomainChange{Before: text, After: &pgtype.DesiredDomain{BaseType: "int"}, Differs: []string{pgtype.DiffBaseType}},
			steps: 2, want: "drop the type and create it again, as nothing changes it in place; the drop is not CASCADE and fails while the type is in use"},
		{name: "a change in place", change: &pgtype.DomainChange{Before: text, After: &pgtype.DesiredDomain{BaseType: "text", NotNull: true},
			Differs: []string{pgtype.DiffNotNull}}, steps: 1, want: "change the type in place"},
		{name: "a change in place and a comment", change: &pgtype.DomainChange{Before: text,
			After: &pgtype.DesiredDomain{BaseType: "text", NotNull: true, Comment: "mail"}, Differs: []string{pgtype.DiffComment, pgtype.DiffNotNull}},
			steps: 2, want: "change the type in place, then set its comment"},
		{name: "a comment", change: &pgtype.DomainChange{Before: text, After: &pgtype.DesiredDomain{BaseType: "text", Comment: "mail"},
			Differs: []string{pgtype.DiffComment}}, steps: 1, want: "set the type's comment"},
		{name: "a CHECK changed to none", change: &pgtype.DomainChange{Before: checked, After: &pgtype.DesiredDomain{BaseType: "text"},
			Differs: []string{pgtype.DiffCheck}}, steps: 1, want: "change the type in place"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planning(typeChange(pgtype.DomainKind, "t", test.change))

			result, err := newRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Strategy, qt.Equals, test.want)
			c.Assert(result.Changes[0].Kind, qt.Equals, pgtype.DomainChangeKind)
			c.Assert(result.Changes[0].Steps, qt.HasLen, test.steps)
		})
	}
}

// TestPlanTypes_RefusesWhatTheTargetCannotCreate pins the refusal of a type
// the target cannot create, which names the lexicographically first of its
// kind and the scope that keeps the declaration to the targets that host it.
// A column declared with the type would otherwise name something the server
// has no definition of (stokaro/ptah#1717).
func TestPlanTypes_RefusesWhatTheTargetCannotCreate(t *testing.T) {
	domain := func(name string) schemaext.ChangeRecord {
		return typeChange(pgtype.DomainKind, name, &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text"}})
	}
	composite := typeChange(pgtype.CompositeKind, "addr", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{
		Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}}})
	span := typeChange(pgtype.RangeKind, "span", &pgtype.RangeChange{After: &pgtype.DesiredRange{Subtype: "int4"}})
	tests := []struct {
		name    string
		target  string
		without []capability.Capability
		changes []schemaext.ChangeRecord
		index   int
		want    schemavalidation.Diagnostic
	}{
		{name: "the first domain by name", target: "cockroachdb", without: []capability.Capability{capability.DomainTypes},
			changes: []schemaext.ChangeRecord{domain("email"), domain("code")}, index: 1,
			want: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(pgtype.DomainChangeKind), Object: "code",
				Feature: "CREATE DOMAIN", Message: "cockroachdb: CREATE DOMAIN code: this target does not create it, and a column declared with the type " +
					"would be left naming something the server has no definition of; scope the declaration to the dialects that host it"}},
		{name: "a domain before a composite type", target: "spanner",
			without: []capability.Capability{capability.DomainTypes, capability.CompositeTypes},
			changes: []schemaext.ChangeRecord{composite, domain("email")}, index: 1,
			want: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(pgtype.DomainChangeKind), Object: "email",
				Feature: "CREATE DOMAIN", Message: "spanner: CREATE DOMAIN email: this target does not create it, and a column declared with the type " +
					"would be left naming something the server has no definition of; scope the declaration to the dialects that host it"}},
		{name: "a composite type", target: "spanner", without: []capability.Capability{capability.CompositeTypes},
			changes: []schemaext.ChangeRecord{composite},
			want: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(pgtype.CompositeChangeKind), Object: "addr",
				Feature: "CREATE TYPE ... AS (...)", Message: "spanner: CREATE TYPE ... AS (...) addr: this target does not create it, and a column " +
					"declared with the type would be left naming something the server has no definition of; scope the declaration to the dialects that host it"}},
		{name: "a range type", target: "cockroachdb", without: []capability.Capability{capability.RangeTypes},
			changes: []schemaext.ChangeRecord{domain("email"), span}, index: 1,
			want: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(pgtype.RangeChangeKind), Object: "span",
				Feature: "CREATE TYPE ... AS RANGE", Message: "cockroachdb: CREATE TYPE ... AS RANGE span: this target does not create it, and a column " +
					"declared with the type would be left naming something the server has no definition of; scope the declaration to the dialects that host it"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planning(test.changes...)
			request.Target = test.target
			for _, key := range test.without {
				request.Capabilities = request.Capabilities.With(key, false)
			}

			result, err := newRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, []featureplan.Diagnostic{{Problem: test.want, Change: new(test.index)}})
			c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(result.Contributions, qt.HasLen, 0)
		})
	}
}

// TestPlanTypes_LeavesWhatTheTargetHolds is the control the refusal needs: a
// type the target does not create may still be dropped, changed or rebuilt
// there, since the database already holds it.
func TestPlanTypes_LeavesWhatTheTargetHolds(t *testing.T) {
	c := qt.New(t)
	text := &pgtype.ObservedDomain{BaseType: "text"}
	request := planning(
		typeChange(pgtype.DomainKind, "a", &pgtype.DomainChange{Before: text}),
		typeChange(pgtype.DomainKind, "b", &pgtype.DomainChange{Before: text, After: &pgtype.DesiredDomain{BaseType: "int"},
			Differs: []string{pgtype.DiffBaseType}}),
	)
	request.Target = "cockroachdb"
	request.Capabilities = request.Capabilities.With(capability.DomainTypes, false)

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 2)
}

// TestPlanTypes_DeclaresForARender pins a whole-schema render: each declared
// type is created, one another names first, and followed by its comment.
func TestPlanTypes_DeclaresForARender(t *testing.T) {
	c := qt.New(t)
	request := featureplan.DeclarationRequest{Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Postgres17(),
		Objects: []schemaext.Object{
			must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, "", "d"), &pgtype.DesiredDomain{BaseType: "addr", Comment: "mail"})),
			must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.CompositeKind, "", "addr"),
				&pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}})),
		}}

	result, err := newRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(summaries(stepsOf(c, result.Contributions)), qt.DeepEquals, []summary{
		{"composite type addr", pgtype.CreateCompositeKind}, {"domain d", pgtype.CreateDomainKind}, {"domain d", pgtype.CommentKind},
	})
	c.Assert(result.Declarations, qt.HasLen, 2)
	c.Assert(result.Declarations[0].Steps, qt.HasLen, 2)
	c.Assert(result.Declarations[1].Steps, qt.HasLen, 1)
	c.Assert(result.Declarations[0].Strategy, qt.Equals, "create the type before the columns, routines and types that name it")
}

// TestPlanTypes_RefusesADeclarationTheTargetCannotCreate pins that a render
// refuses a declared type the target cannot create, naming the declared
// model.
func TestPlanTypes_RefusesADeclarationTheTargetCannotCreate(t *testing.T) {
	c := qt.New(t)
	caps := capability.Postgres17().With(capability.RangeTypes, false)
	request := featureplan.DeclarationRequest{Target: "cockroachdb", Identifiers: identifier.ForDialect("cockroachdb"), Capabilities: caps,
		Objects: []schemaext.Object{must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, "", "span"), &pgtype.DesiredRange{Subtype: "int4"}))}}

	result, err := newRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Kind, qt.Equals, string(pgtype.RangeKind))
	c.Assert(result.Diagnostics[0].Object, qt.DeepEquals, new(0))
	c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}

// TestPlanTypes_FailurePath refuses what no plan of user types can answer.
// The runtime sends the owner nothing it was not registered for, so each is
// asked of the service directly.
func TestPlanTypes_FailurePath(t *testing.T) {
	created := typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text"}})
	outside := planning(created)
	outside.Target = "mysql"
	parented := planning(created)
	parented.ParentKinds = []schemaext.Kind{pgtype.DomainKind}
	tests := []struct {
		name    string
		request featureplan.Request
		is      error
		want    string
	}{
		{name: "a target outside the family", request: outside, is: ptaherr.ErrUnsupportedDialect, want: `.*PostgreSQL user type planning on "mysql"`},
		{name: "parent kinds", request: parented, is: schemaext.ErrInvalidValue, want: `.*a user type belongs to no table`},
		{name: "a change of another owner", is: schemaext.ErrInvalidValue, want: `.*unexpected user type change .*`,
			request: planning(schemaext.ChangeRecord{Subject: pgtype.Ref(pgtype.DomainKind, "", "t"), Value: nil})},
		{name: "a change of another model than its subject", is: schemaext.ErrInvalidValue, want: `.*composite.*change for .*`,
			request: planning(typeChange(pgtype.DomainKind, "t", &pgtype.CompositeChange{
				After: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}}}))},
		{name: "an invalid change", is: schemaext.ErrInvalidValue, want: `.*`,
			request: planning(typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{}))},
		{name: "a subject of no owned kind", is: schemaext.ErrInvalidValue, want: `.*is not a PostgreSQL user type kind`,
			request: planning(schemaext.ChangeRecord{Subject: pgtype.Ref("example.org/other", "", "t"), Value: created.Value})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := typeplan.Service{}.PlanFeatures(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}
