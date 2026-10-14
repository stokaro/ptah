package privilegeselect_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegeselect"
)

func TestDescribeScopesPreservesDecodedNamesAndBothRoles(t *testing.T) {
	tests := []struct {
		name     string
		identity pgdefaultprivilege.Identity
		roles    []string
	}{
		{name: "both roles", identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, roles: []string{"owner", "reader"}},
		{name: "self grant", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"}, roles: []string{"owner"}},
		{name: "public", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}, roles: []string{"owner"}},
		{name: "punctuation and whitespace", identity: pgdefaultprivilege.Identity{Grantor: " owner ", Schema: " app ", ObjectType: "TABLES", Grantee: `a."b`}, roles: []string{" owner ", `a."b`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, value := range []schemaext.Value{&pgdefaultprivilege.Desired{}, &pgdefaultprivilege.Observed{}} {
				c := qt.New(t)
				ref := must.Must(pgdefaultprivilege.Ref(test.identity, identifier.ForDialect("postgres")))
				request := selectext.ScopeRequest{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: value}))}
				result, err := (privilegeselect.Service{}).DescribeScopes(c.Context(), request)
				c.Assert(err, qt.IsNil)
				c.Assert(result, qt.DeepEquals, selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: selectext.ScopeSubject{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref}, Schema: test.identity.Schema, Roles: test.roles}}})
			}
		})
	}
}

func TestDescribeScopesCoversUnknownClasses(t *testing.T) {
	for _, test := range []struct {
		grantor string
		roles   []string
	}{{}, {grantor: "owner", roles: []string{"owner"}}} {
		t.Run(test.grantor, func(t *testing.T) {
			c := qt.New(t)
			ref := must.Must(pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{Grantor: test.grantor, Schema: "app", ObjectType: "unknown-code"}, identifier.ForDialect("postgres")))
			coverage := must.Must(pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
				{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "class cannot be read"}},
			}))
			result, err := (privilegeselect.Service{}).DescribeScopes(c.Context(), selectext.ScopeRequest{Coverage: coverage})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, selectext.ScopeResult{Complete: true, Subjects: []selectext.SubjectScope{{ScopeSubject: selectext.ScopeSubject{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref}, Schema: "app", Roles: test.roles}}})
		})
	}
}

func TestDescribeScopesRejectsMalformedState(t *testing.T) {
	c := qt.New(t)
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"}, identifier.ForDialect("postgres")))
	ref.Name.Source = "not an identity"
	request := selectext.ScopeRequest{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &pgdefaultprivilege.Desired{}}))}
	result, err := (privilegeselect.Service{}).DescribeScopes(c.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, selectext.ScopeResult{})
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = (privilegeselect.Service{}).DescribeScopes(ctx, selectext.ScopeRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, selectext.ScopeResult{})
}
