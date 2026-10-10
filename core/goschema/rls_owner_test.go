package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

const ownedPolicySource = "package models\n\n//ptah:schema:table name=\"docs\"\n" +
	"//ptah:schema:rls:policy name=\"readers\" table=\"docs\" using=\"true\"\ntype Doc struct{}\n"

// TestParseSource_TheRowSecurityOwnerReadsAnUnscopedPolicy pins the routing
// of a policy that names no target: the row-security owner reads it as its
// policy, and the source claims to describe every policy.
func TestParseSource_TheRowSecurityOwnerReadsAnUnscopedPolicy(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(rowSecurityOwners, "docs.go", ownedPolicySource)

	c.Assert(err, qt.IsNil)
	c.Assert(db.RLSPolicies, qt.HasLen, 0)
	c.Assert(db.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{pgpolicysource.Ref("", "docs", "readers")})
	other := pgpolicysource.Ref("", "docs", "other")
	c.Assert(db.FeatureCoverage.Lookup(pgpolicy.PolicyKind, other).State, qt.Equals, schemaext.Complete)
}

// TestParseSource_WithoutTheRowSecurityOwnerAPolicyStaysShared is the control
// on the routing: without the owner, the frontend keeps the declaration in the
// shared model and claims no knowledge of the owner's policies, so they stay
// unknown rather than absent.
func TestParseSource_WithoutTheRowSecurityOwnerAPolicyStaysShared(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(annotation.None(), "docs.go", ownedPolicySource)

	c.Assert(err, qt.IsNil)
	c.Assert(db.RLSPolicies, qt.HasLen, 1)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
	other := pgpolicysource.Ref("", "docs", "other")
	c.Assert(db.FeatureCoverage.Lookup(pgpolicy.PolicyKind, other).State, qt.Not(qt.Equals), schemaext.Complete)
}

// TestParseSource_WithoutTheSQLServerOwnerASecurityPolicyStaysShared is the
// control on the SQL Server route: with the row-security owner alone, a
// policy scoped to SQL Server is no owner's and stays a shared declaration,
// and the source claims no knowledge of security policies.
func TestParseSource_WithoutTheSQLServerOwnerASecurityPolicyStaysShared(t *testing.T) {
	c := qt.New(t)
	postgresOnly := must.Must(annotation.NewSet(pgpolicysource.Annotations()))
	source := "package models\n\n//ptah:schema:table name=\"tenants\"\n" +
		"//ptah:schema:rls:policy name=\"p\" table=\"tenants\" using=\"dbo.fn(id)\" dialects=\"mssql\"\ntype Tenant struct{}\n"

	db, err := goschema.ParseSource(postgresOnly, "tenants.go", source)

	c.Assert(err, qt.IsNil)
	c.Assert(db.RLSPolicies, qt.HasLen, 1)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(db.FeatureCoverage.Lookup(mssqlschema.SecurityPolicyKind, mssqlschema.SecurityPolicyRef("dbo", "other")).State,
		qt.Not(qt.Equals), schemaext.Complete)
}
