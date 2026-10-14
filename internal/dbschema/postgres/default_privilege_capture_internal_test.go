package postgres

// White-box testing required: recordDefaultPrivilegeObjects captures the
// catalog query's private row shape. This boundary must group interleaved rows
// and preserve decoded role names before they reach the public schema model.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

func TestDefaultPrivilegeCaptureGroupsCatalogRowsByFullIdentity(t *testing.T) {
	c := qt.New(t)
	reader := NewPostgreSQLReaderWithCapabilities(nil, "public", capability.Postgres18())
	database := &catalog.Database{}
	err := reader.recordDefaultPrivilegeObjects(database, []defaultPrivilegeRow{
		{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader", Privilege: "SELECT"},
		{Grantor: "owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "reader", Privilege: "USAGE"},
		{Grantor: "reporting_owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader", Privilege: "SELECT"},
		{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader", Privilege: "INSERT", WithOption: true},
		{Grantor: "owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "reader", Privilege: "SELECT"},
		{Grantor: "owner ", Schema: "app", ObjectType: "TABLES", Grantee: "reader", Privilege: "SELECT"},
		{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC", Privilege: "EXECUTE", Revoked: true},
	}, nil)
	c.Assert(err, qt.IsNil)
	var want []schemaext.Object
	for _, row := range []struct {
		identity pgdefaultprivilege.Identity
		value    pgdefaultprivilege.Observed
	}{
		{identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, value: pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT", WithOption: true}, {Privilege: "SELECT"}}}},
		{identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "reader"}, value: pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}, {Privilege: "USAGE"}}}},
		{identity: pgdefaultprivilege.Identity{Grantor: "reporting_owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, value: pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
		{identity: pgdefaultprivilege.Identity{Grantor: "owner ", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, value: pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
		{identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}, value: pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}},
	} {
		ref := must.Must(pgdefaultprivilege.Ref(row.identity, identifier.ForDialect("postgres")))
		want = append(want, schemaext.Object{Ref: ref, Value: &row.value})
		c.Assert(database.FeatureCoverage.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, ref).State, qt.Equals, schemaext.Complete)
	}
	c.Assert(must.Must(database.FeatureObjects.All()), qt.ContentEquals, want)
}
