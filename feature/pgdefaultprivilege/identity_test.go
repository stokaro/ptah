package pgdefaultprivilege_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

func TestRef_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		identity pgdefaultprivilege.Identity
	}{
		{name: "whitespace in identifiers", identity: pgdefaultprivilege.Identity{Grantor: " owner ", Schema: " app ", ObjectType: "TABLES", Grantee: " reader "}},
		{name: "global", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}},
		{name: "scoped", identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "public", ObjectType: "TABLES", Grantee: "reader"}},
		{name: "punctuation", identity: pgdefaultprivilege.Identity{Grantor: `a|b,\"c`, Schema: `d.e|f`, ObjectType: "TYPES", Grantee: `g\\h,\"i`}},
		{name: "schema defaults", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "SCHEMAS", Grantee: "reader"}},
		{name: "large objects", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "LARGE OBJECTS", Grantee: "reader"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref, err := pgdefaultprivilege.Ref(test.identity, identifier.ForDialect("postgres"))
			c.Assert(err, qt.IsNil)
			identity, err := pgdefaultprivilege.IdentityOf(ref)
			c.Assert(err, qt.IsNil)
			c.Assert(identity, qt.Equals, test.identity)
			c.Assert(ref.Schema.Source, qt.Equals, test.identity.Schema)
			c.Assert(ref.Schema.Defaulted, qt.IsFalse)
			c.Assert(ref.Parent, qt.Equals, objectidentity.Part{})
		})
	}
}

func TestRef_SeparatesEveryIdentityComponent(t *testing.T) {
	c := qt.New(t)
	identities := []pgdefaultprivilege.Identity{
		{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"},
		{Grantor: " owner ", ObjectType: "TABLES", Grantee: "reader"},
		{Grantor: "owner", ObjectType: "TABLES", Grantee: " reader "},
		{Grantor: "owner", Schema: "public", ObjectType: "TABLES", Grantee: "reader"},
		{Grantor: "Owner", ObjectType: "TABLES", Grantee: "reader"},
		{Grantor: "owner", ObjectType: "SEQUENCES", Grantee: "reader"},
		{Grantor: "owner", ObjectType: "TABLES", Grantee: "Reader"},
		{Grantor: "a|TABLES|b", ObjectType: "TABLES", Grantee: "c"},
		{Grantor: "a", ObjectType: "TABLES", Grantee: "b|TABLES|c"},
	}
	keys := make(map[objectidentity.Key]bool)
	for _, identity := range identities {
		ref, err := pgdefaultprivilege.Ref(identity, identifier.ForDialect("postgres"))
		c.Assert(err, qt.IsNil)
		keys[ref.Key()] = true
	}
	c.Assert(keys, qt.HasLen, len(identities))
}

func TestRef_FailurePath(t *testing.T) {
	tests := []pgdefaultprivilege.Identity{
		{ObjectType: "TABLES", Grantee: "reader"},
		{Grantor: "owner", ObjectType: "TABLES"},
		{Grantor: "owner", ObjectType: "unsupported", Grantee: "reader"},
		{Grantor: "owner", Schema: "public", ObjectType: "SCHEMAS", Grantee: "reader"},
		{Grantor: "owner", Schema: "public", ObjectType: "LARGE OBJECTS", Grantee: "reader"},
		{Grantor: "own\x00er", ObjectType: "TABLES", Grantee: "reader"},
	}
	for _, test := range tests {
		t.Run(test.Grantor+test.ObjectType+test.Grantee, func(t *testing.T) {
			c := qt.New(t)
			ref, err := pgdefaultprivilege.Ref(test, identifier.ForDialect("postgres"))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(ref, qt.Equals, objectidentity.ID{})
		})
	}
}

func TestIdentityOf_RefusesMalformedReferences(t *testing.T) {
	base := objectidentity.ID{Kind: objectidentity.Kind(pgdefaultprivilege.DefaultPrivilegeKind),
		Name: objectidentity.Part{Source: `["owner","TABLES","reader"]`, Normalized: `["owner","TABLES","reader"]`}}
	malformedTuple := base
	malformedTuple.Name.Source = `["owner","TABLES"]`
	catalog := base
	catalog.Catalog.Source = "database"
	parent := base
	parent.Parent.Source = "table"
	defaulted := base
	defaulted.Schema = objectidentity.Part{Source: "public", Normalized: "public", Defaulted: true}
	mismatch := base
	mismatch.Name.Normalized = `["owner","FUNCTIONS","reader"]`
	for _, ref := range []objectidentity.ID{malformedTuple, catalog, parent, defaulted, mismatch} {
		t.Run(ref.String(), func(t *testing.T) {
			c := qt.New(t)
			identity, err := pgdefaultprivilege.IdentityOf(ref)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(identity, qt.Equals, pgdefaultprivilege.Identity{})
		})
	}
}
