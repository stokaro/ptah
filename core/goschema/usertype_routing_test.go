package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/feature/oratype"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/builtintest"
)

// userTypeSource declares a domain and a composite type, each scoped as
// scope writes it.
func userTypeSource(scope string) string {
	return "package fixture\n\n" +
		`//ptah:schema:domain name="email" type="TEXT" not_null="true"` + scope + "\n" +
		"type Email struct{}\n\n" +
		`//ptah:schema:composite name="address" fields="street:TEXT"` + scope + "\n" +
		"type Address struct{}\n"
}

// TestParse_UserTypesReachTheTypeOwnerWithoutAnotherTarget pins the routing of
// the domain and composite directives: a declaration that names no target, or
// only PostgreSQL-family targets, is the PostgreSQL type owner's.
func TestParse_UserTypesReachTheTypeOwnerWithoutAnotherTarget(t *testing.T) {
	for _, test := range []struct {
		name, scope string
		targets     []string
	}{
		{name: "no target"},
		{name: "PostgreSQL", scope: ` dialects="postgresql"`, targets: []string{"postgres"}},
		{name: "the family", scope: ` dialects="postgres,yugabytedb"`, targets: []string{"postgres", "yugabytedb"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(builtintest.Annotations(), "fixture.go", userTypeSource(test.scope))

			c.Assert(err, qt.IsNil)
			objects := must.Must(db.FeatureObjects.All())
			c.Assert(objects, qt.HasLen, 2)
			c.Assert(objects[0].Ref, qt.DeepEquals, pgtype.Ref(pgtype.CompositeKind, "", "address"))
			c.Assert(objects[0].Value, qt.DeepEquals, &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "TEXT"}},
				StructName: "Address"})
			c.Assert(objects[0].Targets, qt.DeepEquals, test.targets)
			c.Assert(objects[1].Ref, qt.DeepEquals, pgtype.Ref(pgtype.DomainKind, "", "email"))
			c.Assert(objects[1].Value, qt.DeepEquals, &pgtype.DesiredDomain{BaseType: "TEXT", NotNull: true, StructName: "Email"})
			c.Assert(objects[1].Targets, qt.DeepEquals, test.targets)
		})
	}
}

// TestParse_AnOracleUserTypeReachesTheOracleTypeOwner is the other half: a
// declaration scoped to Oracle is Oracle's domain or object type, which the
// Oracle type owner reads.
func TestParse_AnOracleUserTypeReachesTheOracleTypeOwner(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(builtintest.Annotations(), "fixture.go", userTypeSource(` dialects="oracle"`))

	c.Assert(err, qt.IsNil)
	objects := must.Must(db.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 2)
	c.Assert(objects[0].Ref, qt.DeepEquals, oratype.Ref(oratype.DomainKind, "", "email"))
	c.Assert(objects[0].Value, qt.DeepEquals, &oratype.DesiredDomain{BaseType: "TEXT", NotNull: true, StructName: "Email"})
	c.Assert(objects[0].Targets, qt.DeepEquals, []string{"oracle"})
	c.Assert(objects[1].Ref, qt.DeepEquals, oratype.Ref(oratype.ObjectTypeKind, "", "address"))
	c.Assert(objects[1].Value, qt.DeepEquals, &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "street", Type: "TEXT"}},
		StructName: "Address"})
	c.Assert(objects[1].Targets, qt.DeepEquals, []string{"oracle"})
}

// TestParse_AUserTypeScopedToBothModelsIsRefused pins that one declaration
// cannot name a PostgreSQL target beside Oracle: the two owners' objects are
// different objects, and the refusal says how to split the declaration.
func TestParse_AUserTypeScopedToBothModelsIsRefused(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(builtintest.Annotations(), "fixture.go", userTypeSource(` dialects="postgres,oracle"`))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `(?s).*ptah:schema:domain.*`)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
}
