package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
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
// only PostgreSQL-family targets, is the PostgreSQL type owner's, and none of
// it stays on the common model.
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
			c.Assert(db.Domains, qt.HasLen, 0)
			c.Assert(db.CompositeTypes, qt.HasLen, 0)
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

// TestParse_AnOracleUserTypeStaysOnTheCommonModel is the other half: a
// declaration scoped to Oracle is Oracle's domain or composite type, which
// the common model holds and the Oracle renderer reads.
func TestParse_AnOracleUserTypeStaysOnTheCommonModel(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(builtintest.Annotations(), "fixture.go", userTypeSource(` dialects="oracle"`))

	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(db.FeatureObjects.All()), qt.HasLen, 0)
	c.Assert(db.Domains, qt.HasLen, 1)
	c.Assert(db.Domains[0].Name, qt.Equals, "email")
	c.Assert(db.Domains[0].NotNull, qt.IsTrue)
	c.Assert(db.Domains[0].Dialects, qt.DeepEquals, []string{"oracle"})
	c.Assert(db.CompositeTypes, qt.HasLen, 1)
	c.Assert(db.CompositeTypes[0].Name, qt.Equals, "address")
	c.Assert(db.CompositeTypes[0].Dialects, qt.DeepEquals, []string{"oracle"})
}

// TestParse_AUserTypeScopedToBothModelsIsRefused pins that one declaration
// cannot name a PostgreSQL target beside Oracle: the two are different
// objects, and the refusal says how to split the declaration.
func TestParse_AUserTypeScopedToBothModelsIsRefused(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(builtintest.Annotations(), "fixture.go", userTypeSource(` dialects="postgres,oracle"`))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `(?s).*ptah:schema:domain.*`)
	c.Assert(db.Domains, qt.HasLen, 0)
}
