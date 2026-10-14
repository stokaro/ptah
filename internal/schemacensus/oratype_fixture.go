package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/oratype"
)

// ownedOracleTypeFixture declares two domains and an object type through
// their Oracle owner with every declared field set, so the census measures
// each one on the Oracle cells. A domain's literal default is written only
// where it declares no expression, so it has a declaration of its own.
func ownedOracleTypeFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	object := func(kind schemaext.Kind, name string, value schemaext.Value) schemaext.Object {
		return must.Must(oratype.DesiredObject(oratype.Ref(kind, "", name), value))
	}
	db.FeatureObjects = must.Must(schemaext.NewObjects(
		object(oratype.DomainKind, "email_d", &oratype.DesiredDomain{BaseType: "VARCHAR2(320)", NotNull: true, DefaultExpr: "LOWER('X')",
			Check: "VALUE <> 'x'", Comment: "mail", StructName: "Email"}),
		object(oratype.DomainKind, "code_d", &oratype.DesiredDomain{BaseType: "NUMBER(5)", Default: "7"}),
		object(oratype.ObjectTypeKind, "addr_t", &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "street", Type: "VARCHAR2(80)"}},
			Comment: "address", StructName: "Address"}),
	))
	db.FeatureCoverage = must.Must(oratype.CompleteCoverage(schemaext.Desired))
	return db
}
