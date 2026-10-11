package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
)

// ownedTypeFixture declares a domain, a composite type and a range type
// through their owner with every declared field set, so the census measures
// each one, except the server's spelling of a domain, which only a connected
// comparison attaches. A domain's literal default is written only where it
// declares no expression, and a range type clears only an attribute it does
// not state, so each of those has a declaration of its own.
func ownedTypeFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	object := func(kind schemaext.Kind, schema, name string, value schemaext.Value) schemaext.Object {
		return must.Must(pgtype.DesiredObject(pgtype.Ref(kind, schema, name), value))
	}
	db.FeatureObjects = must.Must(schemaext.NewObjects(
		object(pgtype.DomainKind, "app", "email", &pgtype.DesiredDomain{BaseType: "text", NotNull: true, DefaultExpr: "lower('X')",
			Check: "VALUE <> ''", Comment: "mail", StructName: "Email"}),
		object(pgtype.DomainKind, "app", "code", &pgtype.DesiredDomain{BaseType: "integer", Default: "7"}),
		object(pgtype.CompositeKind, "", "addr", &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "text"}},
			Comment: "address", StructName: "Address"}),
		object(pgtype.RangeKind, "", "span", &pgtype.DesiredRange{Subtype: "numeric", SubtypeOpClass: "numeric_ops", Collation: "C",
			Canonical: "span_canonical", SubtypeDiff: "span_diff", Comment: "spans", StructName: "Span"}),
		object(pgtype.RangeKind, "", "plain_span", &pgtype.DesiredRange{Subtype: "int4", Cleared: []string{pgtype.RangeCanonical}}),
	))
	db.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Desired))
	return db
}
