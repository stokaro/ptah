package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/schemascope"
)

// TestDeclaredSchemaNames_CountsAnOwnersType pins that a type the PostgreSQL
// type owner declares names its schema as a common type does: it can be the
// document's only mention of the schema, and a read that left the schema out
// would report the type absent and plan it on every run.
func TestDeclaredSchemaNames_CountsAnOwnersType(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", Schema: "public"}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, "app", "email"), &pgtype.DesiredDomain{BaseType: "text"})),
			must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, "", "span"), &pgtype.DesiredRange{Subtype: "int4"})),
		)),
	}

	c.Assert(schemascope.DeclaredSchemaNames(desired), qt.DeepEquals, []string{"app", "public"})
}
