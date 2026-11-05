package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/spanner/spannerschema"
)

// tableRowDeletionFixture is a Spanner row deletion policy, bound to the
// Spanner target that renders it.
func tableRowDeletionFixture() schemamodel.Database {
	policy := &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "30 days"}}
	return oneTable("T", schemamodel.Table{
		Name: "t", Facets: must.Must(must.Must(schemaext.NewFacets(policy)).WithTargetScope(spannerschema.RowDeletionKind, platform.Spanner)),
	}, schemamodel.Field{StructName: "T", FieldName: "CreatedAt", Name: "created_at", Type: "TIMESTAMP", Nullable: true})
}
