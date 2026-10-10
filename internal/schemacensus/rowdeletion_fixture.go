package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/ydb/ydbschema"
)

// tableRowDeletionFixture is a Spanner row deletion policy, bound to the
// Spanner target that renders it.
func tableRowDeletionFixture() schemamodel.Database {
	policy := &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "30 days"}}
	return oneTable("T", schemamodel.Table{
		Name: "t", Facets: must.Must(must.Must(schemaext.NewFacets(policy)).WithTargetScope(spannerschema.RowDeletionKind, platform.Spanner)),
	}, schemamodel.Field{StructName: "T", FieldName: "CreatedAt", Name: "created_at", Type: "TIMESTAMP", Nullable: true})
}

// tableYDBTTLFixture is a YDB TTL on a timestamp column, bound to the YDB
// target that renders it.
func tableYDBTTLFixture() schemamodel.Database {
	policy := &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P30D"}}
	return oneTable("T", schemamodel.Table{
		Name: "t", Facets: must.Must(must.Must(schemaext.NewFacets(policy)).WithTargetScope(ydbschema.TTLKind, platform.YDB)),
	}, schemamodel.Field{StructName: "T", FieldName: "CreatedAt", Name: "created_at", Type: "TIMESTAMP", Nullable: true})
}

// tableRowDeletionEpochFixture is a YDB TTL on an integer column, whose unit
// says what the column counts.
func tableRowDeletionEpochFixture() schemamodel.Database {
	policy := &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "expires", Interval: "PT1H", Unit: "SECONDS"}}
	return oneTable("T", schemamodel.Table{
		Name: "t", Facets: must.Must(must.Must(schemaext.NewFacets(policy)).WithTargetScope(ydbschema.TTLKind, platform.YDB)),
	}, schemamodel.Field{StructName: "T", FieldName: "Expires", Name: "expires", Type: "BIGINT UNSIGNED", Nullable: true})
}
