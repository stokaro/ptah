package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// tableRowTTLFixture sets every managed row-level TTL parameter, bound to the
// CockroachDB target that renders them.
func tableRowTTLFixture() schemamodel.Database {
	policy := &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{
		ExpireAfter: "3 months", ExpirationExpression: "created_at", JobCron: "@daily",
		SelectBatchSize: new(int64(500)), DeleteBatchSize: new(int64(100)), DeleteRateLimit: new(int64(100)),
		SelectRateLimit: new(int64(100)), RowStatsPollInterval: "1m", Pause: true,
		LabelMetrics: true, DisableChangefeedReplication: true,
	}}
	return oneTable("T", schemamodel.Table{
		Name: "t", Facets: must.Must(must.Must(schemaext.NewFacets(policy)).WithTargetScope(crdbschema.RowTTLKind, "cockroachdb")),
	}, schemamodel.Field{StructName: "T", FieldName: "CreatedAt", Name: "created_at", Type: "TIMESTAMP", Nullable: true})
}
