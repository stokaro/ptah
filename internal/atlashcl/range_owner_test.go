package atlashcl_test

import (
	"context"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/builtintest"
)

// parseTypeHCL selects the shipped source owners for documents that declare
// PostgreSQL ranges. The neutral parser selects no owners on its own.
func parseTypeHCL(ctx context.Context, data []byte, name string) (*schemamodel.Database, error) {
	return atlashcl.ParseWithOptions(ctx, data, name, atlashcl.Options{HCL: builtintest.Runtime().HCL()})
}

func rangeObjects(database *schemamodel.Database) []schemaext.Object {
	return typeObjects(database, pgtype.RangeKind)
}

// typeObjects are the PostgreSQL type owner's objects of kind a document
// declares.
func typeObjects(database *schemamodel.Database, kind schemaext.Kind) []schemaext.Object {
	return must.Must(database.FeatureObjects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(kind) }).All())
}

func rangeValues(database *schemamodel.Database) []*pgtype.DesiredRange {
	var values []*pgtype.DesiredRange
	for _, object := range rangeObjects(database) {
		values = append(values, object.Value.(*pgtype.DesiredRange))
	}
	return values
}
