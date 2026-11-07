package schemadiff_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

type relationCaptureRuntime struct {
	*engine.Runtime
	relations []objectidentity.ID
}

func (r *relationCaptureRuntime) CompareFeatures(ctx context.Context, request schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	r.relations = slices.Clone(request.DeclaredRelations)
	return r.Runtime.CompareFeatures(ctx, request)
}

// The comparison names every relation the desired schema declares, each with
// its kind: a table with the identity its comparison parent has, and the views
// and materialized views. A relation only the database holds is not declared.
func TestFeatureComparison_CapturesDeclaredRelations(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	recording := &relationCaptureRuntime{Runtime: runtime}
	desired := &schemamodel.Database{
		Tables:            []schemamodel.Table{{StructName: "Order", Name: "orders"}, {StructName: "Audit", Schema: "audit", Name: "events"}},
		Views:             []schemamodel.View{{Name: "recent", Body: "SELECT 1"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "daily", Body: "SELECT 1"}},
	}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "observed_only"}}}

	_, err = schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", recording)

	c.Assert(err, qt.IsNil)
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	view, materialized := builder.Table("recent"), builder.Table("daily")
	view.Kind, materialized.Kind = objectidentity.KindView, objectidentity.KindMatView
	c.Assert(recording.relations, qt.DeepEquals, []objectidentity.ID{builder.TableParts("", "orders"), builder.TableParts("audit", "events"), view, materialized})
}
