package goschematogo

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// Capture once before writing any files. A named object that has no matching
// table, an unknown kind, or a facet without an annotation must not disappear
// from a successful export.
func (ctx *renderContext) captureFeatureObjects(requestContext context.Context) error {
	ctx.owned = ctx.collectOwnerSlots()
	if err := ctx.refuseUnwrittenFacets(); err != nil {
		return err
	}
	if err := ctx.encodeOwners(requestContext); err != nil {
		return err
	}
	objects, err := ctx.db.FeatureObjects.All()
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := ctx.captureFeatureObject(object); err != nil {
			return err
		}
	}
	ctx.sortExtensions()
	return nil
}

// captureFeatureObject records an object the export writes itself, and
// refuses one no annotation writes. An object of a model a selected owner
// encodes is the owner's to write or refuse.
func (ctx *renderContext) captureFeatureObject(object schemaext.Object) error {
	if ctx.opts.Annotations.Encodes(schemaext.Kind(object.Ref.Kind)) {
		return nil
	}
	if extension, ok := object.Value.(*pgextension.DesiredExtension); ok {
		return ctx.captureExtension(object, extension)
	}
	return fmt.Errorf("%w: Go annotations cannot represent feature object %s with value %T", ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
}

// refuseUnwrittenFacets refuses a facet no annotation writes, so it cannot
// disappear from a successful export. A facet of a model a selected owner
// encodes, on a declaration the owner is handed, is the owner's to write or
// refuse. Materialized view settings are checked before owner encoding.
func (ctx *renderContext) refuseUnwrittenFacets() error {
	views := make(map[*schemaext.Facets]bool, len(ctx.db.MaterializedViews))
	for i := range ctx.db.MaterializedViews {
		views[&ctx.db.MaterializedViews[i].Facets] = true
	}
	for _, facets := range ctx.db.FacetSlots() {
		if views[facets] {
			continue
		}
		for _, kind := range facets.DeclaredKinds() {
			held := slices.Contains(facets.Kinds(), kind)
			if !held || !ctx.owned.handed[facets] || !ctx.opts.Annotations.Encodes(kind) {
				return fmt.Errorf("%w: Go annotations cannot represent feature facet %q", ptaherr.ErrUnsupportedFeature, kind)
			}
		}
	}
	return nil
}
