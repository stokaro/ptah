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
	if err := ctx.captureTablePartitioning(); err != nil {
		return err
	}
	if err := ctx.captureColumnStores(); err != nil {
		return err
	}
	if err := ctx.captureIndexPartitioning(); err != nil {
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
// disappear from a successful export: a facet of a model a selected owner
// encodes, which the owner writes or refuses, a YDB table setting this
// package writes as attributes of the table's directive, and a YDB vector
// index's settings, which the index annotation writes, are the ones
// written. Render has refused any view setting no owner writes already.
func (ctx *renderContext) refuseUnwrittenFacets() error {
	writers := ctx.facetWriters()
	for _, facets := range ctx.db.FacetSlots() {
		writer, written := writers[facets]
		if written && writer == nil {
			continue
		}
		for _, kind := range facets.DeclaredKinds() {
			held := slices.Contains(facets.Kinds(), kind)
			if held && ctx.owned.handed[facets] && ctx.opts.Annotations.Encodes(kind) {
				// Its owner writes it, or refuses it, in encodeOwners.
				continue
			}
			if !held || writer == nil || !writer.accepts(kind) {
				return fmt.Errorf("%w: Go annotations cannot represent feature facet %q", ptaherr.ErrUnsupportedFeature, kind)
			}
			if writer.validate == nil {
				continue
			}
			if err := writer.validate(*facets); err != nil {
				return err
			}
		}
	}
	return nil
}

// facetWriter is what writes the facets of one slot: the kinds it accepts,
// and the check of the values it writes, nil for none.
type facetWriter struct {
	accepts  func(schemaext.Kind) bool
	validate func(schemaext.Facets) error
}

// facetWriters addresses the writer of each slot an annotation writes. A
// materialized view's slot maps to nil: render has refused any setting of a
// view the export does not write.
func (ctx *renderContext) facetWriters() map[*schemaext.Facets]*facetWriter {
	writers := make(map[*schemaext.Facets]*facetWriter)
	table := &facetWriter{accepts: isAnnotatedFacet}
	for i := range ctx.db.Tables {
		writers[&ctx.db.Tables[i].Facets] = table
	}
	index := &facetWriter{accepts: isAnnotatedIndexFacet, validate: validateVectorDeclaration}
	for i := range ctx.db.Indexes {
		writers[&ctx.db.Indexes[i].Facets] = index
	}
	for i := range ctx.db.MaterializedViews {
		writers[&ctx.db.MaterializedViews[i].Facets] = nil
	}
	return writers
}
