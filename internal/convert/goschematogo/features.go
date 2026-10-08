package goschematogo

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
)

func changefeedTableRef(table schemamodel.Table) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(table.Schema, table.Name)
}

// Capture once before writing any files. A named object that has no matching
// table, an unknown kind, or a facet without an annotation must not disappear
// from a successful export.
func (ctx *renderContext) captureFeatureObjects() error {
	for _, facets := range ctx.db.FacetSlots() {
		if !facets.IsZero() {
			return fmt.Errorf("%w: Go annotations cannot represent feature facet %q", ptaherr.ErrUnsupportedFeature, facets.Kinds()[0])
		}
	}
	parents := make(map[objectidentity.Key]struct{}, len(ctx.db.Tables))
	for _, table := range ctx.db.Tables {
		parents[changefeedTableRef(table).Key()] = struct{}{}
	}
	objects, err := ctx.db.FeatureObjects.All()
	if err != nil {
		return err
	}
	ctx.changefeedsByTable = make(map[objectidentity.Key][]ydbschema.ChangefeedSpec)
	for _, object := range objects {
		feed, ok := object.Value.(*ydbschema.DesiredChangefeed)
		if !ok {
			return fmt.Errorf("%w: Go annotations cannot represent feature object %s with value %T", ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
		}
		if err := ydbschema.ValidateChangefeed(feed.Spec); err != nil {
			return err
		}
		if feed.Spec.Disabled {
			return fmt.Errorf("%w: Go annotations cannot preserve disabled changefeed %s", ptaherr.ErrUnsupportedFeature, object.Ref)
		}
		if feed.RetainedReplication != nil {
			return fmt.Errorf("%w: Go annotations cannot preserve the retained replication binding of changefeed %s", ptaherr.ErrUnsupportedFeature, object.Ref)
		}
		parent := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(object.Ref.Schema.Source, object.Ref.Parent.Source)
		if _, found := parents[parent.Key()]; !found {
			return fmt.Errorf("%w: feature object %s has no declared parent table", ptaherr.ErrInvalidSchemaDiff, object.Ref)
		}
		if object.Ref.Key() != ydbschema.ChangefeedRef(object.Ref.Schema.Source, object.Ref.Parent.Source, feed.Spec.Name).Key() {
			return fmt.Errorf("%w: changefeed name disagrees with its reference", ptaherr.ErrInvalidSchemaDiff)
		}
		ctx.changefeedsByTable[parent.Key()] = append(ctx.changefeedsByTable[parent.Key()], feed.Spec)
	}
	return nil
}
