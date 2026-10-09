package goschematogo

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/ydbsource"
)

func changefeedTableRef(table schemamodel.Table) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(table.Schema, table.Name)
}

// Capture once before writing any files. A named object that has no matching
// table, an unknown kind, or a facet without an annotation must not disappear
// from a successful export.
func (ctx *renderContext) captureFeatureObjects() error {
	limits, err := ydbsource.ExportLimits(ctx.db.FeatureCoverage)
	if err != nil {
		return err
	}
	for _, limit := range limits {
		ctx.featureLimitAnnotations = append(ctx.featureLimitAnnotations,
			annotation("ptah:schema:notdescribed", attr{name: "kind", value: string(limit.Kind), set: true},
				attr{name: "name", value: limit.Name, set: limit.Name != ""}))
	}
	for _, facets := range ctx.db.FacetSlots() {
		if !facets.IsZero() {
			return fmt.Errorf("%w: Go annotations cannot represent feature facet %q", ptaherr.ErrUnsupportedFeature, facets.DeclaredKinds()[0])
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
		if err := ctx.captureFeatureObject(object, parents); err != nil {
			return err
		}
	}
	return nil
}

func (ctx *renderContext) captureFeatureObject(object schemaext.Object, parents map[objectidentity.Key]struct{}) error {
	if pool, ok := object.Value.(*ydbworkload.DesiredPool); ok {
		if err := ydbworkload.ValidatePoolRef(object.Ref, pool.Spec); err != nil {
			return err
		}
		ctx.workloadAnnotations = append(ctx.workloadAnnotations, resourcePoolAnnotation(object.Ref.Name.Source, pool.Spec))
		return nil
	}
	if classifier, ok := object.Value.(*ydbworkload.DesiredClassifier); ok {
		if err := ydbworkload.ValidateIdentity(object.Ref, ydbworkload.ClassifierKind); err != nil {
			return err
		}
		if err := ydbworkload.ValidateClassifier(classifier.Spec); err != nil {
			return err
		}
		ctx.workloadAnnotations = append(ctx.workloadAnnotations, resourcePoolClassifierAnnotation(object.Ref.Name.Source, classifier.Spec))
		return nil
	}
	if query, ok := object.Value.(*ydbstreaming.Desired); ok {
		if err := ydbstreaming.ValidateIdentity(object.Ref); err != nil {
			return err
		}
		if err := ydbstreaming.Validate(query.Spec); err != nil {
			return err
		}
		ctx.streamingAnnotations = append(ctx.streamingAnnotations, streamingQueryAnnotation(object.Ref.Schema.Source, object.Ref.Name.Source, query))
		return nil
	}
	if secret, ok := object.Value.(*ydbsecret.Desired); ok {
		annotation, err := secretAnnotation(object.Ref, secret)
		if err != nil {
			return err
		}
		ctx.secretAnnotations = append(ctx.secretAnnotations, annotation)
		return nil
	}
	if node, ok := object.Value.(*ydbcoordination.Desired); ok {
		if err := ydbcoordination.ValidateRef(object.Ref); err != nil {
			return err
		}
		if err := ydbcoordination.Validate(node.Spec); err != nil {
			return err
		}
		ctx.coordinationAnnotations = append(ctx.coordinationAnnotations, coordinationNodeAnnotation(object.Ref.Schema.Source, object.Ref.Name.Source, node.Spec))
		return nil
	}
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
	return nil
}
