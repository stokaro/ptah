package goschematogo

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/ydbsource"
)

func changefeedTableRef(table schemamodel.Table) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(table.Schema, table.Name)
}

// Capture once before writing any files. A named object that has no matching
// table, an unknown kind, or a facet without an annotation must not disappear
// from a successful export.
func (ctx *renderContext) captureFeatureObjects(requestContext context.Context) error {
	limits, err := ydbsource.ExportLimits(ctx.db.FeatureCoverage)
	if err != nil {
		return err
	}
	for _, limit := range limits {
		// A limit no selected owner reads would be refused on the way back,
		// and says nothing a parse with this selection could know: without
		// the owner, its models are unknown rather than absent.
		if _, read := ctx.opts.Annotations.LimitOwner(string(limit.Kind)); !read {
			continue
		}
		ctx.featureLimitAnnotations = append(ctx.featureLimitAnnotations,
			annotation("ptah:schema:notdescribed", attr{name: "kind", value: string(limit.Kind), set: true},
				attr{name: "name", value: limit.Name, set: limit.Name != ""}))
	}
	ctx.owned = ctx.collectOwnerSlots()
	if err := ctx.refuseUnwrittenFacets(); err != nil {
		return err
	}
	if err := ctx.captureColumnFamilies(); err != nil {
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
	parents := make(map[objectidentity.Key]struct{}, len(ctx.db.Tables))
	for _, table := range ctx.db.Tables {
		parents[changefeedTableRef(table).Key()] = struct{}{}
	}
	objects, err := ctx.db.FeatureObjects.All()
	if err != nil {
		return err
	}
	ctx.changefeedsByTable = make(map[objectidentity.Key][]ydbschema.ChangefeedSpec)
	ctx.captureCommonExtensions()
	for _, object := range objects {
		if err := ctx.captureFeatureObject(object, parents); err != nil {
			return err
		}
	}
	ctx.sortExtensions()
	return nil
}

func (ctx *renderContext) captureFeatureObject(object schemaext.Object, parents map[objectidentity.Key]struct{}) error {
	if ctx.opts.Annotations.Encodes(schemaext.Kind(object.Ref.Kind)) {
		// Its owner writes it, or refuses it, in encodeOwners.
		return nil
	}
	if handled, err := ctx.capturePathObject(object); handled {
		return err
	}
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
	if extension, ok := object.Value.(*pgextension.DesiredExtension); ok {
		return ctx.captureExtension(object, extension)
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
	return ctx.captureChangefeed(object, feed, parents)
}

// captureChangefeed records a changefeed under the declared table it belongs
// to, refusing what a Go annotation cannot preserve.
func (ctx *renderContext) captureChangefeed(object schemaext.Object, feed *ydbschema.DesiredChangefeed, parents map[objectidentity.Key]struct{}) error {
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

// capturePathObject writes a declared secret, a declared topic and its
// consumers, a declared external object, or a declared async replication with
// its items or transfer as their annotations, and reports whether object is
// one of them.
func (ctx *renderContext) capturePathObject(object schemaext.Object) (bool, error) {
	switch value := object.Value.(type) {
	case *ydbsecret.Desired:
		annotation, err := secretAnnotation(object.Ref, value)
		if err != nil {
			return true, err
		}
		ctx.secretAnnotations = append(ctx.secretAnnotations, annotation)
		return true, nil
	case *ydbtopic.Desired:
		if err := ydbtopic.ValidateIdentity(object.Ref); err != nil {
			return true, err
		}
		if err := ydbtopic.Validate(value.Spec); err != nil {
			return true, err
		}
		ctx.topicAnnotations = append(ctx.topicAnnotations, topicAnnotations(object.Ref.Schema.Source, object.Ref.Name.Source, value.Spec)...)
		return true, nil
	case *ydbreplication.DesiredReplication:
		if err := ydbreplication.ValidateIdentity(object.Ref); err != nil {
			return true, err
		}
		if err := value.Validate(); err != nil {
			return true, err
		}
		ctx.replicationAnnotations = append(ctx.replicationAnnotations,
			replicationAnnotations(object.Ref.Schema.Source, object.Ref.Name.Source, value.Spec)...)
		return true, nil
	case *ydbreplication.DesiredTransfer:
		if err := ydbreplication.ValidateIdentity(object.Ref); err != nil {
			return true, err
		}
		if err := value.Validate(); err != nil {
			return true, err
		}
		ctx.transferAnnotations = append(ctx.transferAnnotations,
			transferAnnotation(object.Ref.Schema.Source, object.Ref.Name.Source, value.Spec))
		return true, nil
	case *ydbexternal.DesiredSource:
		if err := ydbexternal.ValidateIdentity(object.Ref); err != nil {
			return true, err
		}
		if err := value.Validate(); err != nil {
			return true, err
		}
		ctx.externalAnnotations[0] = append(ctx.externalAnnotations[0],
			externalDataSourceAnnotation(object.Ref.Schema.Source, object.Ref.Name.Source, value.Spec))
		return true, nil
	case *ydbexternal.DesiredTable:
		if err := ydbexternal.ValidateIdentity(object.Ref); err != nil {
			return true, err
		}
		if err := value.Validate(); err != nil {
			return true, err
		}
		ctx.externalAnnotations[1] = append(ctx.externalAnnotations[1],
			externalTableAnnotation(object.Ref.Schema.Source, object.Ref.Name.Source, value.Spec))
		return true, nil
	default:
		return false, nil
	}
}

// refuseUnwrittenFacets refuses a facet no annotation writes, so it cannot
// disappear from a successful export: a facet of a model a selected owner
// encodes, which the owner writes or refuses, a YDB table facet with an
// annotation of its own, and a YDB vector index's settings, which the index
// annotation writes, are the ones written. Render has refused any view
// setting no owner writes already.
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
