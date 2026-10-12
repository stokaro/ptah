package ydbsource

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbfamily"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
	"ptah.run/internal/ydbtype"
)

// settingsKinds are the owner's models a declaration of the frontend's own
// has a spelling for: a table's column families, partitioning and column
// storage, and an index's partitioning and vector settings.
func settingsKinds(slot annotation.Slot) []schemaext.Kind {
	switch {
	case slot.Directive == directiveTable && !slot.PrimaryKey:
		return []schemaext.Kind{ydbschema.ColumnFamiliesKind, ydbschema.TablePartitioningKind, ydbschema.ColumnStoreKind}
	case slot.Directive == directiveIndex:
		return []schemaext.Kind{ydbschema.IndexPartitioningKind, ydbschema.VectorIndexKind}
	default:
		return nil
	}
}

// slotSettings writes the settings of the declarations the owner is handed:
// a table's column families as directives beside it, its column storage with
// the directive's own attributes and its partitioning after the platform
// properties; an index's partitioning with its own attributes, and its
// full-text or local options and vector settings after the platform
// properties. A setting on a declaration with no spelling for it, and an
// invalid value, are refused.
func slotSettings(slots []annotation.SlotFacets) ([]annotation.EncodedDirective, []annotation.SlotAttributes, error) {
	var directives []annotation.EncodedDirective
	var attributes []annotation.SlotAttributes
	for _, slot := range slots {
		accepted := settingsKinds(slot.Slot)
		for _, kind := range slot.Facets.Kinds() {
			if !slices.Contains(accepted, kind) {
				return nil, nil, fmt.Errorf("%w: Go annotations cannot represent feature facet %q", ptaherr.ErrUnsupportedFeature, kind)
			}
		}
		var written []annotation.SlotAttributes
		var err error
		if slot.Slot.Directive == directiveTable {
			var families []annotation.EncodedDirective
			if families, err = familyDirectives(slot); err != nil {
				return nil, nil, err
			}
			directives = append(directives, families...)
			written, err = tableSettings(slot)
		} else {
			written, err = indexSettings(slot)
		}
		if err != nil {
			return nil, nil, err
		}
		for _, entry := range written {
			if len(entry.Attributes) > 0 || len(entry.Options) > 0 {
				attributes = append(attributes, entry)
			}
		}
	}
	return directives, attributes, nil
}

// familyDirectives are a table's column families beside it: every family
// but a default one that holds what YDB gives a family stating nothing (see
// [ydbfamily.Stated]), so a table nobody gave families exports none.
func familyDirectives(slot annotation.SlotFacets) ([]annotation.EncodedDirective, error) {
	value, found, err := schemaext.FacetAs[*ydbschema.DesiredColumnFamilies](slot.Facets, ydbschema.ColumnFamiliesKind)
	if err != nil || !found {
		return nil, err
	}
	if err := ydbschema.ValidateDesiredColumnFamilies(value); err != nil {
		return nil, err
	}
	table := slot.Slot.Table
	var directives []annotation.EncodedDirective
	for _, family := range ydbfamily.Stated(value.Families) {
		var attrs encodedAttrs
		attrs.add(ydbfamily.AttributeName, family.Name)
		attrs.text(ydbfamily.AttributeData, family.Data)
		attrs.text(ydbfamily.AttributeCompression, family.Compression)
		attrs.text(ydbfamily.AttributeCacheMode, family.CacheMode)
		attrs.text(ydbfamily.AttributeFields, strings.Join(family.Columns, ","))
		directive := attrs.directive(directiveColumnFamily, annotation.AfterTable)
		directive.Table = &table
		directives = append(directives, directive)
	}
	return directives, nil
}

// tableSettings are a table's column storage, with the directive's own
// attributes, and its row partitioning, after the platform properties.
func tableSettings(slot annotation.SlotFacets) ([]annotation.SlotAttributes, error) {
	store, err := ydbschema.DeclaredColumnStore(slot.Facets)
	if err != nil {
		return nil, err
	}
	var own encodedAttrs
	if store != nil {
		if err := ydbschema.ValidateDesiredColumnStore(store); err != nil {
			return nil, err
		}
		own.add(ydbcolumn.AttributeStore, "column")
		own.text(ydbcolumn.AttributeHash, strings.Join(store.HashColumns, ","))
		own.count(ydbcolumn.AttributeShards, store.Partitions)
		if store.TTL != nil {
			// The policy holds only strings and a slice, so JSON encoding
			// cannot fail.
			encoded, _ := json.Marshal(store.TTL)
			own.add(ydbcolumn.AttributeTTL, string(encoded))
		}
	}
	partitioning, found, err := schemaext.FacetAs[*ydbschema.DesiredTablePartitioning](slot.Facets, ydbschema.TablePartitioningKind)
	if err != nil {
		return nil, err
	}
	var after encodedAttrs
	if found {
		if err := ydbschema.ValidateDesiredTablePartitioning(partitioning); err != nil {
			return nil, err
		}
		after.tablePartitioning(partitioning.TablePartitioning)
	}
	return []annotation.SlotAttributes{{Slot: slot.Slot, Attributes: own},
		{Slot: slot.Slot, Position: annotation.AfterProperties, Attributes: after}}, nil
}

// tablePartitioning writes the row table's partitioning attributes.
func (a *encodedAttrs) tablePartitioning(spec ydbschema.TablePartitioning) {
	if spec.IsZero() {
		return
	}
	a.partitioning(ydbschema.IndexPartitioning{BySize: spec.BySize, PartitionSizeMB: spec.PartitionSizeMB, ByLoad: spec.ByLoad,
		MinPartitions: spec.MinPartitions, MaxPartitions: spec.MaxPartitions, ReadReplicas: spec.ReadReplicas})
	if spec.KeyBloomFilter != nil {
		a.add(ydbpartition.AttributeKeyBloomFilter, switchValue(spec.KeyBloomFilter))
	}
	a.count(ydbpartition.AttributeUniformPartitions, spec.UniformPartitions)
	if len(spec.PartitionAtKeys) != 0 {
		a.add(ydbpartition.AttributePartitionAtKeys, ydbpartition.FormatSplitPoints(spec.PartitionAtKeys))
	}
}

// indexSettings are an index's partitioning, with the directive's own
// attributes, and its full-text or local options and vector settings after
// the platform properties. The options are those the index's type takes,
// checked as the renderer checks them, and claimed so the writer refuses no
// other.
func indexSettings(slot annotation.SlotFacets) ([]annotation.SlotAttributes, error) {
	partitioning, found, err := schemaext.FacetAs[*ydbschema.DesiredIndexPartitioning](slot.Facets, ydbschema.IndexPartitioningKind)
	if err != nil {
		return nil, err
	}
	var own encodedAttrs
	if found {
		if err := ydbschema.ValidateDesiredIndexPartitioning(partitioning); err != nil {
			return nil, err
		}
		if !partitioning.IsZero() {
			own.partitioning(partitioning.IndexPartitioning)
		}
	}
	after, claimed, err := indexOptionsOf(slot)
	if err != nil {
		return nil, err
	}
	vector, found, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](slot.Facets, ydbschema.VectorIndexKind)
	if err != nil {
		return nil, err
	}
	if found {
		if err := ydbschema.ValidateDesiredVectorIndex(vector); err != nil {
			return nil, err
		}
		spec := vector.Settings()
		after.text(ydbindex.AttributeDistance, spec.Distance)
		after.text(ydbindex.AttributeSimilarity, spec.Similarity)
		after.text(ydbindex.AttributeVectorType, spec.VectorType)
		after.count(ydbindex.AttributeVectorDimension, spec.Dimension)
		after.count(ydbindex.AttributeLevels, spec.Levels)
		after.count(ydbindex.AttributeClusters, spec.Clusters)
	}
	return []annotation.SlotAttributes{{Slot: slot.Slot, Attributes: own},
		{Slot: slot.Slot, Position: annotation.AfterProperties, Attributes: after, Options: claimed}}, nil
}

// indexOptionsOf writes the options of a full-text or local index, the ones
// its type takes in the order YDB lists them, and claims every option the
// index holds. An index of another type claims none.
func indexOptionsOf(slot annotation.SlotFacets) (encodedAttrs, []string, error) {
	kind, err := ydbindex.KindOf(slot.Reads["type"])
	if err != nil || !kind.IsFullText() && !kind.IsLocal() {
		return nil, nil, nil
	}
	names := ydbindex.FullTextAttributes()
	if kind.IsLocal() {
		if _, err := ydbindex.ResolveLocal(kind, slot.Options); err != nil {
			return nil, nil, fmt.Errorf("index %q: %w", slot.Slot.Name, err)
		}
		names = ydbindex.LocalAttributes()
	} else if _, err := ydbindex.ResolveFullText(slot.Options); err != nil {
		return nil, nil, fmt.Errorf("index %q: %w", slot.Slot.Name, err)
	}
	var attrs encodedAttrs
	var claimed []string
	for _, name := range names {
		if value, present := slot.Options[name]; present {
			attrs.add(name, value)
			claimed = append(claimed, name)
		}
	}
	return attrs, claimed, nil
}

// partitioning writes the partitioning settings an index and a row table
// share.
func (a *encodedAttrs) partitioning(spec ydbschema.IndexPartitioning) {
	if spec.BySize != nil {
		a.add(ydbpartition.AttributeBySize, switchValue(spec.BySize))
	}
	a.count(ydbpartition.AttributePartitionSizeMB, spec.PartitionSizeMB)
	if spec.ByLoad != nil {
		a.add(ydbpartition.AttributeByLoad, switchValue(spec.ByLoad))
	}
	a.count(ydbpartition.AttributeMinPartitions, spec.MinPartitions)
	a.count(ydbpartition.AttributeMaxPartitions, spec.MaxPartitions)
	a.text(ydbpartition.AttributeReadReplicas, spec.ReadReplicas)
}

// switchValue writes a setting that is switched on or off.
func switchValue(on *bool) string {
	if on != nil && *on {
		return "ENABLED"
	}
	return "DISABLED"
}

// fieldTypes are the Go types of a YDB export's fields, for the YDB type
// names the YDB reader reports, which internal/ydbtype reads. A nullable key
// column is refused: YDB accepts one, and takes one row whose key is NULL,
// but no Ptah declaration can ask for it, since the YDB renderer writes NOT
// NULL on every key column. Written as an ordinary key field, the column
// would be rebuilt NOT NULL wherever the models are applied.
func fieldTypes(request annotation.EncodeRequest) ([]annotation.FieldType, error) {
	if request.Target != platform.YDB {
		return nil, nil
	}
	var types []annotation.FieldType
	for _, field := range request.Fields {
		if field.Nullable && field.Key {
			return nil, fmt.Errorf("column %q of table %q is a nullable key column, which no Ptah declaration can "+
				"represent: Ptah writes NOT NULL on every YDB key column", field.Slot.Name, field.Slot.Table.QualifiedName())
		}
		if goType, ok := ydbtype.GoTypeOf(field.Type); ok {
			types = append(types, annotation.FieldType{Slot: field.Slot, Name: goType.Name, Import: goType.Import})
		}
	}
	return types, nil
}
