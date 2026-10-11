package ydbsource

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbfamily"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// settingAttributes are the YDB settings of the writer's own blocks: a
// vector index's settings and an index's partitioning as attributes of the
// index block, the spelling the reader reads them from, and the table
// settings HCL has no spelling for as losses at their table.
func settingAttributes(request hclext.EncodeRequest) []hclext.SlotAttributes {
	var slots []hclext.SlotAttributes
	for _, table := range request.Tables {
		if losses := tableLosses(table.Facets); len(losses) > 0 {
			slots = append(slots, hclext.SlotAttributes{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table.Table}, Losses: losses})
		}
	}
	for _, index := range request.Indexes {
		attributes := slices.Concat(vectorAttributes(index.Facets), indexPartitioningAttributes(index.Facets))
		if len(attributes) > 0 {
			slots = append(slots, hclext.SlotAttributes{Slot: index.Slot, Attributes: attributes})
		}
	}
	return slots
}

// tableLosses are the table settings a document leaves out, because HCL has
// no block for them:
//   - the column families, which reading the document back does not move a
//     column out of either, since the loader records that HCL cannot express
//     a family; a table holding only a default family that is
//     [ydbfamily.Plain] loses nothing;
//   - column storage, since reading the document back describes a row table;
//   - how the table splits into partitions, its read replicas, its key bloom
//     filter and the partitions it starts with, which reading the document
//     back does not reset, since a setting a declaration leaves out keeps
//     what the table holds.
//
// Reporting them makes the loss [ptah.run/internal/goannotationexport.ErrLossyCleanup]
// for a cleanup that would otherwise delete the only place they are declared.
func tableLosses(facets schemaext.Facets) []string {
	var losses []string
	if families := ydbfamily.Stated(heldFamilies(facets)); len(families) > 0 {
		names := make([]string, len(families))
		for i, family := range families {
			names[i] = family.Name
		}
		message := fmt.Sprintf("column families %s are not represented in HCL", strings.Join(names, ", "))
		if len(names) == 1 {
			message = fmt.Sprintf("column family %s is not represented in HCL", names[0])
		}
		losses = append(losses, message)
	}
	if slices.Contains(facets.Kinds(), ydbschema.ColumnStoreKind) {
		losses = append(losses, "YDB column storage, hash partitioning and tiered TTL are not represented in HCL")
	}
	if spec := heldTablePartitioning(facets); spec != nil && !spec.IsZero() {
		settings := ydbpartition.CreateClause(spec)
		switch {
		case spec.UniformPartitions != 0:
			settings = append(settings, fmt.Sprintf("UNIFORM_PARTITIONS = %d", spec.UniformPartitions))
		case len(spec.PartitionAtKeys) != 0:
			settings = append(settings, "PARTITION_AT_KEYS = ("+ydbpartition.FormatSplitPoints(spec.PartitionAtKeys)+")")
		}
		losses = append(losses, fmt.Sprintf("YDB table settings (%s) are not represented in HCL", strings.Join(settings, ", ")))
	}
	return losses
}

// vectorAttributes are a vector index's settings, declared or read, written
// as the index block's attributes. A count of zero is left out.
func vectorAttributes(facets schemaext.Facets) []hclext.EncodedAttribute {
	var settings ydbschema.VectorSettings
	if declared, found, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](facets, ydbschema.VectorIndexKind); err == nil && found {
		settings = declared.Settings()
	} else if observed, found, err := schemaext.FacetAs[*ydbschema.ObservedVectorIndex](facets, ydbschema.VectorIndexKind); err == nil && found {
		settings = observed.Settings()
	}
	var attributes []hclext.EncodedAttribute
	attributes = appendText(attributes, ydbindex.AttributeDistance, settings.Distance)
	attributes = appendText(attributes, ydbindex.AttributeSimilarity, settings.Similarity)
	attributes = appendText(attributes, ydbindex.AttributeVectorType, settings.VectorType)
	attributes = appendCount(attributes, ydbindex.AttributeVectorDimension, settings.Dimension)
	attributes = appendCount(attributes, ydbindex.AttributeLevels, settings.Levels)
	return appendCount(attributes, ydbindex.AttributeClusters, settings.Clusters)
}

// indexPartitioningAttributes are an index's partitioning and read replicas,
// declared or read, written as the index block's attributes: a switch as
// ENABLED or DISABLED, and a count of zero left out.
func indexPartitioningAttributes(facets schemaext.Facets) []hclext.EncodedAttribute {
	spec := heldIndexPartitioning(facets)
	if spec == nil || spec.IsZero() {
		return nil
	}
	var attributes []hclext.EncodedAttribute
	for _, setting := range []struct {
		name  string
		value *bool
	}{{ydbpartition.AttributeBySize, spec.BySize}, {ydbpartition.AttributeByLoad, spec.ByLoad}} {
		switch {
		case setting.value == nil:
		case *setting.value:
			attributes = appendText(attributes, setting.name, "ENABLED")
		default:
			attributes = appendText(attributes, setting.name, "DISABLED")
		}
	}
	attributes = appendCount(attributes, ydbpartition.AttributePartitionSizeMB, spec.PartitionSizeMB)
	attributes = appendCount(attributes, ydbpartition.AttributeMinPartitions, spec.MinPartitions)
	attributes = appendCount(attributes, ydbpartition.AttributeMaxPartitions, spec.MaxPartitions)
	return appendText(attributes, ydbpartition.AttributeReadReplicas, spec.ReadReplicas)
}

// appendText appends a string attribute, unless the value is empty.
func appendText(attributes []hclext.EncodedAttribute, name, value string) []hclext.EncodedAttribute {
	if value == "" {
		return attributes
	}
	return append(attributes, hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.String, Text: value}})
}

// appendCount appends a number attribute, unless the count is zero.
func appendCount(attributes []hclext.EncodedAttribute, name string, count uint64) []hclext.EncodedAttribute {
	if count == 0 {
		return attributes
	}
	return append(attributes, hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.Number, Text: strconv.FormatUint(count, 10)}})
}

// heldFamilies are the column families a table's facet states, declared or
// read; an invalid value states none here and is refused where it is used.
func heldFamilies(facets schemaext.Facets) []ydbschema.ColumnFamily {
	if declared, found, err := schemaext.FacetAs[*ydbschema.DesiredColumnFamilies](facets, ydbschema.ColumnFamiliesKind); err == nil && found {
		return declared.Families
	}
	if observed, found, err := schemaext.FacetAs[*ydbschema.ObservedColumnFamilies](facets, ydbschema.ColumnFamiliesKind); err == nil && found {
		return observed.Families
	}
	return nil
}

// heldTablePartitioning is the settings a table's facet states, declared or
// read; an invalid value states none here and is refused where it is used.
func heldTablePartitioning(facets schemaext.Facets) *ydbschema.TablePartitioning {
	if declared, found, err := schemaext.FacetAs[*ydbschema.DesiredTablePartitioning](facets, ydbschema.TablePartitioningKind); err == nil && found {
		return &declared.TablePartitioning
	}
	if observed, found, err := schemaext.FacetAs[*ydbschema.ObservedTablePartitioning](facets, ydbschema.TablePartitioningKind); err == nil && found {
		return &observed.TablePartitioning
	}
	return nil
}

// heldIndexPartitioning is the settings an index's facet states, declared or
// read; an invalid value states none here and is refused where it is used.
func heldIndexPartitioning(facets schemaext.Facets) *ydbschema.IndexPartitioning {
	if declared, found, err := schemaext.FacetAs[*ydbschema.DesiredIndexPartitioning](facets, ydbschema.IndexPartitioningKind); err == nil && found {
		return &declared.IndexPartitioning
	}
	if observed, found, err := schemaext.FacetAs[*ydbschema.ObservedIndexPartitioning](facets, ydbschema.IndexPartitioningKind); err == nil && found {
		return &observed.IndexPartitioning
	}
	return nil
}
