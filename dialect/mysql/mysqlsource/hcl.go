package mysqlsource

import (
	"context"
	"fmt"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// HCL returns the owner's extension of the HCL frontend. It reads no block.
// It claims that an index or a primary key an HCL document declares has no
// block size, since the format has no spelling for one: applying the document
// removes the hint, as its export warns. Its encoder writes the owner's
// settings into the writer's own blocks, the spelling the community binary
// reads:
//   - a table's options as its `engine`, `charset` and `auto_increment`;
//   - a column's settings as its `charset` and its `on_update` SQL;
//   - a FULLTEXT index's parser as the index's `parser`.
//
// It reports the block size of an index or a key as a loss at the block.
func HCL() hclext.Extension {
	return hclext.Extension{Owner: mysqlschema.Owner,
		Kinds:    []schemaext.Kind{mysqlschema.TableKind, mysqlschema.ColumnSettingsKind, mysqlschema.IndexKind, mysqlschema.IndexBlockSizeKind},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) { return BlockSizeCoverage() },
		Encoder:  hclEncoder{}}
}

// hclEncoder writes the owner's settings into the HCL writer's blocks.
type hclEncoder struct{}

// EncodeHCL writes the settings the request's blocks hold. A value the
// owner's models cannot read writes nothing, and is refused where it is used;
// a column's settings that cannot be read are reported as a loss.
func (hclEncoder) EncodeHCL(_ context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	var result hclext.EncodeResult
	add := func(slot hclext.Slot, attributes []hclext.EncodedAttribute, losses []string) {
		if len(attributes) > 0 || len(losses) > 0 {
			result.Attributes = append(result.Attributes, hclext.SlotAttributes{Slot: slot, Attributes: attributes, Losses: losses})
		}
	}
	for _, table := range request.Tables {
		add(hclext.Slot{Kind: hclext.SlotTable, Table: table.Table}, tableAttributes(table.Facets), nil)
	}
	for _, column := range request.Columns {
		attributes, losses := columnAttributes(column.Facets)
		add(column.Slot, attributes, losses)
	}
	for _, index := range request.Indexes {
		var losses []string
		if size, _, _ := mysqlschema.IndexBlockSize(index.Facets); size != 0 {
			losses = append(losses, "the index KEY_BLOCK_SIZE hint cannot be represented in HCL schema output; applying this HCL removes the hint")
		}
		add(index.Slot, indexAttributes(index.Facets), losses)
	}
	for _, key := range request.Keys {
		if size, _, _ := mysqlschema.IndexBlockSize(key.Facets); size != 0 {
			add(key.Slot, nil, []string{"the primary key KEY_BLOCK_SIZE option cannot be represented in HCL schema output"})
		}
	}
	return result, nil
}

// tableAttributes are a table's options, declared or read.
func tableAttributes(facets schemaext.Facets) []hclext.EncodedAttribute {
	var options mysqlschema.DesiredTable
	if declared, found, err := schemaext.FacetAs[*mysqlschema.DesiredTable](facets, mysqlschema.TableKind); err == nil && found && declared != nil {
		options = *declared
	} else if observed, found, err := schemaext.FacetAs[*mysqlschema.ObservedTable](facets, mysqlschema.TableKind); err == nil && found && observed != nil {
		options = *observed.Desired()
	}
	var attributes []hclext.EncodedAttribute
	attributes = appendString(attributes, "engine", options.Engine)
	attributes = appendString(attributes, "charset", options.Charset)
	if options.AutoIncrement != "" {
		attributes = append(attributes, hclext.EncodedAttribute{Name: "auto_increment", Value: hclext.Value{Kind: hclext.Number, Text: options.AutoIncrement}})
	}
	return attributes
}

// columnAttributes are a column's settings, declared or read, and the loss
// of settings that cannot be read.
func columnAttributes(facets schemaext.Facets) ([]hclext.EncodedAttribute, []string) {
	settings, _, err := mysqlschema.Settings(facets)
	var losses []string
	if err != nil {
		losses = append(losses, fmt.Sprintf("column settings are not represented in HCL: %v", err))
	}
	var attributes []hclext.EncodedAttribute
	if settings.OnUpdate != "" {
		attributes = append(attributes, hclext.EncodedAttribute{Name: "on_update", Value: hclext.Value{Kind: hclext.SQL, Text: settings.OnUpdate}})
	}
	return appendString(attributes, "charset", settings.Charset), losses
}

// indexAttributes are an index's options, declared or read.
func indexAttributes(facets schemaext.Facets) []hclext.EncodedAttribute {
	var options mysqlschema.DesiredIndex
	if declared, found, err := schemaext.FacetAs[*mysqlschema.DesiredIndex](facets, mysqlschema.IndexKind); err == nil && found && declared != nil {
		options = *declared
	} else if observed, found, err := schemaext.FacetAs[*mysqlschema.ObservedIndex](facets, mysqlschema.IndexKind); err == nil && found && observed != nil {
		options = *observed.Desired()
	}
	return appendString(nil, "parser", options.Parser)
}

// appendString appends a string attribute, unless the value is empty.
func appendString(attributes []hclext.EncodedAttribute, name, value string) []hclext.EncodedAttribute {
	if value == "" {
		return attributes
	}
	return append(attributes, hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.String, Text: value}})
}
