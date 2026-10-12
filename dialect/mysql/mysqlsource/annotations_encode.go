package mysqlsource

import (
	"context"
	"fmt"
	"strconv"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// The frontend's directives whose declarations carry the owner's settings.
const fieldDirective = "ptah:schema:field"

// annotationEncoder writes the MySQL owner's settings of one schema as
// attributes of the frontend's own directives.
type annotationEncoder struct{}

// EncodeAnnotations writes, for each declaration it is handed:
//
//   - an index's and a PRIMARY KEY constraint's block-size hint as
//     key_block_size, and a primary key's, which the table directive
//     declares, as primary_key_block_size;
//   - an index's FULLTEXT parser as the platform property parser, and a
//     column's character set and ON UPDATE expression as the platform
//     properties charset and on_update, once for each target the setting is
//     bound to, and for every MySQL-family target when it is bound to none.
//     The parser follows the index's other platform properties.
//
// A hint of zero is the engine's default, and no attribute states it. A
// setting on a declaration that has no spelling for it, such as a hint on a
// UNIQUE constraint, and a value that is not a valid declaration, are refused
// rather than left out.
func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	var result annotation.EncodeResult
	for _, slot := range request.Slots {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		written, err := slotAttributes(slot)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		for _, attributes := range written {
			if len(attributes.Attributes) > 0 {
				result.Attributes = append(result.Attributes, attributes)
			}
		}
	}
	return result, nil
}

// slotAttributes writes the settings slot holds, refusing one the
// declaration has no spelling for.
func slotAttributes(slot annotation.SlotFacets) ([]annotation.SlotAttributes, error) {
	accepted := acceptedKinds(slot.Slot)
	for _, kind := range slot.Facets.Kinds() {
		if !accepted[kind] {
			return nil, fmt.Errorf("%w: Go annotations cannot represent feature facet %q", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	switch slot.Slot.Directive {
	case fieldDirective:
		properties, err := columnProperties(slot.Facets)
		return []annotation.SlotAttributes{{Slot: slot.Slot, Attributes: properties}}, err
	case indexDirective:
		// An index holds the declarations a source states, so an
		// observation that was not converted is refused here.
		if _, _, err := schemaext.FacetAs[*mysqlschema.DesiredIndexBlockSize](slot.Facets, mysqlschema.IndexBlockSizeKind); err != nil {
			return nil, err
		}
		size, err := blockSizeAttributes(BlockSizeAttribute, slot.Facets)
		if err != nil {
			return nil, err
		}
		parser, err := parserProperties(slot.Facets)
		return []annotation.SlotAttributes{{Slot: slot.Slot, Attributes: size},
			{Slot: slot.Slot, Position: annotation.AfterProperties, Attributes: parser}}, err
	case tableDirective:
		// Only the table's primary key holds a hint here: acceptedKinds
		// refused one of the table itself.
		size, err := blockSizeAttributes(PrimaryKeyBlockSizeAttribute, slot.Facets)
		return []annotation.SlotAttributes{{Slot: slot.Slot, Attributes: size}}, err
	default:
		// A PRIMARY KEY constraint: acceptedKinds refused a hint on any
		// other constraint.
		size, err := blockSizeAttributes(BlockSizeAttribute, slot.Facets)
		return []annotation.SlotAttributes{{Slot: slot.Slot, Attributes: size}}, err
	}
}

// acceptedKinds are the owner's models a declaration has a spelling for. A
// constraint other than a PRIMARY KEY has none: MySQL keeps a UNIQUE key's
// hint on the unique index it is, so a unique index declares it.
func acceptedKinds(slot annotation.Slot) map[schemaext.Kind]bool {
	switch {
	case slot.Directive == fieldDirective:
		return map[schemaext.Kind]bool{mysqlschema.ColumnSettingsKind: true}
	case slot.Directive == indexDirective:
		return map[schemaext.Kind]bool{mysqlschema.IndexBlockSizeKind: true, mysqlschema.IndexKind: true}
	case slot.PrimaryKey && (slot.Directive == constraintDirective || slot.Directive == tableDirective):
		return map[schemaext.Kind]bool{mysqlschema.IndexBlockSizeKind: true}
	default:
		return nil
	}
}

// blockSizeAttributes writes the block-size hint facets hold as attribute
// name, or nothing for a hint of zero, the engine's default.
func blockSizeAttributes(name string, facets schemaext.Facets) ([]annotation.EncodedAttribute, error) {
	size, _, err := mysqlschema.IndexBlockSize(facets)
	if err != nil || size == 0 {
		return nil, err
	}
	return []annotation.EncodedAttribute{{Name: name, Value: strconv.FormatUint(size, 10)}}, nil
}

// columnProperties writes a column's settings as the platform properties the
// owner decodes, once for each target they are bound to.
func columnProperties(facets schemaext.Facets) ([]annotation.EncodedAttribute, error) {
	settings, found, err := mysqlschema.Settings(facets)
	if err != nil || !found {
		return nil, err
	}
	var attributes []annotation.EncodedAttribute
	for _, target := range targetsOf(facets, mysqlschema.ColumnSettingsKind) {
		prefix := "platform." + target + "."
		if settings.Charset != "" {
			attributes = append(attributes, annotation.EncodedAttribute{Name: prefix + CharsetProperty, Value: settings.Charset})
		}
		if settings.OnUpdate != "" {
			attributes = append(attributes, annotation.EncodedAttribute{Name: prefix + OnUpdateProperty, Value: settings.OnUpdate})
		}
	}
	return attributes, nil
}

// parserProperties writes an index's FULLTEXT parser as the platform
// property the owner decodes, once for each target it is bound to. A SQL
// source binds it to both MySQL and MariaDB, which one target's property
// encoding cannot write.
func parserProperties(facets schemaext.Facets) ([]annotation.EncodedAttribute, error) {
	options, found, err := schemaext.FacetAs[*mysqlschema.DesiredIndex](facets, mysqlschema.IndexKind)
	if err != nil || !found || options.Parser == "" {
		return nil, err
	}
	var attributes []annotation.EncodedAttribute
	for _, target := range targetsOf(facets, mysqlschema.IndexKind) {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "platform." + target + "." + ParserProperty, Value: options.Parser})
	}
	return attributes, nil
}

// targetsOf are the targets the facet of kind is bound to, or every
// MySQL-family target where it is bound to none.
func targetsOf(facets schemaext.Facets, kind schemaext.Kind) []string {
	if targets := facets.TargetScope(kind); len(targets) > 0 {
		return targets
	}
	return mysqlschema.Targets()
}
