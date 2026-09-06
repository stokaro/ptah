package generator

// Reversing the user-type families -- enums, domains, composite types and
// ranges -- and recovering each one's prior definition.

import (
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/objectlookup"
	"ptah.run/internal/tableref"
	"ptah.run/migration/schemadiff/difftypes"
)

// reverseEnumDiffs reverses enum modifications for down migrations
// reverseEnumDiffs reverses enum modifications for down migrations.
//
// The columns a value removal has to convert are resolved against the
// PRE-CHANGE database rather than carried across. The rollback's removal is
// the forward direction's ADDITION, which carried no usages at all -- adding
// a value converts nothing -- and even where the forward direction did carry
// them they are the columns the DESIRED schema types by the enum, not the
// ones the database being rolled back to has.
func reverseEnumDiffs(enumDiffs []difftypes.EnumDiff, prior *schemamodel.Database) []difftypes.EnumDiff {
	reversed := make([]difftypes.EnumDiff, len(enumDiffs))
	for i, enumDiff := range enumDiffs {
		reversed[i] = difftypes.EnumDiff{
			EnumName:      enumDiff.EnumName,
			ValuesAdded:   enumDiff.ValuesRemoved, // Values to remove become values to add
			ValuesRemoved: enumDiff.ValuesAdded,   // Values to add become values to remove
			// Named here rather than assigned after, so the reversal census can
			// see the field is accounted for: it reads the literal's keys.
			Usages: priorEnumColumnUsages(prior, enumDiff.EnumName, enumDiff.ValuesAdded),
		}
	}
	return reversed
}

// priorEnumColumnUsages lists the columns the pre-change database typed by the
// named enum, in the shape the comparison carries for the forward direction.
func priorEnumColumnUsages(
	prior *schemamodel.Database,
	enumName string,
	removedByReversal []string,
) []difftypes.EnumColumnUsage {
	// Only a removal converts columns, and the rollback removes what the
	// forward direction added.
	if prior == nil || len(removedByReversal) == 0 {
		return nil
	}
	tablesByStruct := make(map[string]schemamodel.Table, len(prior.Tables))
	for _, table := range prior.Tables {
		tablesByStruct[table.StructName] = table
	}
	bareName := enumName
	if ref, ok := tableref.Parse(enumName); ok {
		bareName = ref.Name
	}
	usages := make([]difftypes.EnumColumnUsage, 0)
	for _, field := range prior.Fields {
		if field.Type != enumName && field.Type != bareName {
			continue
		}
		table, ok := tablesByStruct[field.StructName]
		if !ok {
			continue
		}
		usages = append(usages, difftypes.EnumColumnUsage{
			Table:       table.QualifiedName(),
			Column:      field.Name,
			Default:     field.Default,
			DefaultSet:  field.DefaultSet,
			DefaultExpr: field.DefaultExpr,
		})
	}
	if len(usages) == 0 {
		return nil
	}
	return usages
}

// reverseDomainDiffs turns each modified domain around for the down direction.
//
// CurrentBaseType has to be re-derived rather than carried over: it names the
// shape the DROP will run against, and a down migration runs against the shape
// the up migration created. schema is the up direction's target, so that is
// where the down direction's from-side lives. A nil schema leaves it empty and
// the drop ordering falls back to declaration order.
func reverseDomainDiffs(
	domainDiffs []difftypes.DomainDiff,
	schema, prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.DomainDiff {
	reversed := make([]difftypes.DomainDiff, len(domainDiffs))
	for i, domainDiff := range domainDiffs {
		reversed[i] = difftypes.DomainDiff{
			DomainName: domainDiff.DomainName,
			Changes:    reverseChangeMap(domainDiff.Changes),
			// The current-side base type is re-derived from the schema the UP
			// migration leaves behind, which is what the DOWN direction is
			// changing away from. CurrentCheckConstraints cannot be: see
			// nestedCoverageExempt.
			CurrentBaseType: targetDomainBaseType(schema, domainDiff.DomainName),
			// The recreate half renders from the operand, so reversing the
			// change map without reversing the operand would rebuild the very
			// definition the rollback is undoing (stokaro/ptah#2315).
			Desired: priorDomain(prior, domainDiff.DomainName, semantics),
		}
	}
	return reversed
}

// priorDomain is the domain the pre-change database held, or a zero one when it
// held none -- which is what withholds the drop.
func priorDomain(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.Domain {
	if prior == nil {
		return schemamodel.Domain{}
	}
	if domain := objectlookup.Qualified(prior.Domains, name, semantics); domain != nil {
		return *domain
	}
	return schemamodel.Domain{}
}

// reverseRangeDiffs mirrors reverseDomainDiffs for range types.
func reverseRangeDiffs(
	rangeDiffs []difftypes.RangeDiff,
	schema, prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.RangeDiff {
	reversed := make([]difftypes.RangeDiff, len(rangeDiffs))
	for i, rangeDiff := range rangeDiffs {
		reversed[i] = difftypes.RangeDiff{
			RangeName:      rangeDiff.RangeName,
			Changes:        reverseChangeMap(rangeDiff.Changes),
			CurrentSubtype: targetRangeSubtype(schema, rangeDiff.RangeName),
			Desired:        priorRange(prior, rangeDiff.RangeName, semantics),
		}
	}
	return reversed
}

// priorRange mirrors [priorDomain] for range types.
func priorRange(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.Range {
	if prior == nil {
		return schemamodel.Range{}
	}
	if rangeType := objectlookup.Qualified(prior.Ranges, name, semantics); rangeType != nil {
		return *rangeType
	}
	return schemamodel.Range{}
}

func targetRangeSubtype(schema *schemamodel.Database, name string) string {
	if schema == nil {
		return ""
	}
	for _, rangeType := range schema.Ranges {
		if rangeType.QualifiedName() == name {
			return rangeType.Subtype
		}
	}
	return ""
}

// reverseCompositeTypeDiffs mirrors reverseDomainDiffs for composite types.
func reverseCompositeTypeDiffs(
	compositeDiffs []difftypes.CompositeTypeDiff,
	schema, prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.CompositeTypeDiff {
	reversed := make([]difftypes.CompositeTypeDiff, len(compositeDiffs))
	for i, compositeDiff := range compositeDiffs {
		reversed[i] = difftypes.CompositeTypeDiff{
			TypeName:          compositeDiff.TypeName,
			Changes:           reverseChangeMap(compositeDiff.Changes),
			CurrentFieldTypes: targetCompositeFieldTypes(schema, compositeDiff.TypeName),
			// An added attribute is a removed one coming back, and the pair is
			// what lets the planner ALTER instead of rebuilding. Dropping both
			// sent every reversed composite down the drop-and-recreate path
			// (stokaro/ptah#2418).
			AttributesAdded:   restoredCompositeAttributes(prior, compositeDiff.TypeName, compositeDiff.AttributesRemoved),
			AttributesRemoved: compositeAttributeNames(compositeDiff.AttributesAdded),
			Desired:           priorCompositeType(prior, compositeDiff.TypeName, semantics),
		}
	}
	return reversed
}

// priorCompositeType mirrors [priorDomain] for composite types.
func priorCompositeType(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.CompositeType {
	if prior == nil {
		return schemamodel.CompositeType{}
	}
	if composite := objectlookup.Qualified(prior.CompositeTypes, name, semantics); composite != nil {
		return *composite
	}
	return schemamodel.CompositeType{}
}

func targetDomainBaseType(schema *schemamodel.Database, name string) string {
	if schema == nil {
		return ""
	}
	for _, domain := range schema.Domains {
		if domain.QualifiedName() == name {
			return domain.BaseType
		}
	}
	return ""
}

func targetCompositeFieldTypes(schema *schemamodel.Database, name string) []string {
	if schema == nil {
		return nil
	}
	for _, composite := range schema.CompositeTypes {
		if composite.QualifiedName() != name {
			continue
		}
		fieldTypes := make([]string, len(composite.Fields))
		for i, field := range composite.Fields {
			fieldTypes[i] = field.Type
		}
		return fieldTypes
	}
	return nil
}

// compositeAttributeNames is the removal shape of a set of added attributes: a
// DROP ATTRIBUTE takes a name, and nothing else of the attribute survives it.
func compositeAttributeNames(added []difftypes.CompositeAttribute) []string {
	if len(added) == 0 {
		return nil
	}
	names := make([]string, 0, len(added))
	for _, attribute := range added {
		names = append(names, attribute.Name)
	}
	return names
}

// restoredCompositeAttributes gives a removed attribute back its type, which a
// removal does not carry and an ADD ATTRIBUTE needs.
//
// The type comes from the PRE-CHANGE schema, because that is the composite the
// rollback is restoring. An attribute the prior schema does not describe is
// left out rather than added with an empty type, which would render
// `ADD ATTRIBUTE "x" ` and be refused.
func restoredCompositeAttributes(
	prior *schemamodel.Database,
	typeName string,
	removed []string,
) []difftypes.CompositeAttribute {
	if prior == nil || len(removed) == 0 {
		return nil
	}
	types := make(map[string]string)
	for _, composite := range prior.CompositeTypes {
		if composite.QualifiedName() != typeName {
			continue
		}
		for _, field := range composite.Fields {
			types[field.Name] = field.Type
		}
	}
	restored := make([]difftypes.CompositeAttribute, 0, len(removed))
	for _, name := range removed {
		fieldType, known := types[name]
		if !known {
			continue
		}
		restored = append(restored, difftypes.CompositeAttribute{Name: name, Type: fieldType})
	}
	return restored
}
