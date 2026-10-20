package generator

// Reversing enum changes, and recovering the columns the prior database typed
// by each enum. A domain, a composite type and a range belong to their owners,
// which reverse them.

import (
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

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
