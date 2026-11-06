package postgres

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/internal/pgeffects"
	"ptah.run/internal/routineargs"
	"ptah.run/internal/typeorder"
)

// typeReads are the user types of types a common statement uses, as reads a
// feature owner orders its types by: a statement that creates or changes a
// column or a routine reads the types it declares (see
// [pgeffects.TypeReads]), and a statement that drops a table or a routine
// reads the types the object holds as the database has it. The type owner
// creates a type before a statement that reads it and keeps a removed type
// until every drop that reads it has run.
//
// A drop's statement does not spell those types, so they come from the
// removal in diff: the columns of a removed table, and the arguments and the
// result of a removed routine. A routine drop the removals do not explain
// reads the argument types it spells.
func typeReads(diff *difftypes.SchemaDiff, node ast.Node, types featureplan.TypeContext) []plangraph.Effect {
	switch typed := node.(type) {
	case *ast.DropTableNode:
		return typeorder.Reads(removedTableTypes(diff, typed.TableNames()), types.Current)
	case *ast.DropFunctionNode:
		return typeorder.Reads(removedRoutineTypes(diff, typed), types.Current)
	default:
		return pgeffects.TypeReads(node, types.Desired)
	}
}

// dropsAnObjectWithTypes reports a statement that drops a table or a routine. Ahead of
// the feature removal window it holds no type back, since every window a removed type
// can leave in comes after it.
func dropsAnObjectWithTypes(node ast.Node) bool {
	switch node.(type) {
	case *ast.DropTableNode, *ast.DropFunctionNode:
		return true
	default:
		return false
	}
}

// removedTableTypes spells the type of each column of the removed tables
// names, every way the catalog records it: as the server formats it, as the
// domain it is, and as the type information_schema names, an array's element
// type among them. A spelling that names no user type reads nothing.
func removedTableTypes(diff *difftypes.SchemaDiff, names []string) []string {
	var spellings []string
	for _, removal := range diff.TablesRemoved {
		if !slices.Contains(names, removal.Name) {
			continue
		}
		for _, column := range removal.Current.Table.Columns {
			if column.FormattedType != "" {
				spellings = append(spellings, column.FormattedType)
			}
			if column.DomainName != "" {
				spellings = append(spellings, qualifiedSpelling(column.DomainSchema, column.DomainName))
			}
			if column.UDTName != "" {
				name := column.UDTName
				if strings.EqualFold(column.DataType, "ARRAY") {
					name = strings.TrimPrefix(name, "_")
				}
				spellings = append(spellings, qualifiedSpelling(column.UDTSchema, name), name)
			}
		}
	}
	return spellings
}

// removedRoutineTypes spells the types of each removed routine drop names:
// its arguments and its result.
func removedRoutineTypes(diff *difftypes.SchemaDiff, drop *ast.DropFunctionNode) []string {
	var spellings []string
	removed := diff.FunctionsRemoved
	if drop.IsProcedure() {
		removed = diff.ProceduresRemoved
	}
	for _, target := range drop.Targets() {
		matched := false
		for _, routine := range removed {
			if routine.Name != target.Name || (routine.Signature == nil) != (target.Parameters == nil) ||
				(routine.Signature != nil && *routine.Signature != *target.Parameters) {
				continue
			}
			matched = true
			spellings = append(spellings, routineargs.ArgumentTypes(routine.Parameters)...)
			spellings = append(spellings, routineargs.ResultTypes(routine.Returns)...)
		}
		if !matched && target.Parameters != nil {
			spellings = append(spellings, routineargs.ArgumentTypes(*target.Parameters)...)
		}
	}
	return spellings
}

func qualifiedSpelling(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}
