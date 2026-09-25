package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/modelast"
	"ptah.run/migration/schemadiff/difftypes"
)

// routinePlacements says where each routine this plan creates or replaces
// goes; see [deporder.PlaceRoutines], which is the rule. A replaced routine is
// placed by its new definition, because that is the one the server resolves.
func (p *Planner) routinePlacements(diff *difftypes.SchemaDiff) map[string]deporder.RoutinePlacement {
	routines := diff.FunctionsAdded.Declarations()
	for _, fnDiff := range diff.FunctionsModified {
		if fnDiff.Desired.Name != "" {
			routines = append(routines, fnDiff.Desired)
		}
	}
	return deporder.PlaceRoutines(
		routines,
		diff.DeclaredFunctions.Dependencies,
		routineCreation(diff),
		p.targetDialect(),
	)
}

// routineCreation is what this plan creates that a routine's definition can
// name. A rebuilt user type counts, because it is dropped and created again
// after the routines placed first. A table gaining a column counts, because a
// LANGUAGE sql body reading the column is refused until the column exists.
func routineCreation(diff *difftypes.SchemaDiff) deporder.RoutineCreation {
	var created deporder.RoutineCreation
	created.Types = append(created.Types, diff.EnumsAdded.Names()...)
	created.Types = append(created.Types, diff.DomainsAdded.Names()...)
	created.Types = append(created.Types, diff.CompositeTypesAdded.Names()...)
	created.Types = append(created.Types, diff.RangesAdded.Names()...)
	for _, domainDiff := range diff.DomainsModified {
		created.Types = append(created.Types, domainDiff.DomainName)
	}
	for _, compositeDiff := range diff.CompositeTypesModified {
		created.Types = append(created.Types, compositeDiff.TypeName)
	}
	for _, rangeDiff := range diff.RangesModified {
		created.Types = append(created.Types, rangeDiff.RangeName)
	}
	created.Relations = append(created.Relations, diff.TablesAdded.Names()...)
	created.Relations = append(created.Relations, diff.ViewsAdded.Names()...)
	created.Relations = append(created.Relations, diff.MaterializedViewsAdded.Names()...)
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsAdded) > 0 {
			created.Relations = append(created.Relations, tableDiff.TableName)
		}
	}
	return created
}

// addNewFunctions creates the routines this plan adds that are placed at
// placement, callees before their callers.
func (p *Planner) addNewFunctions(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	placements map[string]deporder.RoutinePlacement,
	placement deporder.RoutinePlacement,
) []ast.Node {
	for _, fn := range orderedAddedRoutines(diff) {
		if placements[fn.Name] == placement {
			result = append(result, modelast.FromFunction(fn))
		}
	}
	return result
}

func orderedAddedRoutines(diff *difftypes.SchemaDiff) []schemamodel.Function {
	return deporder.FunctionsForCreateWithOrdering(
		diff.FunctionsAdded.Declarations(),
		diff.DeclaredFunctions.Order,
		diff.DeclaredFunctions.Dependencies,
	)
}

// modifyExistingFunctions replaces the routines this plan changes that are
// placed at placement.
func (p *Planner) modifyExistingFunctions(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	placements map[string]deporder.RoutinePlacement,
	placement deporder.RoutinePlacement,
) []ast.Node {
	// The definition travels WITH the change (stokaro/ptah#2315). Without it
	// there is no faithful CREATE OR REPLACE to emit -- the change map records
	// what differs, never the whole body and attribute set -- so a change
	// carrying none is skipped.
	for _, fnDiff := range diff.FunctionsModified {
		if fnDiff.Desired.Name == "" || placements[fnDiff.Desired.Name] != placement {
			continue
		}
		result = append(result, modifiedFunctionNodes(fnDiff)...)
	}
	return result
}

// relationRoutineViewLikes are the routines placed with the relations, as
// entries in the view-like ordering, together with the statements each name
// stands for. Added routines come first, in call order, and replaced ones after
// them, so a routine that neither reads a view nor is called by one keeps the
// place it had among the routines.
func (p *Planner) relationRoutineViewLikes(
	diff *difftypes.SchemaDiff,
	placements map[string]deporder.RoutinePlacement,
) ([]deporder.ViewLike, map[string][]ast.Node) {
	var objects []deporder.ViewLike
	nodes := make(map[string][]ast.Node)
	add := func(routine schemamodel.Function, statements []ast.Node) {
		if _, seen := nodes[routine.Name]; !seen {
			objects = append(objects, deporder.ViewLike{
				Name:      routine.Name,
				Body:      deporder.RoutineOrderingBody(routine, p.targetDialect()),
				Routine:   true,
				DependsOn: diff.DeclaredFunctions.Dependencies[routine.Name],
			})
		}
		nodes[routine.Name] = append(nodes[routine.Name], statements...)
	}
	for _, fn := range orderedAddedRoutines(diff) {
		if placements[fn.Name] == deporder.RoutineWithRelations {
			add(fn, []ast.Node{modelast.FromFunction(fn)})
		}
	}
	for _, fnDiff := range diff.FunctionsModified {
		if fnDiff.Desired.Name != "" && placements[fnDiff.Desired.Name] == deporder.RoutineWithRelations {
			add(fnDiff.Desired, modifiedFunctionNodes(fnDiff))
		}
	}
	return objects, nodes
}
