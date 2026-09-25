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

// relationRoutines are the routines placed with the relations, and where each
// of them is created.
type relationRoutines struct {
	// objects are the routines as entries in the view-like ordering. Added
	// routines come first, in call order, and replaced ones after them, so a
	// routine that neither reads a view nor is called by one keeps the place it
	// had among the routines.
	objects []deporder.ViewLike
	// nodes are the statements each routine name stands for: every overload,
	// and a replacement's drop where it needs one.
	nodes map[string][]ast.Node
	// tableSteps is the table phase: every new table, and the routines among
	// objects that a table's column default or CHECK calls, moved in between;
	// see [deporder.TablesWithRoutinesForCreate].
	tableSteps []deporder.TableStep
}

// viewLikes are the routines left for the view-like ordering: the ones the
// table phase does not create.
func (r relationRoutines) viewLikes() []deporder.ViewLike {
	inTablePhase := make(map[string]bool)
	for _, step := range r.tableSteps {
		if step.Routine {
			inTablePhase[step.Name] = true
		}
	}
	objects := make([]deporder.ViewLike, 0, len(r.objects))
	for _, object := range r.objects {
		if !inTablePhase[object.Name] {
			objects = append(objects, object)
		}
	}
	return objects
}

// relationRoutines collects the routines placed with the relations and decides
// which of them the table phase creates.
func (p *Planner) relationRoutines(
	diff *difftypes.SchemaDiff,
	placements map[string]deporder.RoutinePlacement,
) relationRoutines {
	routines := relationRoutines{nodes: make(map[string][]ast.Node)}
	var declarations []schemamodel.Function
	add := func(routine schemamodel.Function, statements []ast.Node) {
		if _, seen := routines.nodes[routine.Name]; !seen {
			routines.objects = append(routines.objects, deporder.ViewLike{
				Name:      routine.Name,
				Body:      deporder.RoutineOrderingBody(routine, p.targetDialect()),
				Routine:   true,
				DependsOn: diff.DeclaredFunctions.Dependencies[routine.Name],
			})
		}
		routines.nodes[routine.Name] = append(routines.nodes[routine.Name], statements...)
		declarations = append(declarations, routine)
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
	routines.tableSteps = deporder.TablesWithRoutinesForCreate(
		tableCreations(diff),
		declarations,
		diff.DeclaredFunctions.Dependencies,
		p.targetDialect(),
	)
	return routines
}

// tableCreations are the tables the table phase creates, in dependency order,
// and the existing tables gaining columns: their column defaults are added
// before the view-likes too, so a routine one calls has to come first as well.
func tableCreations(diff *difftypes.SchemaDiff) []deporder.TableCreation {
	creations := diff.TablesAdded.Qualified(diff.DeclaredUserTypes, DialectName).InDependencyOrder()
	tables := make([]deporder.TableCreation, 0, len(creations)+len(diff.TablesModified))
	for _, creation := range creations {
		tables = append(tables, deporder.TableCreation{
			Name:        creation.Name,
			Expressions: deporder.TableExpressions(creation.Table.Checks, creation.Fields, creation.Constraints),
		})
	}
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsAdded) == 0 {
			continue
		}
		tables = append(tables, deporder.TableCreation{
			Name:        tableDiff.TableName,
			Expressions: deporder.TableExpressions(nil, tableDiff.ColumnsAdded, nil),
			Existing:    true,
		})
	}
	return tables
}
