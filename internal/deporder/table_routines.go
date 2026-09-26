package deporder

import (
	"strconv"
	"strings"

	"ptah.run/core/schemamodel"
)

// TableCreation is a table as the routine ordering sees it: its name and the
// expressions its definition carries.
type TableCreation struct {
	Name string
	// Expressions is the text of the column defaults, CHECK clauses, generated
	// columns and table constraints the table is created or altered with; see
	// [TableExpressions]. A routine one of them calls has to exist first.
	Expressions string
	// Existing marks a table that is already there and gains the columns
	// Expressions came from. It constrains the routines the columns call and
	// creates nothing, so it is not in the result.
	Existing bool
}

// TableStep is one statement of the table phase: a table, or a routine a table
// calls.
type TableStep struct {
	Name    string
	Routine bool
}

// TableExpressions is the expression text a table's checks, columns and
// constraints carry, in which it can call a routine: table CHECK clauses,
// column defaults, CHECK clauses, generated columns and ON UPDATE expressions,
// and CHECK and EXCLUDE constraints.
func TableExpressions(checks []string, fields []schemamodel.Field, constraints []schemamodel.Constraint) string {
	var text strings.Builder
	for _, check := range checks {
		text.WriteString(check)
		text.WriteString("\n")
	}
	for _, field := range fields {
		for _, expression := range []string{
			field.Default,
			field.DefaultExpr,
			field.Check,
			field.GeneratedExpression,
			field.UpdateExpression,
		} {
			text.WriteString(expression)
			text.WriteString("\n")
		}
	}
	for _, constraint := range constraints {
		text.WriteString(constraint.CheckExpression)
		text.WriteString("\n")
		text.WriteString(constraint.ExcludeElements)
		text.WriteString("\n")
	}
	return text.String()
}

// TablesWithRoutinesForCreate returns the table phase of a plan or a render:
// every table that is not [TableCreation.Existing], in the order given, and the
// routines among routines that a table calls, each moved in between.
//
// routines are the ones placed [RoutineWithRelations], which otherwise wait
// for the views, after every table. That is too late for a routine a column
// default or a CHECK calls. Measured on PostgreSQL 18.6, a column default
// calling a LANGUAGE sql routine that reads another new table has one order
// the server accepts: the table the routine reads, the routine, then the table
// with the default. So such a routine goes after the tables its definition
// names and before the tables that call it, and so do the routines it calls,
// for the same reason one level down.
//
// A routine no table calls is not in the result; it stays with the views. A
// cycle -- a routine that reads the very table whose default calls it -- is
// one no order satisfies, and degrades to the order given, which PostgreSQL
// then refuses at the routine.
func TablesWithRoutinesForCreate(
	tables []TableCreation,
	routines []schemamodel.Function,
	dependencies map[string][]string,
	dialect string,
) []TableStep {
	byName := make(map[string][]schemamodel.Function, len(routines))
	var routineNames []string
	for _, routine := range routines {
		if _, seen := byName[routine.Name]; !seen {
			routineNames = append(routineNames, routine.Name)
		}
		byName[routine.Name] = append(byName[routine.Name], routine)
	}

	called := calledRoutines(tables, routineNames, dependencies, dialect)
	if len(called) == 0 {
		return tablesOnly(tables)
	}

	nodes, edges := tableRoutineGraph(tables, routineNames, byName, called, dependencies, dialect)

	steps := make([]TableStep, 0, len(nodes))
	for _, id := range StableTopologicalSort(nodes, edges) {
		if name, isRoutine := strings.CutPrefix(id, "routine:"); isRoutine {
			steps = append(steps, TableStep{Name: name, Routine: true})
			continue
		}
		index, _ := strconv.Atoi(strings.TrimPrefix(id, "table:"))
		if !tables[index].Existing {
			steps = append(steps, TableStep{Name: tables[index].Name})
		}
	}
	return steps
}

// tableRoutineGraph is the graph the table phase is sorted by. A table depends
// on the routines its expressions call, a routine on the new tables its
// definition names and on the routines it calls. Tables come first in node
// order, so a table and a routine that become ready together keep the table
// ahead.
func tableRoutineGraph(
	tables []TableCreation,
	routineNames []string,
	byName map[string][]schemamodel.Function,
	called map[string]bool,
	dependencies map[string][]string,
	dialect string,
) (nodes []string, edges map[string][]string) {
	edges = make(map[string][]string)
	for i := range tables {
		nodes = append(nodes, tableNodeID(i))
	}
	for _, name := range routineNames {
		if !called[name] {
			continue
		}
		nodes = append(nodes, routineNodeID(name))
		edges[routineNodeID(name)] = append(edges[routineNodeID(name)], namedTables(tables, byName[name], dialect)...)
		for _, callee := range dependencies[name] {
			if called[callee] {
				edges[routineNodeID(name)] = append(edges[routineNodeID(name)], routineNodeID(callee))
			}
		}
	}
	for i, table := range tables {
		for _, name := range routineNames {
			if called[name] && namesObject(table.Expressions, name, dialect) {
				edges[tableNodeID(i)] = append(edges[tableNodeID(i)], routineNodeID(name))
			}
		}
	}
	return nodes, edges
}

// namedTables are the nodes of the new tables any overload of a routine names.
func namedTables(tables []TableCreation, overloads []schemamodel.Function, dialect string) []string {
	var named []string
	for i, table := range tables {
		if table.Existing {
			continue
		}
		for _, routine := range overloads {
			if namesObject(RoutineOrderingBody(routine, dialect), table.Name, dialect) {
				named = append(named, tableNodeID(i))
				break
			}
		}
	}
	return named
}

func tableNodeID(index int) string { return "table:" + strconv.Itoa(index) }

func routineNodeID(name string) string { return "routine:" + name }

// calledRoutines is the routines a table's expressions call, with the routines
// they call in turn.
func calledRoutines(
	tables []TableCreation,
	routineNames []string,
	dependencies map[string][]string,
	dialect string,
) map[string]bool {
	known := make(map[string]bool, len(routineNames))
	for _, name := range routineNames {
		known[name] = true
	}
	called := make(map[string]bool)
	var pending []string
	for _, table := range tables {
		for _, name := range routineNames {
			if !called[name] && namesObject(table.Expressions, name, dialect) {
				called[name] = true
				pending = append(pending, name)
			}
		}
	}
	for len(pending) > 0 {
		name := pending[0]
		pending = pending[1:]
		for _, callee := range dependencies[name] {
			if known[callee] && !called[callee] {
				called[callee] = true
				pending = append(pending, callee)
			}
		}
	}
	return called
}

func tablesOnly(tables []TableCreation) []TableStep {
	steps := make([]TableStep, 0, len(tables))
	for _, table := range tables {
		if !table.Existing {
			steps = append(steps, TableStep{Name: table.Name})
		}
	}
	return steps
}
