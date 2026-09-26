package deporder

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// RoutinePlacement is where a routine a plan creates can go, relative to the
// types and relations the same plan creates.
//
// A routine cannot simply go first or last. A column DEFAULT or a CHECK calls
// a routine, so the routine has to exist before the table. And PostgreSQL
// resolves some of what a routine's definition names when the routine is
// created: measured on PostgreSQL 18.6, `RETURNS mood` answers
// `type "mood" does not exist`, `RETURNS SETOF orders` answers
// `type "orders" does not exist`, and a LANGUAGE sql body reading `orders`
// answers `relation "orders" does not exist` -- the last also with a
// SQL-standard `RETURN` body, which `check_function_bodies = off` does not
// relax. So each routine goes as early as what it names allows.
type RoutinePlacement uint8

const (
	// RoutineBeforeTypes is a routine whose definition names nothing the plan
	// creates. It goes first, where a type, a sequence or a table created
	// later can call it.
	RoutineBeforeTypes RoutinePlacement = iota
	// RoutineAfterTypes is a routine whose signature names a user type the plan
	// creates or rebuilds, and no relation the plan creates. It goes after the
	// types and before the tables, so a column default can still call it.
	RoutineAfterTypes
	// RoutineWithRelations is a routine that names a relation the plan creates
	// or adds columns to: in its signature, as a row type, or in a body the
	// server resolves when the routine is created. It is ordered together with
	// the views and materialized views, because a view can call it and it can
	// read a view.
	RoutineWithRelations
)

// RoutineCreation is what a plan creates that a routine's definition can name.
type RoutineCreation struct {
	// Types are the user types the plan creates or drops and recreates:
	// enums, domains, composite types and ranges.
	Types []string
	// Relations are the tables, views and materialized views the plan
	// creates, and the tables it adds columns to. A body resolved at creation
	// fails on a column that does not exist yet as it does on a table.
	Relations []string
}

// ResolvesBodyAtCreate reports whether dialect resolves the names in routine's
// body when the routine is created, so that everything the body reads has to
// exist first.
//
// On the PostgreSQL family that is a LANGUAGE sql routine. A PL/pgSQL body is
// parsed at creation and resolved at its first call: measured on PostgreSQL
// 18.6, a PL/pgSQL function reading a table that does not exist is created. A
// routine stating no language is refused by the server, so it has nothing to
// order. The other dialects create a routine whose body names a missing table
// and report it later, or not at all, so none of them is listed.
func ResolvesBodyAtCreate(routine schemamodel.Function, dialect string) bool {
	return platform.IsPostgresFamily(dialect) && strings.EqualFold(strings.TrimSpace(routine.Language), "sql")
}

// PlaceRoutines returns where each routine can go, keyed by routine name.
//
// It is the one rule every path that creates routines orders them by, so a
// planner and a full-schema render cannot disagree about which routine has to
// wait for which relation. The placement is name-level, like the call graph:
// the overloads of one name share the latest placement any of them needs.
//
// dependencies is the call graph, routine name to the routine names it calls.
// A caller takes at least its callee's placement, because PostgreSQL resolves
// the call in a LANGUAGE sql body at creation, and the caller is ordered after
// the callee in every other case already.
func PlaceRoutines(
	routines []schemamodel.Function,
	dependencies map[string][]string,
	created RoutineCreation,
	dialect string,
) map[string]RoutinePlacement {
	placements := make(map[string]RoutinePlacement, len(routines))
	for _, routine := range routines {
		placement := placeRoutine(routine, created, dialect)
		if current, known := placements[routine.Name]; !known || placement > current {
			placements[routine.Name] = placement
		}
	}
	// A fixed point over the call graph. Each pass can only raise a placement,
	// and there are three, so the loop ends; a cycle in the graph raises every
	// routine on it to the highest placement any of them has.
	for changed := true; changed; {
		changed = false
		for caller, callees := range dependencies {
			current, known := placements[caller]
			if !known {
				continue
			}
			for _, callee := range callees {
				if calleePlacement, ok := placements[callee]; ok && calleePlacement > current {
					placements[caller] = calleePlacement
					current = calleePlacement
					changed = true
				}
			}
		}
	}
	return placements
}

// placeRoutine is the placement one routine's own definition needs.
func placeRoutine(routine schemamodel.Function, created RoutineCreation, dialect string) RoutinePlacement {
	signature := RoutineSignatureText(routine)
	resolvedBody := ""
	if ResolvesBodyAtCreate(routine, dialect) {
		resolvedBody = routine.Body
	}
	for _, relation := range created.Relations {
		if namesObject(signature, relation, dialect) || namesObject(resolvedBody, relation, dialect) {
			return RoutineWithRelations
		}
	}
	for _, userType := range created.Types {
		if namesObject(signature, userType, dialect) || namesObject(resolvedBody, userType, dialect) {
			return RoutineAfterTypes
		}
	}
	return RoutineBeforeTypes
}

// RoutineSignatureText is the part of a routine's definition the server
// resolves at creation whatever its language: the parameter list and the return
// type.
func RoutineSignatureText(routine schemamodel.Function) string {
	return "(" + routine.Parameters + ") " + routine.Returns
}

// RoutineOrderingBody is the text a routine is ordered by among the view-likes:
// its signature, and its body where the server resolves the body at creation.
// A PL/pgSQL body that names a view adds no edge, because the server does not
// read it until the routine runs.
func RoutineOrderingBody(routine schemamodel.Function, dialect string) string {
	if ResolvesBodyAtCreate(routine, dialect) {
		return RoutineSignatureText(routine) + "\n" + routine.Body
	}
	return RoutineSignatureText(routine)
}

// namesObject reports whether text names object, in any of the spellings a view
// body can use for it.
func namesObject(text, object, dialect string) bool {
	if strings.TrimSpace(text) == "" {
		return false
	}
	return referencesViewLikeIdentifier(text, object, dialect, map[string]int{strings.ToLower(viewLikeBareName(object)): 1})
}
