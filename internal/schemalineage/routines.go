package schemalineage

import (
	"fmt"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/schemamodel"
)

// RoutineEdge is one column a routine reads, and the routine that reads it.
//
// Its own type rather than [Edge] with a widened meaning: `Edge.ToView` names a
// view, and a routine put in that field would be a lie in the field name of a
// document `ptah schema lineage --format json` already publishes. Renaming a
// published field is a decision rather than a detail (stokaro/ptah#2395).
type RoutineEdge struct {
	// FromTable and FromColumn name the source the routine reads.
	FromTable  string `json:"from_table"`
	FromColumn string `json:"from_column"`
	// ToRoutine names the routine that reads it.
	ToRoutine string `json:"to_routine"`
	// Kind separates a function from a procedure.
	Kind string `json:"kind,omitempty"`
}

// UndecidedRoutine records a routine body this package could not resolve.
//
// It names the routine rather than a column for the reason [Undecided] names a
// view: the whole body went unresolved, and a caller cannot know which columns
// would have appeared. A routine whose dependencies were not resolved must not
// be indistinguishable from one with none.
type UndecidedRoutine struct {
	Routine string `json:"routine"`
	Reason  string `json:"reason"`
	Kind    string `json:"kind,omitempty"`
}

// RoutineResult is the derived routine lineage plus what could not be derived.
type RoutineResult struct {
	Edges     []RoutineEdge      `json:"edges"`
	Undecided []UndecidedRoutine `json:"undecided,omitempty"`
}

// DeriveRoutines returns the columns a schema's routines read.
//
// The shape it resolves is the one a `LANGUAGE sql` routine has: a body that is
// a single SELECT over one source, which is the same shape [Derive] resolves
// for a view and is resolved by the same code. Everything else -- a procedural
// body, a join, a subquery, a routine that composes SQL at run time -- becomes
// an [UndecidedRoutine] naming what stopped it.
//
// That boundary is the point rather than a limitation. "Nothing depends on this
// column" and "I could not tell" decide different things, and a confident wrong
// answer about a drop is worse than no answer (stokaro/ptah#2394).
func DeriveRoutines(db *schemamodel.Database) RoutineResult {
	if db == nil {
		return RoutineResult{}
	}
	columns := columnsByTable(db)
	var result RoutineResult
	for _, routine := range db.Functions {
		result.absorbRoutine(deriveRoutine(routine, columns))
	}
	sortRoutineResult(&result)
	return result
}

// deriveRoutine resolves one routine body, or records why it could not.
func deriveRoutine(routine schemamodel.Function, columns map[string][]string) RoutineResult {
	kind := routineKind(routine)
	undecided := func(reason string) RoutineResult {
		return RoutineResult{Undecided: []UndecidedRoutine{
			{Routine: routine.Name, Reason: reason, Kind: kind},
		}}
	}

	// A procedural body is a sequence of statements with control flow, and
	// resolving it needs the references the parser does not yet yield. #1270
	// records that as the prerequisite the rest waits on, and names matching
	// identifiers lexically as the approach #1280/#1281 measured as wrong.
	if language := strings.ToLower(strings.TrimSpace(routine.Language)); language != "sql" && language != "" {
		return undecided(fmt.Sprintf("the body is %s rather than plain SQL", language))
	}
	if strings.TrimSpace(routine.Body) == "" {
		return undecided("the routine declares no body")
	}

	// The view path resolves exactly this shape, and asking it is what keeps
	// one answer rather than two that drift.
	viewResult := deriveView(routine.Name, routine.Body, false, columns)
	if len(viewResult.Undecided) > 0 {
		return undecided(viewResult.Undecided[0].Reason)
	}

	edges := make([]RoutineEdge, 0, len(viewResult.Edges))
	for _, edge := range viewResult.Edges {
		edges = append(edges, RoutineEdge{
			FromTable:  edge.FromTable,
			FromColumn: edge.FromColumn,
			ToRoutine:  routine.Name,
			Kind:       kind,
		})
	}
	return RoutineResult{Edges: edges}
}

// routineKind names the routine family, defaulting to a function for the
// declarations written before procedures existed.
func routineKind(routine schemamodel.Function) string {
	if kind := strings.ToLower(strings.TrimSpace(routine.Kind)); kind != "" {
		return kind
	}
	return "function"
}

// absorbRoutine merges one routine's result into the whole.
func (r *RoutineResult) absorbRoutine(other RoutineResult) {
	r.Edges = append(r.Edges, other.Edges...)
	r.Undecided = append(r.Undecided, other.Undecided...)
}

// sortRoutineResult orders both lists, so two runs over one schema produce the
// same document and a diff of two schemas is a diff of their lineage rather
// than of Go's map iteration order.
func sortRoutineResult(result *RoutineResult) {
	sort.Slice(result.Edges, func(i, j int) bool {
		a, b := result.Edges[i], result.Edges[j]
		if a.ToRoutine != b.ToRoutine {
			return a.ToRoutine < b.ToRoutine
		}
		if a.FromTable != b.FromTable {
			return a.FromTable < b.FromTable
		}
		return a.FromColumn < b.FromColumn
	})
	sort.Slice(result.Undecided, func(i, j int) bool {
		return result.Undecided[i].Routine < result.Undecided[j].Routine
	})
}
