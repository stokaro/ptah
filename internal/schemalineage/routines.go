package schemalineage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/routineext"
	"ptah.run/core/schemamodel"
)

// Bodies reads routine bodies in their engine's language: the engine runtime
// a caller holds answers it, through the owner of the target's routine body
// reader (ptah.run/core/routineext).
type Bodies interface {
	ReadRoutineBodies(ctx context.Context, request routineext.ReadRequest) (routineext.ReadResult, error)
}

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
	Reads     []RoutineRead      `json:"reads"`
	Writes    []RoutineWrite     `json:"writes"`
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
//
// A procedural body is read by the owner of the dialect's routine bodies,
// every body of the schema in one call to bodies. A dialect whose owner reads
// none says so on each of its procedural routines. A reader's error, and a
// procedural routine without a reader to ask, return no result.
func DeriveRoutines(ctx context.Context, bodies Bodies, db *schemamodel.Database, dialect string) (RoutineResult, error) {
	if db == nil {
		return RoutineResult{}, nil
	}
	columns := columnsByTable(db)
	var procedural []int
	for i, routine := range db.Functions {
		if isProcedural(routine, columns) {
			procedural = append(procedural, i)
		}
	}
	readings, err := readBodies(ctx, bodies, db.Functions, procedural, dialect)
	if err != nil {
		return RoutineResult{}, err
	}
	var result RoutineResult
	for i, routine := range db.Functions {
		var reading *routineext.Reading
		if read, ok := readings[i]; ok {
			reading = &read
		}
		result.absorbRoutine(deriveRoutine(routine, dialect, columns, reading))
	}
	sortRoutineResult(&result)
	return result, nil
}

// readBodies asks the dialect's owner to read the procedural routines' bodies,
// all of them in one call, and returns the readings by routine index. A
// dialect whose owner reads no bodies returns none.
func readBodies(
	ctx context.Context, bodies Bodies, routines []schemamodel.Function, procedural []int, dialect string,
) (map[int]routineext.Reading, error) {
	if len(procedural) == 0 {
		return nil, nil
	}
	if bodies == nil {
		return nil, errors.New("routine lineage reads procedural bodies and was given no routine body reader")
	}
	request := routineext.ReadRequest{Target: dialect, Bodies: make([]routineext.Body, 0, len(procedural))}
	for _, index := range procedural {
		routine := routines[index]
		request.Bodies = append(request.Bodies, routineext.Body{Kind: routineKind(routine), Language: routine.Language, SQL: routine.Body})
	}
	result, err := bodies.ReadRoutineBodies(ctx, request)
	if errors.Is(err, ptaherr.ErrUnsupportedDialect) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the routine bodies: %w", err)
	}
	readings := make(map[int]routineext.Reading, len(procedural))
	for i, index := range procedural {
		readings[index] = result.Bodies[i]
	}
	return readings, nil
}

// isProcedural reports whether a routine takes the procedural path rather
// than the single-SELECT one.
//
// A procedural body is resolved for its writes and not for its reads, and the
// caller is told exactly that. Deriving the writes and staying silent about
// the reads would let "nothing reads this column" be concluded from a routine
// whose reads were never looked at.
//
// A procedure declaring plain SQL is how a MySQL or T-SQL routine reaches this
// package: it has no LANGUAGE clause of its own, and the converter records it
// as SQL. Its body is that dialect's procedural language, so it takes the
// procedural path rather than being asked to be one SELECT
// (stokaro/ptah#2435 is what makes these reach here at all).
func isProcedural(routine schemamodel.Function, columns map[string][]string) bool {
	if language := strings.ToLower(strings.TrimSpace(routine.Language)); language != "sql" && language != "" {
		return true
	}
	if strings.TrimSpace(routine.Body) == "" {
		return false
	}
	return routineKind(routine) == "procedure" && !isSingleSelectBody(routine.Body, columns)
}

// deriveRoutine resolves one routine body, or records why it could not.
// reading is the owner's reading of a procedural body, nil when the owner read
// none.
func deriveRoutine(
	routine schemamodel.Function, dialect string, columns map[string][]string, reading *routineext.Reading,
) RoutineResult {
	kind := routineKind(routine)
	if isProcedural(routine, columns) {
		return deriveProceduralRoutine(routine, dialect, kind, columns, reading)
	}
	if strings.TrimSpace(routine.Body) == "" {
		return RoutineResult{Undecided: []UndecidedRoutine{
			{Routine: routine.Name, Reason: "the routine declares no body", Kind: kind},
		}}
	}

	// The view path resolves exactly this shape, and asking it is what keeps
	// one answer rather than two that drift.
	viewResult := deriveView(routine.Name, routine.Body, false, columns, bodyReading{})
	if len(viewResult.Undecided) > 0 {
		return RoutineResult{Undecided: []UndecidedRoutine{
			{Routine: routine.Name, Reason: viewResult.Undecided[0].Reason, Kind: kind},
		}}
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

// deriveProceduralRoutine resolves the writes a procedural body performs, and
// says what about that routine is still unresolved.
//
// Every procedural routine carries an undecided entry, whether or not a
// statement went unrecognized, because its reads are not derived at all. The
// entry says which of the two it is: an incomplete write list and unresolved
// reads, or a complete write list and unresolved reads.
func deriveProceduralRoutine(
	routine schemamodel.Function, dialect, kind string, columns map[string][]string, reading *routineext.Reading,
) RoutineResult {
	analysis, analyzed := analyzeProcedural(routine, dialect, kind, reading)
	if !analyzed {
		return RoutineResult{Undecided: []UndecidedRoutine{{
			Routine: routine.Name,
			Kind:    kind,
			Reason:  fmt.Sprintf("no routine-body analysis exists for the %s dialect", dialect),
		}}}
	}
	result := RoutineResult{Writes: analysis.writes}
	if analysis.columnReads {
		result.Reads = deriveProceduralReads(routine.Name, kind, columns, analysis.statements)
	}
	language := strings.ToLower(strings.TrimSpace(routine.Language))
	if language == "sql" {
		language = ""
	}
	result.Undecided = []UndecidedRoutine{{
		Routine: routine.Name,
		Kind:    kind,
		Reason:  analysis.reason(bodyLanguage(language, dialect)),
	}}
	return result
}

// procedural is what the analysis of one procedural body established.
type procedural struct {
	writes     []RoutineWrite
	unresolved []string
	// statements are the text of each statement, read for the columns the
	// body reads when columnReads says a name in it may be attributed.
	statements  []string
	columnReads bool
}

// analyzeProcedural analyzes a procedural body from the owner's reading, or,
// for a dialect whose owner reads no bodies yet, with this package's own body
// parser for it. It reports false for a dialect with neither.
func analyzeProcedural(routine schemamodel.Function, dialect, kind string, reading *routineext.Reading) (procedural, bool) {
	if reading != nil {
		writes, unresolved := readingWrites(reading.Statements, routine.Name, kind)
		return procedural{writes: writes, unresolved: unresolved, statements: readingSQL(reading.Statements),
			columnReads: reading.ColumnReads}, true
	}
	writes, unresolved, analyzed := proceduralWritesFor(dialect, routine, kind)
	if !analyzed {
		return procedural{}, false
	}
	// The one dialect analyzed here is T-SQL. Its variables carry an @ prefix
	// and cannot share a name with a column, so a name in its body may be
	// attributed as a column read.
	return procedural{writes: writes, unresolved: unresolved, statements: proceduralStatementSQL(routine), columnReads: true}, true
}

// bodyLanguage names the language a body is written in for the reason line.
//
// A routine declaring one is taken at its word; one that declares none is
// written in whatever procedural language its dialect has, and naming the
// dialect is more use to a reader than naming nothing.
func bodyLanguage(language, dialect string) string {
	if language != "" {
		return language
	}
	return dialect
}

// isSingleSelectBody reports whether a body is the shape the read path
// resolves, which is what decides whether a plain-SQL routine is procedural.
func isSingleSelectBody(body string, columns map[string][]string) bool {
	return len(deriveView("", body, false, columns, bodyReading{}).Undecided) == 0
}

// reason states what was resolved and what was not.
//
// The reads clause is per engine rather than fixed, because whether a read can
// be answered at all is a property of the engine: see
// [routineext.Reading.ColumnReads].
func (a procedural) reason(language string) string {
	if len(a.unresolved) == 0 {
		return fmt.Sprintf("the body is %s: every statement was classified, so the writes are complete; %s",
			language, a.readsClause())
	}
	return fmt.Sprintf("the body is %s: %s, so the writes are not complete; %s",
		language, strings.Join(a.unresolved, "; "), a.readsClause())
}

// readsClause says what was established about the columns a body reads.
func (a procedural) readsClause() string {
	if !a.columnReads {
		return "the columns it reads are not resolved, because a declared variable takes " +
			"precedence over a column of the same name on this engine and the body does not " +
			"distinguish them"
	}
	return "the reads are those of its statements that name one table"
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
	r.Reads = append(r.Reads, other.Reads...)
	r.Writes = append(r.Writes, other.Writes...)
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
	sortRoutineReads(result.Reads)
	sort.Slice(result.Writes, func(i, j int) bool {
		a, b := result.Writes[i], result.Writes[j]
		if a.ByRoutine != b.ByRoutine {
			return a.ByRoutine < b.ByRoutine
		}
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		return a.Column < b.Column
	})
	sort.Slice(result.Undecided, func(i, j int) bool {
		return result.Undecided[i].Routine < result.Undecided[j].Routine
	})
}
