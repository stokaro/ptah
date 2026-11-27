package deporder

import (
	"slices"
	"strings"

	"ptah.run/core/platform/identifier"
)

// CreationDependenciesIn returns, for each object, the indexes of the objects
// its definition needs first. It reads names as [ViewLikesForCreateIn] does,
// keeping the edges separate so a planner can preserve additional statement
// boundaries while scheduling tables, views and routines together.
//
// A table supplies its column and constraint expressions as Body. An unnamed
// entry names no object, but its expressions can still require a routine, as
// an ALTER TABLE adding a column default does. Entries are not changed, and
// every returned index refers to objects in its original order.
func CreationDependenciesIn(objects []ViewLike, dialect string, semantics identifier.Semantics) [][]int {
	scope := sessionScope(semantics)
	positions := make(map[string]int, len(objects))
	idsByName := make(map[string][]string, len(objects))
	idsByKind := map[bool]map[string][]string{false: {}, true: {}}
	for i, object := range objects {
		id := viewLikeID(object, i)
		positions[id] = i
		if object.Name != "" {
			idsByName[object.Name] = append(idsByName[object.Name], id)
			idsByKind[object.Routine][object.Name] = append(idsByKind[object.Routine][object.Name], id)
		}
	}
	counts := map[bool]map[string]int{
		false: viewLikeBareNameCounts(idsByKind[false]),
		true:  viewLikeBareNameCounts(idsByKind[true]),
	}
	dependencies := make([][]int, len(objects))
	for i, object := range objects {
		relations, calls := creationNameReads(object.Body, dialect)
		if !object.Routine {
			relations = relationsReadBy(object.Body, dialect)
		}
		for before, candidate := range objects {
			if before == i || candidate.Name == "" {
				continue
			}
			reads := &relations
			if candidate.Routine {
				reads = &calls
			}
			if referencesViewLikeIdentifier(object.Body, candidate.Name, dialect, scope, counts[candidate.Routine], reads) {
				dependencies[i] = append(dependencies[i], before)
			}
		}
		for _, before := range declaredViewLikeEdges(object, idsByName) {
			dependencies[i] = append(dependencies[i], positions[before])
		}
		slices.Sort(dependencies[i])
		dependencies[i] = slices.Compact(dependencies[i])
	}
	return dependencies
}

// creationNameReads separates the namespaces at the use: f() calls a routine,
// FROM f reads a relation, and a parameter named f reads neither. A routine's
// statements name relations beyond a SELECT, such as INSERT INTO and UPDATE.
// The query reader supplies reads without aliases or common table expressions;
// the extra statement positions supply write targets and row types.
func creationNameReads(body, dialect string) (relations, calls relationReads) {
	body = strings.ToLower(body)
	relations.positions = make([]bool, len(body))
	calls.positions = make([]bool, len(body))
	masks := sqlMasksForDialect(body, dialect)
	reader := relationReader{body: body, masks: masks, frames: []*relationFrame{{}}, statements: true}
	reader.read()
	for _, span := range reader.relations {
		if !reader.hidden(span) && !creationLocalOperand(body, masks, span) {
			addCreationRead(&relations, span)
		}
	}
	for _, span := range dottedNameSpans(body, masks) {
		if creationLocalOperand(body, masks, span) {
			continue
		}
		use := codeNameUse(body, masks, span)
		write := creationWriteOperand(body, masks, span)
		if use.Call && !write {
			addCreationRead(&calls, span)
		}
		if write && !reader.hidden(span) {
			addCreationRead(&relations, span)
		}
		if relation, ok := creationAnchoredType(body, masks, span); ok {
			addCreationRead(&relations, relation)
		}
	}
	return relations, calls
}

// The name reader starts after unquoted local-name sigils. @t, #t and :t
// must not become a dependency on a catalog relation named t. A quoted name
// such as [@t] starts at its quote and remains a catalog relation.
func creationLocalOperand(body string, masks sqlMasks, span nameSpan) bool {
	return span.start > 0 && masks.code[span.start-1] &&
		strings.ContainsRune("@#:", rune(body[span.start-1]))
}

func creationWriteOperand(body string, masks sqlMasks, span nameSpan) bool {
	switch previousCode(body, masks, span.start).word {
	case "into":
		return slices.Contains([]string{"insert", "replace", "merge"}, creationStatementBefore(body, masks, span.start))
	case "using":
		return slices.Contains([]string{"merge", "delete"}, creationStatementBefore(body, masks, span.start))
	case "insert", "replace", "update", "truncate", "lock":
		return true
	default:
		return false
	}
}

// An anchored type occurs in a declaration. In an expression, % is modulo,
// even when its right operand happens to be a variable named rowtype.
func creationAnchoredType(body string, masks sqlMasks, span nameSpan) (nameSpan, bool) {
	at := skipCodeSpace(body, masks, span.end)
	if !codeByteAt(body, masks, at, '%') || !creationTypeContext(body, masks, span.start) {
		return nameSpan{}, false
	}
	at++
	for at < len(body) && (!masks.code[at] || isSpace(body[at])) {
		at++
	}
	part, end, ok := namePart(body, masks, at)
	if !ok {
		return nameSpan{}, false
	}
	reader := relationReader{body: body, masks: masks}
	qualifier := reader.word(nameSpan{name: dottedName{part}, start: at, end: end})
	switch qualifier {
	case "rowtype":
		return span, true
	case "type":
		// table.column%TYPE names a table; variable%TYPE does not.
		if len(span.name) > 1 {
			span.name = span.name[:len(span.name)-1]
			return span, true
		}
	}
	return nameSpan{}, false
}

func creationTypeContext(body string, masks sqlMasks, at int) bool {
	before := previousCode(body, masks, at)
	if before.at >= 0 && before.word == "" {
		return false
	}
	for before.at >= 0 {
		switch before.word {
		case "declare":
			return true
		case "begin", "end", "return", "select", "where", "when", "then", "if", "set", "default":
			return false
		}
		before = previousCode(body, masks, before.at)
	}
	return true
}

func addCreationRead(reads *relationReads, span nameSpan) {
	reads.names = append(reads.names, span.name)
	for i := span.start; i < span.end; i++ {
		reads.positions[i] = true
	}
}

// INTO and USING can name relations or local variables. Read the statement
// that owns the operand before deciding: EXECUTE USING binds a value, while
// MERGE USING reads a relation. SELECT INTO and FETCH INTO name no prerequisite.
func creationStatementBefore(body string, masks sqlMasks, at int) string {
	for {
		previous := previousCode(body, masks, at)
		if previous.at < 0 || previous.mark == ';' {
			return ""
		}
		at = previous.at
		switch previous.word {
		case "insert", "replace", "merge", "delete", "select", "returning", "fetch", "execute", "begin", "end":
			return previous.word
		}
	}
}
