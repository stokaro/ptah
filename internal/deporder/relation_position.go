package deporder

import "strings"

// relationReads is what a view's query reads: each name in a position where a
// relation can appear, and the bytes those names cover in the query as
// [codeNames] reads it, lowered. A name anywhere else is not a relation the
// query reads: a column alias, a table alias, a column a table alias
// qualifies, or a common table expression's name.
type relationReads struct {
	positions []bool
	names     []dottedName
}

// relationsReadBy reads the relations body reads, with dialect's lexical
// rules; see [sqlMasksForDialect].
//
// A relation position is the name after FROM, after a JOIN of any kind, after
// APPLY or TABLE, after a comma of a FROM list, and inside parentheses that
// open in one of those places, as `FROM (a JOIN b ON ...)` does; ONLY and
// LATERAL may come between. A name directly after IN counts too, which is how
// ClickHouse tests membership in a table, and so does a type after `::` or
// after AS in a CAST, since on PostgreSQL a view's row type is a type of the
// view's name. A name followed by an opening parenthesis, outside a type, is a
// function, not a relation. A common table expression hides
// a relation of its name from the end of its definition to the end of the
// query that declares it, and from inside its definition too under WITH
// RECURSIVE.
func relationsReadBy(body, dialect string) relationReads {
	body = strings.ToLower(body)
	reads := relationReads{positions: make([]bool, len(body))}
	if strings.TrimSpace(body) == "" {
		return reads
	}
	masks := sqlMasksForDialect(body, dialect)
	reader := relationReader{body: body, masks: masks, frames: []*relationFrame{{}}}
	reader.read()
	for _, span := range reader.relations {
		if reader.hidden(span) {
			continue
		}
		reads.names = append(reads.names, span.name)
		for i := span.start; i < span.end; i++ {
			reads.positions[i] = true
		}
	}
	return reads
}

// relationReader walks a lowered body, keeping one frame for each open
// parenthesis.
type relationReader struct {
	body      string
	masks     sqlMasks
	frames    []*relationFrame
	relations []nameSpan
	ctes      []*commonTable
	// castNext reports that the next parenthesis opens a CAST.
	castNext bool
}

// relationFrame is the state of one level of parentheses.
type relationFrame struct {
	// inFrom reports that a comma here separates relations.
	inFrom bool
	// expectRelation reports that the next name is a relation.
	expectRelation bool
	// expectBare reports that the next name is a relation if no
	// parenthesis comes first, as after IN.
	expectBare bool
	// expectType reports that the next name is a type, after `::` or after
	// AS in a CAST.
	expectType bool
	// cast reports that this level holds the operands of a CAST.
	cast bool
	// started reports that this level has read a word or a parenthesis, so
	// a WITH here is not the start of its query.
	started bool
	with    withState
	// pending is the common table expression this level is declaring.
	pending *commonTable
	// defines is the common table expression whose definition this level
	// holds, nil for any other level.
	defines *commonTable
	// declared are the common table expressions this level's WITH declares.
	declared  []*commonTable
	recursive bool
}

// withState is where a level is in its WITH clause.
type withState int

const (
	withNone withState = iota
	withName
	withAfterName
	withDefinition
	withAfterDefinition
)

// commonTable is a common table expression: its name, where the name and its
// definition end, whether WITH RECURSIVE declares it, and where the query that
// declares it ends.
type commonTable struct {
	name      string
	start     int
	defEnd    int
	scopeEnd  int
	recursive bool
}

func (r *relationReader) top() *relationFrame {
	return r.frames[len(r.frames)-1]
}

func (r *relationReader) read() {
	for i := 0; i < len(r.body); {
		part, end, ok := namePart(r.body, r.masks, i)
		if ok {
			span := r.dottedNameAt(i, part, end)
			r.name(span)
			i = span.end
			continue
		}
		if r.masks.code[i] && !r.masks.quotedIdentifier[i] {
			switch r.body[i] {
			case ':':
				if i+1 < len(r.body) && r.body[i+1] == ':' && r.masks.code[i+1] {
					r.top().expectType = true
					i += 2
					continue
				}
			case '(':
				r.open()
			case ')':
				r.close(i)
			case ',':
				r.comma()
			}
		}
		i = skipNonName(r.body, r.masks, i)
	}
	for len(r.frames) > 0 {
		r.pop(len(r.body))
	}
}

// dottedNameAt reads the dotted name whose first part, part, starts at start
// and ends at end, as [dottedNameSpans] does.
func (r *relationReader) dottedNameAt(start int, part string, end int) nameSpan {
	name := dottedName{part}
	for {
		dot := skipCodeSpace(r.body, r.masks, end)
		if dot >= len(r.body) || !isCodeDot(r.body, r.masks, dot) {
			break
		}
		next, nextEnd, ok := namePart(r.body, r.masks, skipCodeSpace(r.body, r.masks, dot+1))
		if !ok {
			name = append(name, "")
			end = dot + 1
			break
		}
		name = append(name, next)
		end = nextEnd
	}
	return nameSpan{name: name, start: start, end: end}
}

// word is span's text when span is one bare word, and empty for a quoted or
// dotted name, which is never a keyword.
func (r *relationReader) word(span nameSpan) string {
	if len(span.name) != 1 || r.masks.quoteOpen[span.start] || r.masks.quotedIdentifier[span.start] ||
		(r.masks.bracketNames && r.body[span.start] == '[') {
		return ""
	}
	return span.name[0]
}

func (r *relationReader) name(span nameSpan) {
	frame := r.top()
	word := r.word(span)
	if r.withClause(frame, span, word) {
		return
	}
	frame.started = true
	switch {
	case frame.expectType:
		frame.expectType = false
		r.relations = append(r.relations, span)
	case word == "as" && frame.cast:
		frame.expectType = true
	case word == "from" || word == "apply" || word == "join" || word == "straight_join":
		frame.inFrom, frame.expectRelation, frame.expectBare = true, true, false
	case word == "table":
		frame.expectRelation, frame.expectBare = true, false
	case word == "in":
		frame.expectBare = true
	case (word == "only" || word == "lateral") && frame.expectRelation:
	case relationClauseEnds[word]:
		frame.inFrom, frame.expectRelation, frame.expectBare = false, false, false
	case frame.expectRelation || frame.expectBare:
		frame.expectRelation, frame.expectBare = false, false
		if !r.followedByParenthesis(span.end) {
			r.relations = append(r.relations, span)
		}
	}
	r.castNext = (word == "cast" || word == "try_cast") && r.followedByParenthesis(span.end)
}

// withClause reads span as part of a WITH clause at frame's level and reports
// whether it did.
func (r *relationReader) withClause(frame *relationFrame, span nameSpan, word string) bool {
	switch frame.with {
	case withNone:
		if word != "with" || frame.started {
			return false
		}
		frame.started, frame.with = true, withName
	case withName:
		switch word {
		case "recursive":
			frame.recursive = true
		case "":
			frame.pending = &commonTable{name: span.name[len(span.name)-1], start: span.start, recursive: frame.recursive}
			frame.with = withAfterName
		case "as":
			// WITH <expression> AS <name>, as ClickHouse writes an alias.
			frame.with = withNone
			return false
		default:
			if relationClauseEnds[word] {
				frame.with = withNone
				return false
			}
			frame.pending = &commonTable{name: word, start: span.start, recursive: frame.recursive}
			frame.with = withAfterName
		}
	case withAfterName:
		if word != "as" {
			frame.with, frame.pending = withNone, nil
			return false
		}
		frame.with = withDefinition
	case withDefinition:
		if word != "not" && word != "materialized" {
			frame.with, frame.pending = withNone, nil
			return false
		}
	case withAfterDefinition:
		frame.with = withNone
		return false
	}
	return true
}

// followedByParenthesis reports that the code after offset end opens a
// parenthesis, so the name before it is a function.
func (r *relationReader) followedByParenthesis(end int) bool {
	next := skipCodeSpace(r.body, r.masks, end)
	return next < len(r.body) && r.masks.code[next] && !r.masks.quotedIdentifier[next] && r.body[next] == '('
}

func (r *relationReader) open() {
	frame := r.top()
	frame.started = true
	child := &relationFrame{cast: r.castNext}
	r.castNext = false
	switch frame.with {
	case withName:
		// WITH (subquery) AS name, ClickHouse's alias of a value.
		frame.with = withNone
	case withAfterName:
		// The column list of a common table expression, which reads nothing.
		r.frames = append(r.frames, child)
		return
	case withDefinition:
		child.defines = frame.pending
		frame.with, frame.pending = withAfterDefinition, nil
		r.frames = append(r.frames, child)
		return
	case withAfterDefinition:
		frame.with = withNone
	}
	if frame.expectRelation {
		child.inFrom, child.expectRelation = true, true
	}
	frame.expectRelation, frame.expectBare = false, false
	r.frames = append(r.frames, child)
}

func (r *relationReader) close(at int) {
	if len(r.frames) > 1 {
		r.pop(at)
	}
}

func (r *relationReader) pop(at int) {
	frame := r.top()
	for _, table := range frame.declared {
		table.scopeEnd = at
	}
	r.frames = r.frames[:len(r.frames)-1]
	if frame.defines == nil || len(r.frames) == 0 {
		return
	}
	frame.defines.defEnd = at
	parent := r.top()
	parent.declared = append(parent.declared, frame.defines)
	r.ctes = append(r.ctes, frame.defines)
}

func (r *relationReader) comma() {
	frame := r.top()
	switch {
	case frame.with == withAfterDefinition:
		frame.with = withName
	case frame.inFrom:
		frame.expectRelation = true
	}
}

// hidden reports that span names a common table expression where that
// expression is in scope, so it reads no relation.
func (r *relationReader) hidden(span nameSpan) bool {
	if len(span.name) != 1 {
		return false
	}
	for _, table := range r.ctes {
		if table.name != span.name[0] || span.start >= table.scopeEnd {
			continue
		}
		if span.start > table.defEnd || table.recursive && span.start > table.start {
			return true
		}
	}
	return false
}

// relationClauseEnds are the words that end a FROM list, or start a part of a
// query where no relation follows until the next FROM.
var relationClauseEnds = map[string]bool{
	"select": true, "where": true, "group": true, "having": true, "order": true,
	"limit": true, "offset": true, "fetch": true, "window": true, "qualify": true,
	"union": true, "intersect": true, "except": true, "minus": true, "values": true,
	"returning": true, "into": true, "for": true, "prewhere": true, "settings": true,
	"format": true, "connect": true, "start": true, "pivot": true, "unpivot": true,
}
