package schemacensus

import (
	"regexp"
	"sort"
	"strings"

	"go.5x5.cz/ptah/internal/capabilityprobe"
)

// Emission is one physical database object a render creates.
//
// Kind and Name together are the identity the invariant is about: #2606's
// second invariant is that every physical object has one semantic owner and one
// DDL emission path, and the observable form of "one path" is that no render
// creates the same object twice.
//
// Name is scoped where the object is: a constraint belongs to its table, so two
// tables may each carry a constraint called `fk_parent` without either being a
// duplicate. It is normalized -- unquoted, lower-cased -- because the same
// object is spelled `"nodes"`, “ `nodes` “ and `[nodes]` across the dialects
// this corpus renders on, and a comparison holding the quoting would report a
// PostgreSQL duplicate and miss the MySQL one.
type Emission struct {
	Kind string
	Name string
}

// Emissions is what one render created, and what it wrote that creates nothing.
//
// Unclassified is returned rather than discarded. A statement shape this
// package does not recognize is a hole in the guard, and a guard that drops
// what it cannot read reports success over exactly the statements it is blind
// to -- so the corpus's unclassified set is asserted against a written list
// instead of being allowed to grow in silence.
type Emissions struct {
	Objects      []Emission
	Unclassified []string
}

// identifier matches a SQL identifier in every quoting the renderers produce,
// plus the bare form.
const identifier = `(?:"[^"]+"|` + "`[^`]+`" + `|\[[^\]]+\]|[A-Za-z_][A-Za-z_0-9$]*)`

// qualified matches an identifier that may carry a schema, or a database and a
// schema.
const qualified = identifier + `(?:\s*\.\s*` + identifier + `){0,2}`

// The pieces every CREATE pattern shares, named so no pattern below has to
// spell them again. MATERIALIZED VIEW precedes VIEW because a regexp
// alternation takes the first branch that matches.
const (
	orReplace       = `(?:OR\s+(?:REPLACE|ALTER)\s+)?`
	notExists       = `(?:IF\s+NOT\s+EXISTS\s+)?`
	tableModifiers  = `(?:GLOBAL\s+|LOCAL\s+|TEMP(?:ORARY)?\s+|UNLOGGED\s+|VIRTUAL\s+)?`
	indexModifiers  = `(?:UNIQUE\s+|FULLTEXT\s+|SPATIAL\s+|CLUSTERED\s+|NONCLUSTERED\s+)*`
	namedObjectKind = `MATERIALIZED\s+VIEW|TYPE|DOMAIN|SEQUENCE|VIEW|FUNCTION|` +
		`PROCEDURE|TRIGGER|SCHEMA|EXTENSION|ROLE|POLICY|DATABASE|SYNONYM`
	anyObjectKind = `TABLE|INDEX|` + namedObjectKind
)

var (
	commentLine   = regexp.MustCompile(`--[^\n]*`)
	createTable   = regexp.MustCompile(`(?is)^CREATE\s+` + orReplace + tableModifiers + `TABLE\s+` + notExists + `(` + qualified + `)`)
	createIndex   = regexp.MustCompile(`(?is)^CREATE\s+` + indexModifiers + `INDEX\s+(?:CONCURRENTLY\s+)?` + notExists + `(` + qualified + `)`)
	addNamed      = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+(?:ONLY\s+|IF\s+EXISTS\s+)*(` + qualified + `)\s+ADD\s+CONSTRAINT\s+(` + qualified + `)`)
	inlineNamed   = regexp.MustCompile(`(?is)\bCONSTRAINT\s+(` + qualified + `)`)
	createOther   = regexp.MustCompile(`(?is)^CREATE\s+` + orReplace + `(?:` + namedObjectKind + `)\s+` + notExists + `(` + qualified + `)`)
	createKind    = regexp.MustCompile(`(?is)^CREATE\s+` + orReplace + `(` + namedObjectKind + `)\b`)
	guardedCreate = regexp.MustCompile(`(?is)\bCREATE\s+` + orReplace + indexModifiers + tableModifiers + `(?:` + anyObjectKind + `)\b`)
	collapseGaps  = regexp.MustCompile(`\s+`)
)

// EmissionsOf reads what a render's statements create.
//
// It takes the statement slice the renderer returns rather than the joined
// text, because that is what the shipping path hands its caller; joining and
// re-splitting would make this measure a rendering of the render.
//
// Each element may carry a leading comment and more than one statement, so the
// comments go first and the split is on the statement terminator. A `;` inside
// a quoted literal would split wrongly, and the corpus produces none -- which
// is a property of the corpus rather than of SQL, so the classification test
// asserts the unclassified set rather than trusting this.
func EmissionsOf(statements []string) Emissions {
	var found Emissions
	for _, statement := range statements {
		for _, one := range splitStatements(statement) {
			found.take(one)
		}
	}
	return found
}

// take classifies one statement.
func (e *Emissions) take(statement string) {
	statement = unwrapGuard(statement)
	if match := createTable.FindStringSubmatch(statement); match != nil {
		table := normalizeName(match[1])
		e.Objects = append(e.Objects, Emission{Kind: "table", Name: table})
		// A named constraint written inside the CREATE TABLE body is the same
		// physical object an ALTER TABLE ... ADD CONSTRAINT would create, and
		// emitting both is exactly the duplicate this guard is for. Reading
		// only the leading clause would see the table and miss it.
		for _, inline := range inlineNamed.FindAllStringSubmatch(statement, -1) {
			e.Objects = append(e.Objects,
				Emission{Kind: "constraint", Name: table + "." + normalizeName(inline[1])})
		}
		return
	}
	if match := addNamed.FindStringSubmatch(statement); match != nil {
		e.Objects = append(e.Objects, Emission{
			Kind: "constraint",
			Name: normalizeName(match[1]) + "." + normalizeName(match[2]),
		})
		return
	}
	if match := createIndex.FindStringSubmatch(statement); match != nil {
		e.Objects = append(e.Objects, Emission{Kind: "index", Name: normalizeName(match[1])})
		return
	}
	if match := createOther.FindStringSubmatch(statement); match != nil {
		kind := createKind.FindStringSubmatch(statement)
		e.Objects = append(e.Objects, Emission{
			Kind: strings.ToLower(collapseGaps.ReplaceAllString(kind[1], " ")),
			Name: normalizeName(match[1]),
		})
		return
	}
	e.Unclassified = append(e.Unclassified, leadingClause(statement))
}

// Duplicates reports every object created more than once, in the order it was
// first created, with the count.
//
// A map would answer the same question and in a different order on every run,
// which is what makes a failure message impossible to compare between two runs
// of the same corpus.
func (e Emissions) Duplicates() []string {
	counts := make(map[Emission]int, len(e.Objects))
	order := make([]Emission, 0, len(e.Objects))
	for _, object := range e.Objects {
		if counts[object] == 0 {
			order = append(order, object)
		}
		counts[object]++
	}
	duplicated := make([]string, 0)
	for _, object := range order {
		if counts[object] < 2 {
			continue
		}
		duplicated = append(duplicated,
			object.Kind+" "+object.Name+" emitted "+itoa(counts[object])+" times")
	}
	return duplicated
}

// itoa keeps the message building free of a strconv import for one call.
func itoa(n int) string {
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	if digits == "" {
		return "0"
	}
	return digits
}

// unwrapGuard removes an existence check standing in front of a creation.
//
// SQL Server renders an index as
// `IF NOT EXISTS (SELECT 1 FROM sys.indexes ...)` followed by the CREATE on its
// own line, so a classifier reading the leading clause sees a conditional and
// records no object at all. Every SQL Server index in the corpus was invisible
// to this guard, which is the shape a blind spot takes: a clean report over
// statements nothing looked at.
//
// SQL Server also writes a schema as `IF SCHEMA_ID('app') IS NULL
// EXEC('CREATE SCHEMA [app]')`, where the creation is inside a string passed to
// EXEC and begins no line of its own. So this finds where the creation begins
// rather than where the condition ends, and it looks anywhere in the statement.
//
// What keeps that from firing on the condition is that the pattern requires
// CREATE to be followed by an object keyword. A catalog query naming
// `sys.indexes` or `sys.sequences` carries no such pair, and matching a bare
// CREATE would have been a match on any catalog column whose name contained
// the word. The guard's own parentheses nest -- `OBJECT_ID('t')` -- and Go's
// regexp cannot balance them, which is why the condition is not parsed at all.
//
// A conditional with no creation in it keeps its original text and stays
// unclassified, which is the honest answer for a statement that creates
// nothing.
func unwrapGuard(statement string) string {
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "IF ") {
		return statement
	}
	where := guardedCreate.FindStringIndex(statement)
	if where == nil {
		return statement
	}
	return strings.TrimSpace(statement[where[0]:])
}

// splitStatements strips comments and splits on the terminator.
func splitStatements(block string) []string {
	stripped := commentLine.ReplaceAllString(block, "")
	parts := strings.Split(stripped, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		statements = append(statements, trimmed)
	}
	return statements
}

// normalizeName unquotes each part of a possibly qualified name and lower-cases
// it.
//
// Lower-cased because the corpus renders one declaration on ten dialects and
// this compares an object with itself, never two declarations with each other:
// a duplicate that differed only in case would be the same object emitted
// twice, which is the finding.
func normalizeName(name string) string {
	parts := strings.Split(collapseGaps.ReplaceAllString(name, ""), ".")
	for index, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, "\"`")
		part = strings.TrimPrefix(part, "[")
		part = strings.TrimSuffix(part, "]")
		parts[index] = strings.ToLower(part)
	}
	return strings.Join(parts, ".")
}

// leadingClause is how an unclassified statement is reported: its first few
// words, so a list of them names shapes rather than reprinting a schema.
func leadingClause(statement string) string {
	words := strings.Fields(collapseGaps.ReplaceAllString(statement, " "))
	if len(words) > 3 {
		words = words[:3]
	}
	return strings.ToUpper(strings.Join(words, " "))
}

// CorpusEmissions is what the whole corpus creates on every declared cell.
//
// The four fields answer four different questions and none of them stands in
// for another. Duplicates is the invariant. Unclassified is the guard's own
// blind spot, reported so it is asserted against a written list rather than
// growing in silence. Objects is the floor: a corpus that rendered nothing, or
// an extractor that recognized nothing, produces no duplicates either, and
// without a count that answer is indistinguishable from the invariant holding.
//
// Dark is the floor one level down, and the total cannot stand in for it.
// Measured on the corpus this was written against: PostgreSQL contributes 1068
// of 3986 objects, and every other dialect contributes fewer than the total's
// slack -- so eight of the ten could stop rendering ENTIRELY with the total
// still above any floor worth writing, and with no duplicate reported because
// nothing was measured.
//
// It carries names rather than a count because the question is which target
// went quiet, and because a count would be one more number to edit when a
// dialect is added.
type CorpusEmissions struct {
	Objects      int
	Duplicates   []string
	Unclassified []string
	Dark         []string
}

// MeasureEmissions renders every fixture on every declared cell and reports
// what each render creates.
//
// A cell that refuses a fixture contributes nothing. That is not a hole: a
// refusal creates no object, so there is nothing for the invariant to be about,
// and counting it would make the floor below depend on which targets happen to
// accept which fixture.
func MeasureEmissions() CorpusEmissions {
	measured := CorpusEmissions{
		Duplicates: make([]string, 0), Unclassified: make([]string, 0), Dark: make([]string, 0),
	}
	shapes := make(map[string]bool)
	// Seeded from the declared cells rather than from a list, so a dialect is
	// watched by being declared and a dialect that leaves takes no edit here.
	byDialect := make(map[string]int, len(capabilityprobe.Cells))
	for _, cell := range capabilityprobe.Cells {
		byDialect[cell.Dialect] += 0
	}
	for _, fixture := range Fixtures() {
		for _, cell := range capabilityprobe.Cells {
			statements, err := RenderStatements(fixture.Schema, cell)
			if err != nil {
				continue
			}
			emitted := EmissionsOf(statements)
			measured.Objects += len(emitted.Objects)
			byDialect[cell.Dialect] += len(emitted.Objects)
			for _, duplicate := range emitted.Duplicates() {
				measured.Duplicates = append(measured.Duplicates,
					fixture.Name+" / "+CellName(cell)+": "+duplicate)
			}
			for _, shape := range emitted.Unclassified {
				shapes[shape] = true
			}
		}
	}
	for dialect, objects := range byDialect {
		if objects == 0 {
			measured.Dark = append(measured.Dark, dialect)
		}
	}
	for shape := range shapes {
		measured.Unclassified = append(measured.Unclassified, shape)
	}
	sort.Strings(measured.Unclassified)
	sort.Strings(measured.Duplicates)
	sort.Strings(measured.Dark)
	return measured
}
