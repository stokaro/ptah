// Package tableref parses and formats structural table and column references,
// and names a table in a message.
package tableref

import (
	"fmt"
	"strings"
)

// Ref is a structural table reference.
type Ref struct {
	Schema    string
	Name      string
	Qualified bool
}

// Canonical formats schema and name so that [Parse] reads back the same
// names. A name is written as it is unless a bare one would read back as
// something else: a name that holds a dot, a quote, a backtick or a bracket, or
// that begins or ends with white space, is double-quoted, each double quote
// inside doubled. The bytes of each name are kept, so `" a".t` and `a.t` name
// tables of two schemas. An empty schema writes the name alone.
func Canonical(schema, name string) string {
	if schema == "" {
		return canonicalPart(name)
	}
	return canonicalPart(schema) + "." + canonicalPart(name)
}

// CanonicalExact formats schema and name without normalizing their contents.
// It is for already parsed identifiers whose leading or trailing spaces are
// significant because the source quoted them.
func CanonicalExact(schema, name string) string {
	if schema == "" {
		return canonicalExactPart(name)
	}
	return canonicalExactPart(schema) + "." + canonicalExactPart(name)
}

// Phrase is how a message names table: `table "orders"`, or `a table` when
// the name is empty.
//
// An alter operation that reaches a renderer without its ALTER TABLE is
// checked against an ALTER TABLE that names no table, and the refusal that
// check returns is the one the caller sees. Written through Phrase, that
// refusal reads `changing the changefeeds of a table` rather than naming a
// table called "".
func Phrase(table string) string {
	if table == "" {
		return "a table"
	}
	return fmt.Sprintf("table %q", table)
}

// Parse parses an unqualified or schema-qualified SQL identifier reference.
// It accepts SQL-standard quotes, MySQL backticks, and SQL Server brackets.
func Parse(value string) (Ref, bool) {
	parts, ok := split(strings.TrimSpace(value))
	if !ok || len(parts) == 0 || len(parts) > 2 {
		return Ref{}, false
	}
	if len(parts) == 1 {
		return Ref{Name: parts[0]}, parts[0] != ""
	}
	return Ref{
		Schema:    parts[0],
		Name:      parts[1],
		Qualified: true,
	}, parts[0] != "" && parts[1] != ""
}

// ColumnRef is a structural column reference, as [ParseColumn] reads one.
type ColumnRef struct {
	Schema string // empty when the reference names no schema
	Table  string
	Column string
}

// Column formats a column reference, table.column, or schema.table.column when
// schema is not empty, so that [ParseColumn] reads back the same names. A name
// is written as it is unless a bare one would read back as something else: a
// name that holds a dot or a double quote, begins with a backtick or a bracket,
// or begins or ends with white space is double-quoted, each double quote inside
// doubled. So `"a.b".id` is the column id of a table a.b, and `a."x.y"` the
// column x.y of a table a.
func Column(schema, table, column string) string {
	reference := columnPart(table) + "." + columnPart(column)
	if schema == "" {
		return reference
	}
	return columnPart(schema) + "." + reference
}

// ParseColumn parses a column reference as [Column] writes it, table.column or
// schema.table.column, accepting the quotes [Parse] accepts. It reports false
// for a reference of one name or of more than three, and for an empty name.
func ParseColumn(value string) (ColumnRef, bool) {
	parts, ok := split(strings.TrimSpace(value))
	switch {
	case !ok:
		return ColumnRef{}, false
	case len(parts) == 2:
		return ColumnRef{Table: parts[0], Column: parts[1]}, true
	case len(parts) == 3:
		return ColumnRef{Schema: parts[0], Table: parts[1], Column: parts[2]}, true
	default:
		return ColumnRef{}, false
	}
}

// columnPart is one name of a [Column] reference: as it is, unless [split]
// would read the bare name back as another one.
func columnPart(value string) string {
	if value == strings.TrimSpace(value) && !strings.ContainsAny(value, `."`) &&
		!strings.HasPrefix(value, "`") && !strings.HasPrefix(value, "[") {
		return value
	}
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func canonicalPart(value string) string {
	if value == strings.TrimSpace(value) && !strings.ContainsAny(value, ".\"`[]") {
		return value
	}
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func canonicalExactPart(value string) string {
	if postgresUnquotedIdentifier(value) {
		return canonicalPart(value)
	}
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func postgresUnquotedIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for i := range len(value) {
		character := value[i]
		if character >= 'a' && character <= 'z' || character == '_' ||
			i > 0 && (character >= '0' && character <= '9' || character == '$') {
			continue
		}
		return false
	}
	return true
}

func split(value string) ([]string, bool) {
	parser := referenceParser{parts: make([]string, 0, 2)}
	for position := 0; position < len(value); position++ {
		if !parser.consume(value, &position) {
			return nil, false
		}
	}
	return parser.result()
}

type referenceParser struct {
	parts            []string
	part             strings.Builder
	quote            byte
	partQuoted       bool
	quotedPartClosed bool
}

func (p *referenceParser) consume(value string, position *int) bool {
	if p.quote != 0 {
		return p.consumeQuoted(value, position)
	}
	return p.consumeUnquoted(value[*position])
}

func (p *referenceParser) consumeUnquoted(character byte) bool {
	if p.quotedPartClosed && character != '.' {
		return isReferenceSpace(character)
	}
	switch character {
	case '"', '`', '[':
		if strings.TrimSpace(p.part.String()) != "" {
			p.part.WriteByte(character)
			return true
		}
		p.part.Reset()
		p.quote = character
		p.partQuoted = true
	case '.':
		if !p.appendPart() {
			return false
		}
		p.part.Reset()
		p.partQuoted = false
		p.quotedPartClosed = false
	default:
		p.part.WriteByte(character)
	}
	return true
}

func (p *referenceParser) consumeQuoted(value string, position *int) bool {
	character := value[*position]
	closeQuote := p.quote
	if p.quote == '[' {
		closeQuote = ']'
	}
	if character != closeQuote {
		p.part.WriteByte(character)
		return true
	}
	if *position+1 < len(value) && value[*position+1] == closeQuote {
		p.part.WriteByte(closeQuote)
		(*position)++
		return true
	}
	p.quote = 0
	p.quotedPartClosed = true
	return true
}

func (p *referenceParser) result() ([]string, bool) {
	if p.quote != 0 || !p.appendPart() {
		return nil, false
	}
	return p.parts, true
}

func (p *referenceParser) appendPart() bool {
	value := p.part.String()
	if !p.partQuoted {
		value = strings.TrimSpace(value)
	}
	if value == "" {
		return false
	}
	p.parts = append(p.parts, value)
	return true
}

func isReferenceSpace(character byte) bool {
	return character == ' ' || character == '\t' || character == '\r' || character == '\n'
}
