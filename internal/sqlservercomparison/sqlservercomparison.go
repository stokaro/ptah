// Package sqlservercomparison holds the rules SQL Server gives the shared
// schema comparison ([schemacomparison.Rules]): how an index definition, a
// filtered index's predicate and a computed column's expression read on SQL
// Server. The built-in runtime registers them on its SQL Server target, so the
// shared comparison holds no SQL Server knowledge of its own.
package sqlservercomparison

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
)

// Rules returns SQL Server's comparison rules.
func Rules() schemacomparison.Rules {
	return schemacomparison.Rules{
		IndexDefinitions:     IndexDefinitions{},
		IndexPredicates:      IndexPredicates{},
		GeneratedExpressions: GeneratedExpressions{},
	}
}

// IndexDefinitions compares SQL Server index definitions: the uniqueness, and
// each key column and its direction, in order. A UNIQUE constraint and a
// unique index are separate objects on SQL Server, so a unique index is
// compared as an index. A changed definition is a DROP INDEX and a CREATE
// INDEX.
type IndexDefinitions struct{}

// DefinitionChanged reports whether current has another uniqueness or other
// key columns than desired declares.
func (IndexDefinitions) DefinitionChanged(desired schemamodel.Index, current catalog.Index, semantics identifier.Semantics) bool {
	return desired.Unique != current.IsUnique || keyPartsChanged(desired, current, semantics)
}

// keyPartsChanged compares the key columns and their directions in order,
// under the target's column-name comparison.
func keyPartsChanged(desired schemamodel.Index, current catalog.Index, semantics identifier.Semantics) bool {
	desiredParts := declaredParts(desired)
	currentParts := reportedParts(current)
	if len(desiredParts) != len(currentParts) {
		return true
	}
	for position, part := range desiredParts {
		reported := currentParts[position]
		if semantics.ColumnIdentityKey(part.Name) != semantics.ColumnIdentityKey(reported.Name) ||
			part.Desc != reported.Desc {
			return true
		}
	}
	return false
}

// declaredParts are the key parts of a declared index, or its fields as
// ascending parts where it declares no parts.
func declaredParts(index schemamodel.Index) []schemamodel.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]schemamodel.IndexPart, len(index.Fields))
	for position, field := range index.Fields {
		parts[position] = schemamodel.IndexPart{Name: field}
	}
	return parts
}

// reportedParts are the key parts of a reported index, or its columns as
// ascending parts where the reader reported no parts.
func reportedParts(index catalog.Index) []catalog.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]catalog.IndexPart, len(index.Columns))
	for position, column := range index.Columns {
		parts[position] = catalog.IndexPart{Name: column}
	}
	return parts
}

// IndexPredicates reads the predicate of a SQL Server filtered index.
//
// SQL Server persists sys.indexes.filter_definition in a canonical spelling:
// identifiers come back bracket-quoted and numeric literals parenthesized, so
// a user-authored predicate such as "status = 1" is stored as
// "([status]=(1))". Comparing raw spellings verbatim would classify every
// such filtered index as a perpetual drop/create replacement.
type IndexPredicates struct{}

// Spelling returns predicate without the square-bracket quoting outside
// string literals, and without the parentheses around a bare numeric literal.
//
// String literal content, including doubled single-quote escapes, is
// preserved verbatim. Spellings SQL Server rewrites more aggressively (for example
// implicit N'...' prefixes or CAST insertions) are intentionally not
// reconstructed here; they compare as changed and are documented as requiring
// the catalog's canonical spelling in the annotation.
func (IndexPredicates) Spelling(predicate string) string {
	previous := predicate
	for {
		next := stripPredicateSpelling(previous)
		if next == previous {
			return next
		}
		previous = next
	}
}

func stripPredicateSpelling(expr string) string {
	var builder strings.Builder
	builder.Grow(len(expr))
	inString := false
	for pos := 0; pos < len(expr); pos++ {
		ch := expr[pos]
		if ch == '\'' {
			if inString && pos+1 < len(expr) && expr[pos+1] == '\'' {
				builder.WriteString("''")
				pos++
				continue
			}
			inString = !inString
			builder.WriteByte(ch)
			continue
		}
		if inString {
			builder.WriteByte(ch)
			continue
		}
		if ch == '[' || ch == ']' {
			continue
		}
		if ch == '(' {
			if literal, closing, ok := parenthesizedNumericLiteral(expr, pos); ok {
				builder.WriteString(literal)
				pos = closing
				continue
			}
		}
		builder.WriteByte(ch)
	}
	return builder.String()
}

// parenthesizedNumericLiteral reports whether the parenthesis opening at
// expr[open] wraps nothing but a bare numeric literal. It returns the literal
// text and the position of the closing parenthesis.
func parenthesizedNumericLiteral(expr string, open int) (literal string, closing int, ok bool) {
	pos := skipPredicateSpaces(expr, open+1)
	literalStart := pos
	if pos < len(expr) && (expr[pos] == '-' || expr[pos] == '+') {
		pos++
	}
	digits, dots := 0, 0
	for pos < len(expr) {
		ch := expr[pos]
		if ch >= '0' && ch <= '9' {
			digits++
			pos++
			continue
		}
		if ch == '.' {
			dots++
			pos++
			continue
		}
		break
	}
	literalEnd := pos
	pos = skipPredicateSpaces(expr, pos)
	if digits == 0 || dots > 1 || pos >= len(expr) || expr[pos] != ')' {
		return "", 0, false
	}
	return expr[literalStart:literalEnd], pos, true
}

func skipPredicateSpaces(expr string, pos int) int {
	for pos < len(expr) && (expr[pos] == ' ' || expr[pos] == '\t' || expr[pos] == '\n' || expr[pos] == '\r') {
		pos++
	}
	return pos
}

// GeneratedExpressions reads the expression of a SQL Server computed column.
//
// The catalog prints the expression back with every name in square brackets
// and its own spacing and case, so both sides are reduced to the expression
// with the brackets, the spacing and the case of everything outside string
// literals removed.
type GeneratedExpressions struct{}

// Spelling returns expression with ASCII letters lowered and whitespace
// dropped outside string literals, and each bracketed name written as its
// lowered content. A string literal, its doubled quotes included, is copied as
// written. An unclosed bracket is kept with the rest of the expression.
func (GeneratedExpressions) Spelling(expression string) string {
	var b strings.Builder
	inString := false
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if ch == '\'' {
			b.WriteByte(ch)
			if inString && i+1 < len(expression) && expression[i+1] == '\'' {
				i++
				b.WriteByte('\'')
				continue
			}
			inString = !inString
			continue
		}
		if !inString && ch == '[' {
			i = writeBracketedIdentifier(&b, expression, i)
			continue
		}
		if !inString && ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		if !inString && (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') {
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

// writeBracketedIdentifier writes the lowered content of the bracketed name
// that opens at expression[start] and returns the position of its closing
// bracket. A doubled closing bracket is one bracket of the name. A name that
// is never closed writes its content and an opening bracket, and returns
// start, so the rest of the expression is read again after it.
func writeBracketedIdentifier(b *strings.Builder, expression string, start int) int {
	for i := start + 1; i < len(expression); i++ {
		ch := expression[i]
		if ch == ']' {
			if i+1 < len(expression) && expression[i+1] == ']' {
				b.WriteByte(']')
				i++
				continue
			}
			return i
		}
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		b.WriteByte(ch)
	}

	b.WriteByte('[')
	return start
}
