package roundingscan

import (
	"fmt"
	"strings"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/parser"
	"ptah.run/internal/tableref"
)

// CheckStatements refuses a rounding record unless an ALTER TABLE in statements
// changes its column to its declared type. Every ALTER of that column must agree,
// so a matching statement cannot hide another conversion. Names and types come
// from the dialect's parser; a mention in a default, comment or another operation
// establishes no conversion. Type keywords ignore case and whitespace, while
// quoted type arguments keep their bytes. A statement that may name the change
// but cannot be parsed is refused. An empty statement list is refused too.
func CheckStatements(dialect string, statements []string, change Change) error {
	target, err := StatementType(dialect, statements, change)
	if err != nil {
		return err
	}
	if typeTokens(dialect, target) != typeTokens(dialect, change.DeclaredType) {
		return fmt.Errorf("%s declared type %q differs from the statement's type %q; generate and review a new plan",
			column(change), change.DeclaredType, target)
	}
	return nil
}

// StatementType returns the type the parsed ALTER TABLE statements apply to
// change's table and column. Multiple conversions must name the same type.
// Missing, conflicting or unparseable conversions return an error. Plan
// generation uses this spelling for its scan, since a comparison can resolve
// an alias such as INT to int8 while the rendered statement keeps INT.
func StatementType(dialect string, statements []string, change Change) (string, error) {
	target := ""
	for _, statement := range statements {
		if !Names(dialect, statement, change) {
			continue
		}
		changes, err := parser.ReadColumnTypeChanges(statement, parser.WithDialect(dialect))
		if err != nil {
			return "", fmt.Errorf("cannot read the type applied to %s: %w", column(change), err)
		}
		for _, converted := range changes {
			name, ok := statementName(dialect, lexer.Token{Type: lexer.TokenIdentifier, Value: converted.Column})
			if !ok || name != change.Column || !sameTable(dialect, converted.Table, change.Table) {
				continue
			}
			if target != "" && typeTokens(dialect, converted.Type) != typeTokens(dialect, target) {
				return "", fmt.Errorf("statements apply different types %q and %q to %s", target, converted.Type, column(change))
			}
			target = converted.Type
		}
	}
	if target == "" {
		return "", fmt.Errorf("no statement of the plan changes the type of column %q of table %q", change.Column, change.Table)
	}
	return target, nil
}

// sameTable compares parsed names without folding their case: a record names the
// catalog object, not another spelling that only some collations equate. A
// qualifier omitted on one side is resolved by the saved plan's target scope.
func sameTable(dialect, statement, recorded string) bool {
	left, leftOK := statementTable(dialect, statement)
	right, rightOK := tableref.Parse(recorded)
	return leftOK && rightOK && left.Name == right.Name &&
		(!left.Qualified || !right.Qualified || left.Schema == right.Schema)
}

func statementTable(dialect, written string) (tableref.Ref, bool) {
	var parts []string
	lex := lexer.NewLexerWithOptions(written, dialectlexer.Options(dialect))
	for token := lex.NextToken(); token.Type != lexer.TokenEOF; token = lex.NextToken() {
		if name, ok := statementName(dialect, token); ok {
			parts = append(parts, name)
		}
	}
	switch len(parts) {
	case 1:
		return tableref.Ref{Name: parts[0]}, true
	case 2:
		return tableref.Ref{Schema: parts[0], Name: parts[1], Qualified: true}, true
	default:
		return tableref.Ref{}, false
	}
}

// typeTokens removes only formatting. In particular, it keeps precision, scale,
// signedness, zone qualifiers and the case of a quoted time-zone argument.
func typeTokens(dialect, typ string) string {
	var normalized strings.Builder
	lex := lexer.NewLexerWithOptions(typ, dialectlexer.Options(dialect))
	for token := lex.NextToken(); token.Type != lexer.TokenEOF; token = lex.NextToken() {
		if token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment {
			continue
		}
		value := token.Value
		if token.Type == lexer.TokenIdentifier {
			value = strings.ToLower(value)
		}
		normalized.WriteString(value)
		normalized.WriteByte(0)
	}
	return normalized.String()
}
