// Package sqlqualifier recognizes and rewrites database names in SQL. Catalog
// normalization and dev database rehearsal share its recognition so an alias
// cannot be removed in one path and treated as a database in another.
package sqlqualifier

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/sqlreach"
)

// Rewrite replaces every schema name spelled `from` with `to`,
// quoted for dialect. Only bare and dialect-quoted identifiers are rewritten;
// other spellings remain for the caller to validate.
func Rewrite(statement, dialect, from, to string) string {
	if from == "" || from == to {
		return statement
	}
	tokens := sqlreach.SignificantTokens(statement, sqlreach.DialectUsesBackslashEscapes(dialect), dialect)
	positions := Positions(tokens)
	replacement := sqlident.Quote(dialect, to)
	// Splice from the end so earlier offsets stay valid.
	for _, position := range slices.Backward(positions) {
		token := tokens[position]
		if token.Type != lexer.TokenIdentifier || Name(token) != from {
			continue
		}
		statement = statement[:token.Start] + replacement + statement[token.End:]
	}
	return statement
}

// Positions returns the token indexes that name a schema:
//
//   - a qualifier, the head of a dotted object reference (`db`.`users`);
//   - the operand of USE, and of CREATE/DROP/ALTER SCHEMA|DATABASE, which name
//     a database directly rather than through a dot.
//
// The second form is restricted to the statement's leading keywords so that an
// ordinary column named `database` — legal and unreserved in MySQL — is not
// mistaken for one. Inside a view or trigger body, a name the body binds in
// front of a column -- an alias, a table it reads, a trigger's NEW or OLD -- is
// not a qualifier either; see [columnQualifiers].
func Positions(tokens []lexer.Token) []int {
	var positions []int
	columns := columnQualifiers(tokens)
	for i := range tokens {
		if !isSchemaNameToken(tokens[i]) || columns[i] {
			continue
		}
		if i+1 >= len(tokens) || !tokens[i+1].MatchOperatorValue(".") {
			continue
		}
		// The tail of a longer dotted path names an object, not a schema.
		if i > 0 && tokens[i-1].MatchOperatorValue(".") {
			continue
		}
		positions = append(positions, i)
	}
	if operand, ok := schemaOperandPosition(tokens); ok {
		positions = append(positions, operand)
	}
	slices.Sort(positions)
	return slices.Compact(positions)
}

// QueryRelations returns the token indexes naming sources in SELECT queries,
// including queries under a CREATE VIEW header. It excludes aliases and
// expressions, such as ARRAY JOIN operands and FROM inside EXTRACT or TRIM.
// The returned map is new and tokens are unchanged.
func QueryRelations(tokens []lexer.Token) map[int]bool {
	positions, _ := readTables(tokens, 0, true)
	return positions
}

// schemaOperandPosition finds the database named directly by a statement's
// leading keywords: `USE <db>` and `CREATE|DROP|ALTER SCHEMA|DATABASE [IF [NOT]
// EXISTS] <db>`. USE matters most: a plan that switches the session's database
// would silently move every later statement out of the dev database.
func schemaOperandPosition(tokens []lexer.Token) (int, bool) {
	if len(tokens) >= 2 && sqlreach.IsKeyword(tokens[0], "USE") && isSchemaNameToken(tokens[1]) {
		return 1, true
	}
	if len(tokens) < 3 {
		return 0, false
	}
	if !sqlreach.IsKeyword(tokens[0], "CREATE") && !sqlreach.IsKeyword(tokens[0], "DROP") && !sqlreach.IsKeyword(tokens[0], "ALTER") {
		return 0, false
	}
	if !sqlreach.IsKeyword(tokens[1], "SCHEMA") && !sqlreach.IsKeyword(tokens[1], "DATABASE") {
		return 0, false
	}
	position := 2
	for position < len(tokens) && (sqlreach.IsKeyword(tokens[position], "IF") ||
		sqlreach.IsKeyword(tokens[position], "NOT") || sqlreach.IsKeyword(tokens[position], "EXISTS")) {
		position++
	}
	if position >= len(tokens) || !isSchemaNameToken(tokens[position]) {
		return 0, false
	}
	return position, true
}

// isSchemaNameToken reports whether a token can name a schema. Double-quoted
// names arrive from the lexer as strings, and they are accepted here so a
// quoted foreign schema name is refused rather than silently unseen; only bare
// and dialect-quoted identifiers are ever rewritten.
func isSchemaNameToken(token lexer.Token) bool {
	switch token.Type {
	case lexer.TokenIdentifier:
		return true
	case lexer.TokenString:
		return strings.HasPrefix(token.Value, `"`)
	default:
		return false
	}
}

// Name returns the schema name a token spells, undoubling the
// escaped delimiter the SQL standard uses inside a quoted identifier.
func Name(token lexer.Token) string {
	value := token.Value
	for _, quote := range []string{"`", `"`, "[]"} {
		open, closing := quote[:1], quote[len(quote)-1:]
		if !strings.HasPrefix(value, open) || !strings.HasSuffix(value, closing) || len(value) < 2 {
			continue
		}
		inner := value[1 : len(value)-1]
		if open == closing {
			inner = strings.ReplaceAll(inner, open+open, open)
		}
		return inner
	}
	return value
}

// ViewBody rewrites database from to database to in a stored view body. An
// empty to removes the local database qualifier. It uses the same name
// recognition as Rewrite, including its treatment of three-part expressions
// as database-qualified columns. LocalViewBody is the strict removal path for
// an engine whose tuple fields can make that interpretation ambiguous.
func ViewBody(body, dialect, from, to string) string {
	rewritten, _ := rewriteViewBody(body, dialect, from, to, nil)
	return rewritten
}

// RespellViewBody spells each database qualifier of a view body as respell
// answers for the name it spells: a qualifier respell answers with another
// name is written as that name, quoted for dialect, and one it answers
// unchanged keeps its text. It recognizes a qualifier as [ViewBody] does, so an
// alias, the table in front of a column and a name inside a string are left
// alone, and only bare and dialect-quoted identifiers are rewritten.
func RespellViewBody(body, dialect string, respell func(name string) string) string {
	const head = "CREATE VIEW v AS "
	statement := head + body
	tokens := sqlreach.SignificantTokens(statement, sqlreach.DialectUsesBackslashEscapes(dialect), dialect)
	for _, position := range slices.Backward(Positions(tokens)) {
		token := tokens[position]
		if token.Type != lexer.TokenIdentifier || token.Start < len(head) {
			continue
		}
		name := Name(token)
		if spelled := respell(name); spelled != name {
			statement = statement[:token.Start] + sqlident.Quote(dialect, spelled) + statement[token.End:]
		}
	}
	return strings.TrimPrefix(statement, head)
}

// LocalViewBody removes database from proven relation references in a stored
// view body. It keeps two-part column expressions. An expression with three
// or more parts starting with database is refused, even when an alias binds
// that name: removing its first component can change which tuple field or
// table column it reads, and keeping it can retain the source database. On
// error no body is returned. decode must read quoted identifiers as the engine
// does; its false answer refuses the read.
func LocalViewBody(body, dialect, database string, decode func(string) (string, bool)) (string, error) {
	if decode == nil {
		return "", errors.New("local view normalization requires an identifier decoder")
	}
	return rewriteViewBody(body, dialect, database, "", decode)
}

func rewriteViewBody(body, dialect, from, to string, decode func(string) (string, bool)) (string, error) {
	if from == "" || from == to {
		return body, nil
	}
	const head = "CREATE VIEW v AS "
	statement := head + body
	tokens := sqlreach.SignificantTokens(statement, sqlreach.DialectUsesBackslashEscapes(dialect), dialect)
	if err := decodeQuotedNames(tokens, decode); err != nil {
		return "", err
	}
	relations := QueryRelations(tokens)
	for _, position := range slices.Backward(Positions(tokens)) {
		token := tokens[position]
		if Name(token) != from || (decode == nil && token.Type != lexer.TokenIdentifier) {
			continue
		}
		if decode != nil {
			// A ClickHouse relation is database.table. Check the path before
			// trusting its position: an unrecognized query clause must never
			// turn a multipart column expression into a removable relation.
			if !isColumnPath(tokens, position) {
				return "", fmt.Errorf("cannot make database-qualified expression %q portable; use an explicit table alias different from the database name",
					statement[token.Start:tokens[min(position+4, len(tokens)-1)].End])
			}
			if !relations[position] {
				continue
			}
		}
		if to != "" {
			statement = statement[:token.Start] + sqlident.Quote(dialect, to) + statement[token.End:]
			continue
		}
		dot := tokens[position+1]
		// Keep the whitespace and comments between the name and its dot.
		statement = statement[:dot.Start] + statement[dot.End:]
		statement = statement[:token.Start] + statement[token.End:]
	}
	return strings.TrimPrefix(statement, head), nil
}

// decodeQuotedNames keeps offsets into the original statement while spelling
// quoted tokens with standard doubled delimiters for the shared name reader.
// The tokens remain quoted, so a quoted keyword cannot become SQL syntax.
func decodeQuotedNames(tokens []lexer.Token, decode func(string) (string, bool)) error {
	if decode == nil {
		return nil
	}
	for i, token := range tokens {
		if !isSchemaNameToken(token) || (!strings.HasPrefix(token.Value, "`") && !strings.HasPrefix(token.Value, `"`)) {
			continue
		}
		name, ok := decode(token.Value)
		if !ok {
			return fmt.Errorf("cannot decode quoted SQL identifier %q", token.Value)
		}
		tokens[i].Value = "`" + strings.ReplaceAll(name, "`", "``") + "`"
		tokens[i].Type = lexer.TokenIdentifier
	}
	return nil
}
