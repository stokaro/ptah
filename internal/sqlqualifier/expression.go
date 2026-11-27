package sqlqualifier

import (
	"slices"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlreach"
)

// Expression takes schema off each object expression names in it: a part
// that names schema at the head of a dotted reference of two or more parts.
// It is for an expression a catalog stores, such as a column default or a
// virtual column, where the only qualified names are objects: MariaDB stores
// a default that reads a sequence as nextval(`app`.`s`), and Oracle stores
// one as "APP"."SEQ"."NEXTVAL" and a virtual column that calls a function as
// "APP"."DBL"("N"). With schema app, APP and APP, those read nextval(`s`),
// "SEQ"."NEXTVAL" and "DBL"("N").
//
// schema is the name as the catalog stores it. A quoted part names the name
// inside its quotes, and a bare part the name dialect's unquoted fold gives
// it, so on Oracle app and "APP" both name APP and "app" names another
// schema. A string literal, a part after a dot and a part no dot follows are
// kept, and an empty schema keeps the whole expression.
func Expression(expression, dialect, schema string) string {
	if schema == "" {
		return expression
	}
	tokens := sqlreach.SignificantTokens(expression, sqlreach.DialectUsesBackslashEscapes(dialect), dialect)
	semantics := identifier.ForDialect(dialect)
	var heads []int
	for i := range tokens {
		if !isSchemaNameToken(tokens[i]) || i+2 >= len(tokens) {
			continue
		}
		if !tokens[i+1].MatchOperatorValue(".") || !isSchemaNameToken(tokens[i+2]) {
			continue
		}
		if i > 0 && tokens[i-1].MatchOperatorValue(".") {
			continue
		}
		if storedPart(semantics, tokens[i]) == schema {
			heads = append(heads, i)
		}
	}
	for _, head := range slices.Backward(heads) {
		name, dot := tokens[head], tokens[head+1]
		// Keep the whitespace and comments between the name and its dot.
		expression = expression[:dot.Start] + expression[dot.End:]
		expression = expression[:name.Start] + expression[name.End:]
	}
	return expression
}

// storedPart is the name one part of a reference stores: the name inside the
// quotes of a quoted part, and the name semantics' unquoted fold gives a bare
// one.
func storedPart(semantics identifier.Semantics, token lexer.Token) string {
	if name := Name(token); name != token.Value {
		return name
	}
	return semantics.StoredName(token.Value)
}
