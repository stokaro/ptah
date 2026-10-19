package atlasreport

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// indentSQL adds presentation whitespace only outside quoted SQL tokens.
// Some report payloads have no dialect, so retain the union of quoted spans
// under the supported string-escape policies. Extra preservation costs only
// alignment; guessing a policy can change a literal's value or a routine body.
//
// The three lexers are named here, rather than read from the descriptors,
// because a SchemaDiff payload carries no dialect. Every rule that would
// derive the set from the registry changes the output: the union of every
// dialect's lexer adds YQL, which reads `@@...@@` as a string, so a T-SQL or
// MySQL body that names two `@@` variables would lose its indentation between
// them. The fix is a payload that carries its dialect and is lexed under it.
func indentSQL(sql, indent string) string {
	protected := make([]bool, len(sql))
	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.SQLServer} {
		markQuotedNewlines(sql, dialect, protected)
	}
	var output strings.Builder
	output.WriteString(indent)
	for index := range len(sql) {
		output.WriteByte(sql[index])
		if sql[index] == '\n' && !protected[index] {
			output.WriteString(indent)
		}
	}
	return output.String()
}

func markQuotedNewlines(sql, dialect string, protected []bool) {
	lex := lexer.NewLexerWithOptions(sql, dialectlexer.Options(dialect))
	for {
		token := lex.NextToken()
		if token.Type == lexer.TokenEOF {
			return
		}
		// Backtick and bracket quoting produce identifiers; dollar-quoted
		// routine bodies and double-quoted identifiers produce strings.
		if token.Type != lexer.TokenString && token.Type != lexer.TokenIdentifier {
			continue
		}
		for index := token.Start; index < token.End; index++ {
			if sql[index] == '\n' {
				protected[index] = true
			}
		}
	}
}
