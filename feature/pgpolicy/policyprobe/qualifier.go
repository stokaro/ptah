package policyprobe

import (
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/internal/lexer"
)

// significantTokens reads clause into its tokens without whitespace and
// comments, each with its offsets in clause.
func significantTokens(clause string) []lexer.Token {
	var tokens []lexer.Token
	scanner := lexer.NewLexerWithOptions(clause, lexer.Options{StandardStrings: true})
	for token := scanner.NextToken(); token.Type != lexer.TokenEOF; token = scanner.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// ownSchemaQualifier reports whether the token at i is the schema of the
// policy's own table, written in front of that table and a column: the
// `public` of `public.notes.owner` in a policy on public.notes. PostgreSQL
// reads the reference as the policy's table and stores it as `owner`.
//
// The probe and the spelling comparison both take the qualifier out, and they
// share this predicate because they have to agree on the set. The probe's copy
// of the table sits in pg_temp, where the server refuses `public.notes.owner`
// as an invalid reference to a FROM-clause entry, so a probe sending it
// answers nothing. A spelling comparison that kept it would then read the
// declaration as a different clause and plan ALTER POLICY on every run.
//
// A qualifier that is itself qualified, as in `db.public.notes.owner`, is not
// this one and stays.
func ownSchemaQualifier(tokens []lexer.Token, i int, ref objectidentity.ID) bool {
	return (i == 0 || !startsOperator(tokens, i-1, ".")) &&
		namesPart(tokens[i], ref.Schema) && startsOperator(tokens, i+1, ".") &&
		i+2 < len(tokens) && namesPart(tokens[i+2], ref.Parent) && startsOperator(tokens, i+3, ".")
}

// probeClause is clause with each [ownSchemaQualifier] taken out, and the
// rest byte for byte as declared. A nil clause stays nil.
func probeClause(clause *string, ref objectidentity.ID) *string {
	if clause == nil {
		return nil
	}
	tokens := significantTokens(*clause)
	var rewritten strings.Builder
	written := 0
	for i := range tokens {
		if ownSchemaQualifier(tokens, i, ref) {
			rewritten.WriteString((*clause)[written:tokens[i].Start])
			written = tokens[i+2].Start
		}
	}
	if written == 0 {
		return clause
	}
	rewritten.WriteString((*clause)[written:])
	return new(rewritten.String())
}
