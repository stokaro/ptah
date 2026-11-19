// Package sequenceref reads the references a column default makes to a
// sequence, through a pseudocolumn such as Oracle's seq.NEXTVAL or a clause
// such as SQL Server's NEXT VALUE FOR seq, so that a declared default and the
// one a catalog stores compare by the sequence they read.
//
// Oracle stores an unqualified seq.NEXTVAL qualified and quoted, as
// "APP"."SEQ"."NEXTVAL", and keeps a qualified one as written. SQL Server
// stores NEXT VALUE FOR dbo.seq as NEXT VALUE FOR [dbo].[seq]. Compared as
// text, every declared reference differs from the catalog's on every run. The
// pseudocolumns and the clause a dialect has come from its descriptor,
// [descriptor.ColumnGrammar.SequencePseudocolumns] and
// [descriptor.ColumnGrammar.NextValueClause]; the parser reads the same
// fields to keep such a default an expression.
package sequenceref

import (
	"slices"
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
)

// Rules are what a dialect states about sequence references.
type Rules struct {
	// Pseudocolumns names, in upper case, the pseudocolumns a reference may
	// end in.
	Pseudocolumns []string
	// NextValue is the words, in upper case, a reference may start with,
	// before the sequence's name.
	NextValue []string
	// Fold is the case the dialect stores an unquoted name in.
	Fold identifier.Fold
	// Lexing reads the expression as the dialect writes it.
	Lexing lexer.Options
}

// Canonical returns expression with each sequence reference written as the
// names the catalog stores, each double-quoted and joined by dots, without a
// schema equal to schema, and found reports whether expression holds one.
// The other tokens are kept as written, separated by one space, so the spaces
// a server drops from what it stores do not count.
//
// A reference is a sequence name, optionally after a schema and a dot, then a
// dot and a pseudocolumn of rules, each part bare or double-quoted and
// written without spaces. A bare part is stored in rules.Fold's case and a
// quoted one as written inside its quotes, so `seq.NEXTVAL`,
// `"APP"."SEQ"."NEXTVAL"` in schema APP and `app.seq.nextval` are one
// reference, and `"Seq".NEXTVAL` is another. An argument list after the words
// makes them a call, and a string literal is never a reference, so the default
// 'seq.NEXTVAL' finds none.
//
// A reference may also be the words of rules.NextValue, in any case, then the
// sequence name, optionally after a schema and a dot, each part bare,
// bracketed or double-quoted. The words are written in upper case and the
// schema is kept as written, as SQL Server keeps it: `next value for seq` and
// `NEXT VALUE FOR [seq]` are one reference, and `NEXT VALUE FOR dbo.seq` is
// another.
func Canonical(expression string, rules Rules, schema string) (canonical string, found bool) {
	if len(rules.Pseudocolumns) == 0 && len(rules.NextValue) == 0 {
		return expression, false
	}
	tokens := significantTokens(expression, rules.Lexing)
	words := make([]string, 0, len(tokens))
	for index := 0; index < len(tokens); index++ {
		if clause, parts := nextValueAt(tokens, index, rules.NextValue); parts > 0 {
			found = true
			name := tokens[index+len(clause) : index+len(clause)+2*parts-1]
			words = append(words, strings.Join(rules.NextValue, " "), storedReference(name, rules.Fold, ""))
			index += len(clause) + 2*parts - 2
			continue
		}
		parts := referenceAt(tokens, index, rules.Pseudocolumns)
		if parts == 0 {
			words = append(words, tokens[index].Value)
			continue
		}
		found = true
		words = append(words, storedReference(tokens[index:index+2*parts-1], rules.Fold, schema))
		index += 2*parts - 2
	}
	return strings.Join(words, " "), found
}

// nextValueAt returns the tokens of the clause words starting at
// tokens[index] and how many name parts follow them, and 0 parts where no
// reference through the clause starts there.
func nextValueAt(tokens []lexer.Token, index int, clause []string) ([]lexer.Token, int) {
	if len(clause) == 0 || index+len(clause) >= len(tokens) {
		return nil, 0
	}
	words := tokens[index : index+len(clause)]
	for position, word := range words {
		if word.Type != lexer.TokenIdentifier || !strings.EqualFold(word.Value, clause[position]) {
			return nil, 0
		}
	}
	name := index + len(clause)
	if !isNamePart(tokens[name]) {
		return nil, 0
	}
	if name+2 < len(tokens) && adjacentDot(tokens, name) && isNamePart(tokens[name+2]) {
		return words, 2
	}
	return words, 1
}

// significantTokens reads expression into its tokens, without whitespace and
// comments.
func significantTokens(expression string, lexing lexer.Options) []lexer.Token {
	scan := lexer.NewLexerWithOptions(expression, lexing)
	var tokens []lexer.Token
	for {
		tok := scan.NextToken()
		switch tok.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
		default:
			tokens = append(tokens, tok)
		}
	}
}

// referenceAt returns how many name parts the reference starting at
// tokens[index] has, and 0 where none starts there.
func referenceAt(tokens []lexer.Token, index int, pseudocolumns []string) int {
	parts := 1
	end := index
	for end+2 < len(tokens) && parts < 3 && adjacentDot(tokens, end) && isNamePart(tokens[end+2]) {
		parts++
		end += 2
	}
	if !isNamePart(tokens[index]) || parts < 2 {
		return 0
	}
	if end+1 < len(tokens) && tokens[end+1].MatchOperatorValue("(") {
		return 0
	}
	if !slices.Contains(pseudocolumns, storedPart(tokens[end].Value, identifier.FoldUpper)) {
		return 0
	}
	return parts
}

// adjacentDot reports whether a dot follows tokens[index] and the next part
// follows the dot, each with no space between.
func adjacentDot(tokens []lexer.Token, index int) bool {
	dot, next := tokens[index+1], tokens[index+2]
	return dot.MatchOperatorValue(".") && dot.Start == tokens[index].End && next.Start == dot.End
}

// isNamePart reports whether tok is a bare or a double-quoted name.
func isNamePart(tok lexer.Token) bool {
	return tok.Type == lexer.TokenIdentifier ||
		(tok.Type == lexer.TokenString && strings.HasPrefix(tok.Value, `"`))
}

// storedReference writes the reference tokens spell, every second token a
// dot, as its stored names, each quoted, leaving out a schema equal to schema.
func storedReference(tokens []lexer.Token, fold identifier.Fold, schema string) string {
	names := make([]string, 0, (len(tokens)+1)/2)
	for index := 0; index < len(tokens); index += 2 {
		names = append(names, storedPart(tokens[index].Value, fold))
	}
	if len(names) == 3 && names[0] == schema {
		names = names[1:]
	}
	quoted := make([]string, len(names))
	for index, name := range names {
		quoted[index] = `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
	return strings.Join(quoted, ".")
}

// storedPart is the name the catalog stores for one part: the text inside
// the quotes of a double-quoted part or the brackets of a bracketed one, and
// a bare part in fold's case.
func storedPart(part string, fold identifier.Fold) string {
	switch {
	case strings.HasPrefix(part, `"`):
		return sqlident.UnquoteDoubleQuoted(part)
	case strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]"):
		return strings.ReplaceAll(part[1:len(part)-1], "]]", "]")
	default:
		return fold.Apply(part)
	}
}
