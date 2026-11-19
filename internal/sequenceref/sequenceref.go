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
	// Key answers the name a stored schema or sequence name compares by, such
	// as the equivalence class a server's collation puts it in, so two
	// spellings of one sequence read alike. Nil compares the stored names as
	// they are.
	Key func(name string) string
	// Lexing reads the expression as the dialect writes it.
	Lexing lexer.Options
}

// segment is one part of an expression as [scan] reads it: a token kept as
// written, or a sequence reference, with its name parts as the catalog stores
// them.
type segment struct {
	// text is the token, for a segment that is not a reference.
	text string
	// names are the stored schema and sequence names of a reference, the
	// pseudocolumn included where it ends in one.
	names []string
	// clause reports a reference through rules.NextValue.
	clause bool
	// reference reports that the segment is a reference.
	reference bool
}

// scan reads expression into its segments. [Canonical] writes them and
// [Names] lists their names, so the names a caller asks a server about are
// the ones the comparison keys.
func scan(expression string, rules Rules) []segment {
	tokens := significantTokens(expression, rules.Lexing)
	segments := make([]segment, 0, len(tokens))
	for index := 0; index < len(tokens); index++ {
		if clause, parts := nextValueAt(tokens, index, rules.NextValue); parts > 0 {
			name := tokens[index+len(clause) : index+len(clause)+2*parts-1]
			segments = append(segments, segment{names: storedNames(name, rules.Fold), clause: true, reference: true})
			index += len(clause) + 2*parts - 2
			continue
		}
		parts := referenceAt(tokens, index, rules.Pseudocolumns)
		if parts == 0 {
			segments = append(segments, segment{text: tokens[index].Value})
			continue
		}
		segments = append(segments, segment{names: storedNames(tokens[index:index+2*parts-1], rules.Fold), reference: true})
		index += 2*parts - 2
	}
	return segments
}

// Names returns the stored schema and sequence names each sequence reference
// in expression spells, in the order written, without a pseudocolumn. A
// caller that keys names through rules.Key asks its server about these.
func Names(expression string, rules Rules) []string {
	if len(rules.Pseudocolumns) == 0 && len(rules.NextValue) == 0 {
		return nil
	}
	var names []string
	for _, part := range scan(expression, rules) {
		switch {
		case !part.reference:
		case part.clause:
			names = append(names, part.names...)
		default:
			names = append(names, part.names[:len(part.names)-1]...)
		}
	}
	return names
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
//
// Where rules.Key is set, each schema and sequence name is written as its
// key, so under a key that folds case `NEXT VALUE FOR S` and `NEXT VALUE FOR
// [s]` are one reference too.
func Canonical(expression string, rules Rules, schema string) (canonical string, found bool) {
	if len(rules.Pseudocolumns) == 0 && len(rules.NextValue) == 0 {
		return expression, false
	}
	segments := scan(expression, rules)
	words := make([]string, 0, len(segments))
	for _, part := range segments {
		switch {
		case !part.reference:
			words = append(words, part.text)
		case part.clause:
			found = true
			words = append(words, strings.Join(rules.NextValue, " "), storedReference(part, rules.Key, schema))
		default:
			found = true
			words = append(words, storedReference(part, rules.Key, schema))
		}
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

// storedNames reads the reference tokens spell, every second token a dot, as
// its stored names.
func storedNames(tokens []lexer.Token, fold identifier.Fold) []string {
	names := make([]string, 0, (len(tokens)+1)/2)
	for index := 0; index < len(tokens); index += 2 {
		names = append(names, storedPart(tokens[index].Value, fold))
	}
	return names
}

// storedReference writes the stored names of a reference, each quoted and
// keyed by key where it is set, leaving out a schema equal to schema. The
// last name of a reference through a pseudocolumn is the pseudocolumn, which
// is written as it is stored. A reference through the clause has at most two
// names, so it keeps its schema, as SQL Server keeps it.
func storedReference(part segment, key func(string) string, schema string) string {
	keyed := slices.Clone(part.names)
	last := len(keyed)
	if !part.clause {
		last--
	}
	for index := range last {
		if key != nil {
			keyed[index] = key(keyed[index])
		}
	}
	if len(keyed) == 3 && keyed[0] == keyOf(schema, key) {
		keyed = keyed[1:]
	}
	quoted := make([]string, len(keyed))
	for index, name := range keyed {
		quoted[index] = `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
	return strings.Join(quoted, ".")
}

// keyOf answers name keyed by key, or name where key is nil.
func keyOf(name string, key func(string) string) string {
	if key == nil {
		return name
	}
	return key(name)
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
