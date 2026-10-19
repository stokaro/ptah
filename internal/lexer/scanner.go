package lexer

// Scanner is a lexer of an engine's own: one that reads the engine's text by
// rules the shared scanner does not hold, as YDB's YQL lexer reads @@...@@ as
// a string. A [Lexer] built with [Options.Scanner] set returns the tokens of
// the reader the scanner gives it, and holds no rule of the engine itself.
//
// internal/lexerext declares the services that bring a scanner, and
// internal/dialectlexer sets one for a dialect whose descriptor names a lexer
// of its own.
type Scanner interface {
	// Scan returns the reader of text from the offset start. head reports
	// that start is the head of the text, the place before its first token
	// where a server may read settings ahead of the query: it is true for a
	// lexer [NewLexerWithOptions] builds and false for a [Lexer.Range].
	//
	// The reader emits tokens of the types this package declares, with
	// Start and End as offsets in text, and a TokenEOF at the end of text.
	Scan(text string, start int, head bool) TokenReader

	// StringValue answers the text a string token of the reader stands for,
	// as [StringValue] answers it for the shared rules, and reports false for
	// a token that is not one of the scanner's string forms or whose escapes
	// the server refuses.
	StringValue(token string) (string, bool)

	// IdentifierValue answers the name an identifier token of the reader
	// stands for, with the scanner's quoting undone, and reports false for a
	// token that is not one whole identifier or whose escapes the server
	// refuses.
	IdentifierValue(token string) (string, bool)
}

// TokenReader returns the tokens of one text in order. After the last token
// it returns a TokenEOF, and it goes on returning one.
type TokenReader interface {
	// NextToken returns the next token.
	NextToken() Token
}
