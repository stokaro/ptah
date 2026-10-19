// Package lexerext declares what a lexer of an engine's own brings to the
// readers of SQL text: the scanner that reads the engine's text in place of
// the shared rules of internal/lexer. internal/dialectlexer asks for it by the
// lexer a dialect's descriptor names (core/platform/descriptor.Lexical.Lexer),
// so the shared lexer holds no engine's rules and dialectlexer names no
// lexer, and internal/lexerext/builtin lists the services of the lexers Ptah
// ships.
//
// A rule the shared lexer can read by a switch is descriptor data instead
// (core/platform/descriptor.Lexical). A service holds a lexer whose rules the
// descriptor's fields do not describe.
package lexerext

import "ptah.run/internal/lexer"

// Services is what one lexer brings to the readers of SQL text. The zero
// value brings nothing, and a dialect whose lexer brings no scanner is read by
// the shared rules.
type Services struct {
	// Lexers are the descriptor.Lexical.Lexer names these services serve.
	Lexers []string
	// Scanner reads the lexer's text. internal/dialectlexer sets it as
	// lexer.Options.Scanner, so it must be comparable, as lexer.Options is.
	Scanner lexer.Scanner
}
