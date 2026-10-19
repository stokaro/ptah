// Package lexer provides SQL tokenization and lexical analysis for the Ptah schema management system.
//
// This package implements a comprehensive SQL lexer that breaks down SQL input into tokens
// for consumption by the parser. It handles various SQL constructs including strings,
// comments, identifiers, operators, and whitespace while maintaining position information
// for accurate error reporting.
//
// # Architecture
//
// The lexer follows a state-machine approach with these key components:
//
//   - Lexer: Main tokenizer that processes SQL input character by character
//   - Token: Represents individual SQL tokens with type and position information
//   - TokenType: Enumeration of all supported token types
//
// # Supported Token Types
//
// The lexer recognizes the following token types:
//
//   - TokenString: String literals enclosed in single or double quotes
//   - TokenComment: Line comments (--) and block comments (/* */)
//   - TokenIdentifier: SQL identifiers including keywords and column names
//   - TokenOperator: SQL operators and punctuation
//   - TokenSemicolon: Statement terminators
//   - TokenWhitespace: Spaces, tabs, and newlines
//   - TokenEOF: End of input marker
//
// # Key Features
//
//   - Proper string literal handling with escape sequence support
//   - Comment recognition for both line and block comments
//   - Backtick-quoted identifier support for MySQL compatibility
//   - Position tracking for error reporting and debugging
//   - Efficient single-pass tokenization
//
// # Usage Example
//
// Basic tokenization:
//
//	lexer := lexer.NewLexer("CREATE TABLE users (id INTEGER PRIMARY KEY);")
//	for {
//		token := lexer.NextToken()
//		if token.Type == lexer.TokenEOF {
//			break
//		}
//		fmt.Printf("Token: %s, Value: %s\n", token.Type, token.Value)
//	}
//
// # A lexer of an engine's own
//
// This package holds the shared rules and no engine's own lexer. An engine
// whose text those rules cannot read, as YDB's YQL, brings a [Scanner], and
// a [Lexer] built with [Options.Scanner] set returns the scanner's tokens.
// [StringValue] asks the scanner too. internal/lexerext declares the services
// that bring a scanner, and internal/dialectlexer sets one for a dialect whose
// descriptor names a lexer of its own, so a reader of the dialect's text gets
// that lexer's tokens without naming it.
//
// # Integration with Ptah
//
// The lexer integrates with other Ptah components:
//
//   - ptah/internal/parser: Consumes tokens to build AST nodes
//   - ptah/core/sqlutil: Uses lexer for SQL statement splitting and comment removal
//
// # Token Position Information
//
// Each token includes position information for accurate error reporting:
//
//	type Token struct {
//		Type  TokenType
//		Value string
//		Start int  // Starting position in input
//		End   int  // Ending position in input
//	}
//
// # String Handling
//
// The lexer properly handles SQL string literals:
//
//   - Single-quoted strings: 'example string'
//   - Double-quoted strings: "example string"
//   - Escape sequences within strings
//   - Proper handling of quotes within strings
//   - PostgreSQL's E'...', U&'...' and $tag$...$tag$ strings, and a string
//     continued on the next line, each as one token where the options ask
//     for them; [StringValue] reads the text any of them stands for
//
// # Comment Support
//
// Both SQL comment styles are supported:
//
//   - Line comments: -- This is a comment
//   - Block comments: /* This is a block comment */
//   - Nested block comments are not supported (SQL standard)
//
// # Performance Characteristics
//
// The lexer is designed for efficiency:
//
//   - Single-pass tokenization with O(n) complexity
//   - Minimal memory allocation during tokenization
//   - Efficient character-by-character processing
//   - Lookahead beyond the current character only where a token's end
//     depends on it: a dollar quote's tag, and the gap after a string that
//     may continue it
//
// # Error Handling
//
// The lexer handles malformed input gracefully:
//
//   - Unterminated strings are handled as best-effort tokens
//   - Unknown characters are classified as operators
//   - Position information is maintained for error reporting
//
// # Thread Safety
//
// Lexer instances are not thread-safe and should not be used concurrently
// from multiple goroutines. Each lexer instance should be used by a single
// goroutine for the duration of tokenization.
package lexer
