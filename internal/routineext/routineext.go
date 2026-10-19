// Package routineext declares the services of a routine grammar. The SQL
// parser and schema reader select them through descriptor.RoutineGrammar.Body.
// Owners read statement boundaries, headers, and options; the host keeps the
// token cursor, time budget, owner claims, and destination name normalization.
package routineext

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/lexer"
)

// Services describes the routine grammars one owner serves. A zero value
// serves none. Built-in composition is in internal/routineext/builtin.
type Services struct {
	// Grammars are the descriptor.RoutineGrammar.Body values served.
	Grammars []string
	// Converter reads a parsed routine into the schema model.
	Converter Converter
	// Syntax reads a routine header and advances the shared token cursor.
	Syntax Syntax
	// ReadDo reads an anonymous block. Nil leaves it as raw SQL.
	ReadDo func(string, lexer.Options, string) (ast.RoutineBody, string, error)
	// FunctionBody attaches source body metadata to a standard function.
	FunctionBody func(*ast.CreateFunctionNode)
	// Delimiter recovers the opening delimiter of a quoted body; nil records none.
	Delimiter func(string) string
	// ReplacesWithAlter reports whether CREATE OR ALTER replaces a routine.
	ReplacesWithAlter bool
}

// Request carries a non-nil parsed routine and its name in the destination
// schema. The reader normalizes the name using the destination dialect, which
// can differ from the dialect that parsed the routine.
type Request struct {
	Node *ast.RoutineNode
	Name string
}

// Converter reads a routine without changing its node. Errors describe an
// option the model cannot represent; the SQL reader adds its refusal sentinel.
type Converter interface {
	Convert(Request) (schemamodel.Function, error)
}

// Syntax reads the routine grammar an owner supplies. Calls start on the
// target token after CREATE and preserve source offsets through Cursor.
type Syntax interface {
	Create(Cursor, string, int) (ast.Node, error)
	Definer(Cursor, int) (ast.Node, error)
}

// Cursor is the token stream and shared facilities a routine reader uses.
// Advance consumes one token. Statement includes a current terminator;
// Fragment trims a source slice. The function and fallback methods invoke the
// shared best-effort grammar, never the selected owner's Syntax again.
// Current and Previous return borrowed, read-only tokens, valid until Advance.
// Input and offsets are byte-based. Readers must check CheckTimeout while
// advancing and must not retain the cursor after their call returns.
type Cursor interface {
	Input() string
	Dialect() string
	FamilyOptions() lexer.Options
	Current() *lexer.Token
	Previous() *lexer.Token
	AtEnd() bool
	Advance()
	CheckTimeout() error
	Statement(int) string
	Fragment(int, int) string
	Function(int) (ast.Node, error)
	Fallback(string, int) (ast.Node, error)
	DefinerFallback(int) (ast.Node, error)
	BatchSeparator() bool
	ClaimOptions(*ast.RoutineNode, []OptionSpan, int)
}

// OptionSpan carries one routine option for the shared owner's claim matcher.
// Start is a byte offset in the collected statement. Words keep the lexer
// spelling; Text is normalized for the common node and Source is lossless.
type OptionSpan struct {
	Text, Source string
	Words        []string
	Start        int
}
