// Package routineext declares the services of a routine grammar. The SQL
// schema reader selects them through descriptor.RoutineGrammar.Body. Engine
// owners supply the conversion; shared code does not interpret their options.
package routineext

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

// Services describes the routine grammars one owner serves. A zero value
// serves none. Built-in composition is in internal/routineext/builtin.
type Services struct {
	// Grammars are the descriptor.RoutineGrammar.Body values served.
	Grammars []string
	// Converter reads a parsed routine into the schema model.
	Converter Converter
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
