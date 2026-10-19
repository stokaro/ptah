// Package mysql serves the mysql routine grammar to the SQL reader.
package mysql

import (
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/mysqlroutine"
	"ptah.run/internal/routineext"
)

// Services returns the grammars, header reader, and conversion this owner supplies.
func Services() routineext.Services {
	return routineext.Services{Grammars: []string{descriptor.RoutineCompoundStatement}, Converter: converter{}, Syntax: syntax{}}
}

type converter struct{}

// Convert converts a routine the compound-statement routine grammar
// read.
//
// The options are the routine's characteristics. They carry the security mode
// and volatility, which [mysqlroutine.FromCharacteristics] reads, so a routine
// written in SQL
// states what the same routine read back from the server reports. A mode or a
// volatility the statement does not write stays empty, and the target's own
// applies.
func (converter) Convert(request routineext.Request) (schemamodel.Function, error) {
	node := request.Node
	characteristics := make([]string, 0, len(node.Options))
	for _, option := range node.Options {
		characteristics = append(characteristics, option.Text)
	}
	security, volatility := mysqlroutine.FromCharacteristics(characteristics)
	function := schemamodel.Function{
		Name:       request.Name,
		Kind:       string(node.Kind),
		Parameters: node.Parameters,
		Returns:    node.Returns,
		// The body is in the MySQL family's one routine language.
		Language:   mysqlroutine.Language,
		Security:   security,
		Volatility: volatility,
		Body:       strings.TrimSpace(node.Body.SQL),
		Comment:    node.Comment,
	}
	function.Canonicalize()
	return function, nil
}
