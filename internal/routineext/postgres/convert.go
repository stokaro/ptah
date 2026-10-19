// Package postgres serves the postgres routine grammar to the SQL reader.
package postgres

import (
	"slices"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/routineext"
)

// Services returns the grammars, header reader, and conversion this owner supplies.
func Services() routineext.Services {
	return routineext.Services{Grammars: []string{descriptor.RoutineQuotedBody}, Converter: converter{}, Syntax: syntax{}, ReadDo: ReadDo, FunctionBody: attachFunctionBody, Delimiter: dollarQuoteDelimiter}
}

type converter struct{}

// Convert converts a routine the quoted-body routine grammar read.
//
// CREATE FUNCTION reaches this package as a CreateFunctionNode and CREATE
// PROCEDURE as a routine node, so a schema's procedures were parsed and then
// not converted (stokaro/ptah#2435).
func (converter) Convert(request routineext.Request) (schemamodel.Function, error) {
	node := request.Node
	function := schemamodel.Function{
		Name:       request.Name,
		Kind:       string(node.Kind),
		Parameters: node.Parameters,
		Language:   node.Language,
		Security:   node.Security,
		Settings:   slices.Clone(node.Settings),
		Body:       strings.TrimSpace(node.Body.SQL),
	}
	function.Canonicalize()
	return function, nil
}
