// Package ydbgap names the parts of Ptah that accept YDB by name and do not
// implement it yet, so that each one refuses with the same words and points
// to the plan that adds it.
//
// YDB is a dialect name before it has a renderer, a planner, a driver or a
// linter: stokaro/ptah#4015 adds them in phases. A layer reached with the name
// and nothing behind it would otherwise answer with its default arm, which is
// another dialect's behavior or an error about an empty driver name. Each such
// layer refuses through [Layer.Message] instead, and the phase that implements
// a layer removes its constant together with every refusal that names it.
package ydbgap

import "fmt"

// Plan is the issue that plans YDB support and owns every gap named here.
const Plan = "stokaro/ptah#4015"

// Layer is one part of Ptah that YDB does not reach yet.
type Layer int

// The layers YDB does not reach yet. Each answers [Layer.Phase] with the phase
// of [Plan] that implements it.
const (
	// Rendering is turning a desired schema into YQL DDL.
	Rendering Layer = iota + 1
	// Planning is turning a schema difference into a YQL migration.
	Planning
	// SchemaFiles is reading a YQL file as a desired schema.
	SchemaFiles
	// Connecting is opening a YDB server, and with it every reader, writer
	// and migrator that needs one.
	Connecting
	// Linting is `ptah sql lint` and `ptah migrations lint` over YQL.
	Linting
	// CreatingDatabases is making a new YDB database for a scratch or dev
	// run. SQL cannot create one; the alternative is designed with the dev
	// database work.
	CreatingDatabases
)

// work is what a layer does, as a refusal prints it.
func (l Layer) work() string {
	switch l {
	case Rendering:
		return "rendering a YDB schema"
	case Planning:
		return "planning a YDB migration"
	case SchemaFiles:
		return "reading a YDB schema file"
	case Connecting:
		return "connecting to a YDB server"
	case Linting:
		return "linting YQL for YDB"
	case CreatingDatabases:
		return "creating a YDB database"
	default:
		return "this YDB operation"
	}
}

// Phase is the phase of [Plan] that implements the layer, or 0 for a value
// that names no layer.
func (l Layer) Phase() int {
	switch l {
	case SchemaFiles:
		return 2
	case Rendering, Planning:
		return 3
	case Connecting:
		return 4
	case Linting:
		return 8
	case CreatingDatabases:
		return 9
	default:
		return 0
	}
}

// Message is the refusal a layer reports: what is not implemented, and where
// the work is planned. It names YDB, so a caller does not repeat the dialect.
func (l Layer) Message() string {
	return fmt.Sprintf("%s is not implemented yet (%s, phase %d)", l.work(), Plan, l.Phase())
}
