// Package doblockext declares what a procedural language brings to the SQL
// schema reader: the roles an anonymous DO block of a schema file declares
// when the file is read as a desired schema. internal/sqlschema asks for a
// reader by the language a dialect's descriptor names
// (core/platform/descriptor.Statements.DoBlocks), so it names no engine and no
// language, and internal/doblockext/builtin lists the readers of the languages
// Ptah ships.
//
// The reader runs in process. Whether a provider served over the protocol may
// also read DO blocks is open under stokaro/ptah#4204.
package doblockext

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

// Services is what one procedural language brings to the SQL schema reader.
// The zero value brings nothing, and the reader refuses a DO block of a
// dialect whose language it is.
type Services struct {
	// Languages are the descriptor.Statements.DoBlocks values these services
	// serve.
	Languages []string
	// Label names the language in a message, such as "PL/pgSQL".
	Label string
	// Roles reads the roles a DO block declares.
	Roles Reader
}

// Request is one DO block a schema file holds, and what the file declared
// before it.
type Request struct {
	// Dialect is the dialect the block is read as. A statement inside the
	// block is lexed and parsed as this dialect's.
	Dialect string
	// Body is the block's body: the text its dollar quotes enclose.
	Body string
	// Declared are the names of the roles the file declared before the block,
	// as the reader records them.
	Declared []string
	// Role records a CREATE ROLE statement the block runs, as the reader
	// records one the file writes outside a block. The name it records is the
	// one a later condition and a repeated declaration compare with.
	Role func(*ast.CreateRoleNode) schemamodel.Role
}

// Reader reads the roles of a DO block.
type Reader interface {
	// Roles returns the roles the block declares, in the order it declares
	// them, and none that Request.Declared names. An error says why the block
	// is not a declaration the reader can model, and returns no roles; it
	// carries no sentinel, since the schema reader refuses the statement with
	// its own. The text names no part of a CREATE ROLE statement, which can
	// carry a password.
	Roles(request Request) ([]schemamodel.Role, error)
}
