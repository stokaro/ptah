package ast

import (
	"slices"
	"strings"
)

// MySQLRoutineNode represents a MySQL-family CREATE FUNCTION or CREATE
// PROCEDURE statement parsed through the dialect-specific routine layer.
type MySQLRoutineNode struct {
	// SQL is the complete executable routine statement without client
	// delimiter commands.
	SQL string
	// Dialect is the normalized SQL dialect that selected this parser path.
	Dialect string
	// Kind identifies whether this is a CREATE FUNCTION or CREATE PROCEDURE.
	Kind RoutineKind
	// Definer stores an optional MySQL DEFINER clause such as
	// DEFINER=`user`@`host`.
	Definer string
	// Name is the routine name, including any schema qualifier or identifier
	// quoting used by the source statement.
	Name string
	// Parameters contains the raw parameter list without surrounding
	// parentheses.
	Parameters string
	// Returns contains the raw MySQL function return type. It is empty for
	// procedures.
	Returns string
	// Characteristics stores raw routine characteristics before the body, such
	// as DETERMINISTIC, SQL SECURITY INVOKER, or MODIFIES SQL DATA.
	Characteristics []string
	// Body is the parsed routine body. Expressions remain raw SQL fragments;
	// statement boundaries and procedural statement kinds are modeled.
	Body MySQLRoutineBody
}

// MySQLRoutineBody contains a parsed MySQL routine body.
type MySQLRoutineBody struct {
	// SQL is the full body fragment, usually BEGIN ... END or RETURN ...
	SQL string
	// Statements are the top-level routine-body statements.
	Statements []MySQLRoutineStatement
}

// MySQLRoutineStatementKind identifies a MySQL routine-body statement class.
type MySQLRoutineStatementKind string

const (
	MySQLRoutineStatementRaw         MySQLRoutineStatementKind = "raw"
	MySQLRoutineStatementDeclaration MySQLRoutineStatementKind = "declaration"
	MySQLRoutineStatementHandler     MySQLRoutineStatementKind = "handler"
	MySQLRoutineStatementCursor      MySQLRoutineStatementKind = "cursor"
	MySQLRoutineStatementIf          MySQLRoutineStatementKind = "if"
	MySQLRoutineStatementCase        MySQLRoutineStatementKind = "case"
	MySQLRoutineStatementBlock       MySQLRoutineStatementKind = "block"
	MySQLRoutineStatementLoop        MySQLRoutineStatementKind = "loop"
	MySQLRoutineStatementWhile       MySQLRoutineStatementKind = "while"
	MySQLRoutineStatementRepeat      MySQLRoutineStatementKind = "repeat"
	MySQLRoutineStatementLeave       MySQLRoutineStatementKind = "leave"
	MySQLRoutineStatementIterate     MySQLRoutineStatementKind = "iterate"
	MySQLRoutineStatementReturn      MySQLRoutineStatementKind = "return"
	MySQLRoutineStatementSet         MySQLRoutineStatementKind = "set"
	MySQLRoutineStatementSelect      MySQLRoutineStatementKind = "select"
	MySQLRoutineStatementInsert      MySQLRoutineStatementKind = "insert"
	MySQLRoutineStatementUpdate      MySQLRoutineStatementKind = "update"
	MySQLRoutineStatementDelete      MySQLRoutineStatementKind = "delete"
	MySQLRoutineStatementLabel       MySQLRoutineStatementKind = "label"
)

// MySQLRoutineStatement is one top-level statement inside a MySQL routine
// body. SQL expression internals are preserved as raw fragments.
type MySQLRoutineStatement struct {
	Kind MySQLRoutineStatementKind
	SQL  string
}

// NewMySQLRoutine creates a MySQL-family routine node.
func NewMySQLRoutine(sql, dialect string, kind RoutineKind) *MySQLRoutineNode {
	return &MySQLRoutineNode{
		SQL:     strings.TrimSpace(sql),
		Dialect: dialect,
		Kind:    kind,
	}
}

// SetCharacteristics replaces routine characteristics.
func (n *MySQLRoutineNode) SetCharacteristics(characteristics []string) *MySQLRoutineNode {
	n.Characteristics = slices.Clone(characteristics)
	return n
}

// Accept renders MySQL routines through the raw SQL visitor contract while
// keeping the structured routine metadata available to parser consumers.
func (n *MySQLRoutineNode) Accept(visitor Visitor) error {
	raw := RawSQLNode{SQL: n.SQL}
	return visitor.VisitRawSQL(&raw)
}
