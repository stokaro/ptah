package ast

import (
	"slices"
	"strings"
)

// PostgresDoBlockNode represents a PostgreSQL DO statement parsed through the
// dialect-specific routine layer.
type PostgresDoBlockNode struct {
	// SQL is the complete executable DO statement.
	SQL string
	// Language stores the optional LANGUAGE clause. Empty means PostgreSQL's
	// default plpgsql language.
	Language string
	// Body is the parsed anonymous routine body.
	Body PostgresRoutineBody
}

// PostgresRoutineNode represents a PostgreSQL CREATE PROCEDURE statement parsed
// through the dialect-specific routine layer.
type PostgresRoutineNode struct {
	// SQL is the complete executable routine statement.
	SQL string
	// Dialect is the normalized SQL dialect that selected this parser path.
	Dialect string
	// Kind identifies whether this is a CREATE FUNCTION or CREATE PROCEDURE.
	Kind RoutineKind
	// Name is the routine name, including schema qualifiers or quoting.
	Name string
	// Parameters contains the raw parameter list without surrounding
	// parentheses.
	Parameters string
	// Language stores the normalized LANGUAGE clause when present.
	Language string
	// Security is the mode a SECURITY clause states, DEFINER or INVOKER,
	// and empty for a routine that writes none.
	Security string
	// Settings are the routine's SET clauses, each `name=value` or
	// `name FROM CURRENT`, in the order the statement writes them.
	Settings []string
	// Body is the parsed routine body metadata.
	Body PostgresRoutineBody
}

// PostgresRoutineBody is the body of a PostgreSQL function, procedure, or DO
// block as the statement writes it. What its statements are is the PostgreSQL
// owner's to read: a host asks the runtime's routine body reader
// (ptah.run/core/routineext).
type PostgresRoutineBody struct {
	// SQL is the raw body text without string or dollar-quote delimiters.
	SQL string
	// Delimiter is the dollar-quote delimiter such as $$ or $tag$. It is empty
	// for single-quoted and SQL-standard bodies.
	Delimiter string
	// Language is the normalized body language known at parse time.
	Language string
}

// SQLServerRoutineNode represents a SQL Server CREATE FUNCTION or CREATE
// PROCEDURE statement parsed through the dialect-specific routine layer.
type SQLServerRoutineNode struct {
	// SQL is the complete executable routine statement without GO separators.
	SQL string
	// Dialect is the normalized SQL dialect that selected this parser path.
	Dialect string
	// Kind identifies whether this is a CREATE FUNCTION or CREATE PROCEDURE.
	Kind RoutineKind
	// Name is the routine name, including bracketed schema qualifiers.
	Name string
	// Parameters contains the raw parameter list without surrounding
	// parentheses.
	Parameters string
	// Returns contains the raw T-SQL RETURNS clause for functions.
	Returns string
	// Form classifies the SQL Server routine shape.
	Form SQLServerRoutineForm
	// Body is the routine body text.
	Body SQLServerRoutineBody
}

// SQLServerRoutineForm identifies SQL Server function and procedure shapes.
type SQLServerRoutineForm string

const (
	SQLServerRoutineFormProcedure                   SQLServerRoutineForm = "procedure"
	SQLServerRoutineFormScalarFunction              SQLServerRoutineForm = "scalar_function"
	SQLServerRoutineFormInlineTableValuedFunction   SQLServerRoutineForm = "inline_table_valued_function"
	SQLServerRoutineFormMultiStatementTableFunction SQLServerRoutineForm = "multi_statement_table_valued_function"
)

// SQLServerRoutineBody is the text of a T-SQL routine body. The SQL Server
// owner splits it into statements (ptah.run/dialect/mssql/mssqlroutine),
// through the runtime's routine body reader.
type SQLServerRoutineBody struct {
	// SQL is the raw body text after AS.
	SQL string
}

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
	// Comment is the text of the routine's COMMENT characteristic, unquoted,
	// and empty for a routine that has none.
	Comment string
	// Body is the routine body as the statement writes it.
	Body MySQLRoutineBody
}

// MySQLRoutineBody is the body of a MySQL routine as the statement writes it.
type MySQLRoutineBody struct {
	// SQL is the full body fragment, usually BEGIN ... END or RETURN ... What
	// its statements are is the MySQL owner's to read: a host asks the
	// runtime's routine body reader (ptah.run/core/routineext).
	SQL string
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

// Accept hands the visitor this node.
//
// A routine is a complete statement, so a renderer reads the SQL out of this
// node rather than being handed a substitute carrying only the text.
func (n *MySQLRoutineNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// NewPostgresDoBlock creates a PostgreSQL DO block node.
func NewPostgresDoBlock(sql string) *PostgresDoBlockNode {
	return &PostgresDoBlockNode{SQL: strings.TrimSpace(sql)}
}

// NewPostgresRoutine creates a PostgreSQL routine node.
func NewPostgresRoutine(sql, dialect string, kind RoutineKind) *PostgresRoutineNode {
	return &PostgresRoutineNode{
		SQL:     strings.TrimSpace(sql),
		Dialect: dialect,
		Kind:    kind,
	}
}

// Accept hands the visitor this node.
//
// A routine is a complete statement, so a renderer reads the SQL out of this
// node rather than being handed a substitute carrying only the text.
func (n *PostgresDoBlockNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// Accept hands the visitor this node.
//
// A routine is a complete statement, so a renderer reads the SQL out of this
// node rather than being handed a substitute carrying only the text.
func (n *PostgresRoutineNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// NewSQLServerRoutine creates a SQL Server routine node.
func NewSQLServerRoutine(sql, dialect string, kind RoutineKind) *SQLServerRoutineNode {
	return &SQLServerRoutineNode{
		SQL:     strings.TrimSpace(sql),
		Dialect: dialect,
		Kind:    kind,
	}
}

// Accept hands the visitor this node.
//
// A routine is a complete statement, so a renderer reads the SQL out of this
// node rather than being handed a substitute carrying only the text.
func (n *SQLServerRoutineNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }
