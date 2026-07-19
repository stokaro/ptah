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
	// Body is the parsed routine body metadata.
	Body PostgresRoutineBody
}

// PostgresRoutineBody contains parser metadata for a PostgreSQL function,
// procedure, or DO body. Expressions remain raw PostgreSQL fragments.
type PostgresRoutineBody struct {
	// SQL is the raw body text without string or dollar-quote delimiters.
	SQL string
	// Delimiter is the dollar-quote delimiter such as $$ or $tag$. It is empty
	// for single-quoted and SQL-standard bodies.
	Delimiter string
	// Language is the normalized body language known at parse time.
	Language string
	// Statements are top-level PL/pgSQL-ish statements when the body is a
	// block; SQL-language bodies usually remain a single raw statement.
	Statements []PostgresRoutineStatement
}

// PostgresRoutineStatementKind identifies a PostgreSQL routine-body statement
// class.
type PostgresRoutineStatementKind string

const (
	PostgresRoutineStatementRaw         PostgresRoutineStatementKind = "raw"
	PostgresRoutineStatementDeclaration PostgresRoutineStatementKind = "declaration"
	PostgresRoutineStatementBlock       PostgresRoutineStatementKind = "block"
	PostgresRoutineStatementException   PostgresRoutineStatementKind = "exception"
	PostgresRoutineStatementReturn      PostgresRoutineStatementKind = "return"
	PostgresRoutineStatementPerform     PostgresRoutineStatementKind = "perform"
	PostgresRoutineStatementExecute     PostgresRoutineStatementKind = "execute"
	PostgresRoutineStatementRaise       PostgresRoutineStatementKind = "raise"
	PostgresRoutineStatementIf          PostgresRoutineStatementKind = "if"
	PostgresRoutineStatementCase        PostgresRoutineStatementKind = "case"
	PostgresRoutineStatementLoop        PostgresRoutineStatementKind = "loop"
)

// PostgresRoutineStatement is one top-level statement inside a PostgreSQL
// routine body.
type PostgresRoutineStatement struct {
	Kind PostgresRoutineStatementKind
	SQL  string
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
	// Body is parsed routine-body metadata. Expressions remain raw T-SQL.
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

// SQLServerRoutineBody contains parser metadata for a T-SQL routine body.
type SQLServerRoutineBody struct {
	// SQL is the raw body text after AS.
	SQL string
	// Statements are top-level T-SQL routine-body statements when boundaries
	// are recoverable without parsing scalar expressions.
	Statements []SQLServerRoutineStatement
}

// SQLServerRoutineStatementKind identifies a T-SQL routine-body statement
// class.
type SQLServerRoutineStatementKind string

const (
	SQLServerRoutineStatementRaw         SQLServerRoutineStatementKind = "raw"
	SQLServerRoutineStatementDeclaration SQLServerRoutineStatementKind = "declaration"
	SQLServerRoutineStatementAssignment  SQLServerRoutineStatementKind = "assignment"
	SQLServerRoutineStatementBlock       SQLServerRoutineStatementKind = "block"
	SQLServerRoutineStatementIf          SQLServerRoutineStatementKind = "if"
	SQLServerRoutineStatementWhile       SQLServerRoutineStatementKind = "while"
	SQLServerRoutineStatementReturn      SQLServerRoutineStatementKind = "return"
	SQLServerRoutineStatementInsert      SQLServerRoutineStatementKind = "insert"
	SQLServerRoutineStatementSelect      SQLServerRoutineStatementKind = "select"
)

// SQLServerRoutineStatement is one top-level statement inside a T-SQL routine
// body.
type SQLServerRoutineStatement struct {
	Kind SQLServerRoutineStatementKind
	SQL  string
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
	Kind        MySQLRoutineStatementKind
	SQL         string
	Declaration *MySQLRoutineDeclaration
	Cursor      *MySQLRoutineCursor
	Handler     *MySQLRoutineHandler
}

// MySQLRoutineDeclarationKind identifies the DECLARE subform.
type MySQLRoutineDeclarationKind string

const (
	MySQLRoutineDeclarationVariable  MySQLRoutineDeclarationKind = "variable"
	MySQLRoutineDeclarationCondition MySQLRoutineDeclarationKind = "condition"
)

// MySQLRoutineDeclaration models DECLARE variable and condition statements.
// TypeSQL, DefaultSQL, and ConditionSQL remain raw MySQL fragments; expression
// parsing is intentionally outside the routine statement-boundary layer.
type MySQLRoutineDeclaration struct {
	Kind         MySQLRoutineDeclarationKind
	Names        []string
	TypeSQL      string
	DefaultSQL   string
	ConditionSQL string
}

// MySQLRoutineCursor models DECLARE ... CURSOR statements.
type MySQLRoutineCursor struct {
	Name      string
	SelectSQL string
}

// MySQLRoutineHandler models DECLARE ... HANDLER statements.
type MySQLRoutineHandler struct {
	Action       string
	Conditions   []string
	StatementSQL string
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

// Accept renders PostgreSQL DO blocks through the raw SQL visitor contract
// while keeping routine-body metadata available to parser consumers.
func (n *PostgresDoBlockNode) Accept(visitor Visitor) error {
	raw := RawSQLNode{SQL: n.SQL}
	return visitor.VisitRawSQL(&raw)
}

// Accept renders PostgreSQL routines through the raw SQL visitor contract while
// keeping routine-body metadata available to parser consumers.
func (n *PostgresRoutineNode) Accept(visitor Visitor) error {
	raw := RawSQLNode{SQL: n.SQL}
	return visitor.VisitRawSQL(&raw)
}

// NewSQLServerRoutine creates a SQL Server routine node.
func NewSQLServerRoutine(sql, dialect string, kind RoutineKind) *SQLServerRoutineNode {
	return &SQLServerRoutineNode{
		SQL:     strings.TrimSpace(sql),
		Dialect: dialect,
		Kind:    kind,
	}
}

// Accept renders SQL Server routines through the raw SQL visitor contract while
// keeping routine-body metadata available to parser consumers.
func (n *SQLServerRoutineNode) Accept(visitor Visitor) error {
	raw := RawSQLNode{SQL: n.SQL}
	return visitor.VisitRawSQL(&raw)
}
