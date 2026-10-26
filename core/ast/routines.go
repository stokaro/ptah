package ast

import (
	"strings"

	"ptah.run/core/schemaext"
)

// DoBlockNode is a DO statement: an anonymous code block in a procedural
// language, which the server runs once and does not store.
type DoBlockNode struct {
	// SQL is the complete executable DO statement.
	SQL string
	// Language is the block's procedural language in lower case: the
	// LANGUAGE clause the block writes, or the default the reader assumes
	// where it writes none. Empty means no language is known.
	Language string
	// Body is the parsed anonymous routine body.
	Body RoutineBody
}

// RoutineNode is a CREATE FUNCTION or CREATE PROCEDURE statement that the
// routine grammar of a dialect read. The statement is kept whole, beside the
// header the grammar recovered from it, and a renderer emits the statement.
// Which grammar read it is the one the descriptor of [RoutineNode.Dialect]
// names.
//
// A header field holds what the statement writes, and stays empty where the
// grammar that read it does not recover that part: only the quoted-body
// grammar reads LANGUAGE, SECURITY and SET clauses into Language, Security
// and Settings, and only the compound-statement grammar reads DEFINER and
// COMMENT into Definer and Comment.
type RoutineNode struct {
	// SQL is the complete executable routine statement, without client batch
	// separators or delimiter commands.
	SQL string
	// Dialect is the normalized SQL dialect whose routine grammar read the
	// statement.
	Dialect string
	// Kind identifies whether this is a CREATE FUNCTION or CREATE PROCEDURE.
	Kind RoutineKind
	// Replace records that the statement replaces a routine of the same
	// signature that exists: CREATE OR REPLACE, or SQL Server's CREATE OR
	// ALTER. SQL holds the clause either way.
	Replace bool
	// Name is the routine name as the statement writes it, with any schema
	// qualifier and identifier quoting.
	Name string
	// Parameters contains the raw parameter list without surrounding
	// parentheses.
	Parameters string
	// Returns is the raw return type a function's header writes, and empty
	// for a procedure and for a grammar that does not read it.
	Returns string
	// Language is the LANGUAGE clause in lower case, and empty for a routine
	// that writes none.
	Language string
	// Security is the mode a SECURITY clause states, DEFINER or INVOKER,
	// and empty for a routine that writes none.
	Security string
	// Settings are the routine's SET clauses, each `name=value` or
	// `name FROM CURRENT`, in the order the statement writes them.
	Settings []string
	// Definer is the DEFINER clause as the statement writes it, such as
	// DEFINER=`user`@`host`.
	Definer string
	// Options are the clauses the header writes between the signature and the
	// body that no field above holds, in the order the statement writes them,
	// such as DETERMINISTIC, SQL SECURITY INVOKER, WITH EXECUTE AS OWNER or
	// FOR REPLICATION.
	Options []RoutineOption
	// Comment is the text of the routine's COMMENT clause, unquoted, and empty
	// for a routine that has none.
	Comment string
	// Facets are the settings the owners' SQL syntax read from the options;
	// see [RoutineOption.Read].
	Facets schemaext.Facets
	// Body is the routine body as the statement writes it.
	Body RoutineBody
}

// RoutineOption is one clause a routine's header writes between its
// signature and its body.
type RoutineOption struct {
	// Text is the clause as the grammar records it. The transact-sql grammar
	// upper-cases its keywords and keeps a principal's string as written, such
	// as EXECUTE AS 'auditor'; the compound-statement grammar keeps the clause
	// as the statement writes it.
	Text string
	// Read reports that an owner's SQL syntax read the option, and holds what
	// it read in the routine's [RoutineNode.Facets].
	Read bool
}

// RoutineBody is the body of a routine or a DO block as the statement writes
// it. The dialect's owner reads its statements through the runtime's routine
// body reader (ptah.run/core/routineext).
type RoutineBody struct {
	// SQL is the raw body text, without string or dollar-quote delimiters.
	SQL string
	// Delimiter is the dollar-quote delimiter such as $$ or $tag$. It is empty
	// for a body no dollar quote delimits.
	Delimiter string
	// Language is the body's language in lower case where it is known at
	// parse time, and empty otherwise.
	Language string
}

// NewDoBlock creates a DO block node for the statement sql, trimmed of
// surrounding space, with no language and no body.
func NewDoBlock(sql string) *DoBlockNode {
	return &DoBlockNode{SQL: strings.TrimSpace(sql)}
}

// Accept hands the visitor this node.
//
// A routine is a complete statement, so a renderer reads the SQL out of this
// node rather than being handed a substitute carrying only the text.
func (n *DoBlockNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// NewRoutine creates a routine node for the statement sql, trimmed of
// surrounding space, that the routine grammar of dialect read.
func NewRoutine(sql, dialect string, kind RoutineKind) *RoutineNode {
	return &RoutineNode{
		SQL:     strings.TrimSpace(sql),
		Dialect: dialect,
		Kind:    kind,
	}
}

// Accept hands the visitor this node.
//
// A routine is a complete statement, so a renderer reads the SQL out of this
// node rather than being handed a substitute carrying only the text.
func (n *RoutineNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }
