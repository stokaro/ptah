package schemalineage

import (
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/parser"
)

// RoutineRead is one column a routine body reads.
//
// Its own type beside [RoutineWrite] rather than a direction field on one:
// "what breaks if I drop this column" and "what changes this column" are
// different questions, and a caller asking the first must not have to filter
// the second out of the answer.
type RoutineRead struct {
	Table     string `json:"table"`
	Column    string `json:"column"`
	ByRoutine string `json:"by_routine"`
	// Kind separates a function from a procedure.
	Kind string `json:"kind,omitempty"`
	// Statement names what does the reading: select, update or delete.
	Statement string `json:"statement"`
}

// readsAreDerivable reports whether a dialect resolves a variable and a column
// of the same name in a way that lets this analysis tell them apart.
//
// This is the whole of what decides which dialects get a read answer, and it is
// not about parser strength. PostgreSQL defaults plpgsql.variable_conflict to
// error, so a body that compiles has no collision. T-SQL variables carry an @
// prefix and cannot collide at all. MySQL resolves the other way -- a declared
// variable takes precedence over a column of the same name -- so an identifier
// in a MySQL body does not say which it is, and reporting it as a column read
// would be the confident wrong answer this package exists to avoid
// (stokaro/ptah#2394).
func readsAreDerivable(dialect string) bool {
	if platform.IsPostgresFamily(dialect) {
		return true
	}
	return isSQLServerFamily(dialect)
}

// deriveProceduralReads resolves the columns a procedural body reads.
//
// It answers only for a statement whose source table is unambiguous, which is
// the same boundary the view half draws: with more than one table in scope an
// unqualified column cannot be attributed, and attributing it to the wrong one
// is worse than saying nothing.
func deriveProceduralReads(routine schemamodel.Function, dialect, kind string, columns map[string][]string) []RoutineRead {
	statements := proceduralStatementSQL(routine, dialect)
	reads := make([]RoutineRead, 0)
	for _, sql := range statements {
		reads = append(reads, statementReads(sql, routine.Name, kind, columns)...)
	}
	sortRoutineReads(reads)
	return dedupeReads(reads)
}

// proceduralStatementSQL flattens a body into the text of each statement.
func proceduralStatementSQL(routine schemamodel.Function, dialect string) []string {
	if platform.IsPostgresFamily(dialect) {
		return postgresStatementSQL(parser.ParseRoutineBody(routine.Body, routine.Language).Statements)
	}
	var sql []string
	for _, statement := range parser.ParseSQLServerRoutineBody(routine.Body).Statements {
		sql = append(sql, statement.SQL)
	}
	return sql
}

// postgresStatementSQL walks the PL/pgSQL statements, descending into the ones
// that carry others so a read inside a branch is not missed.
func postgresStatementSQL(statements []ast.PostgresRoutineStatement) []string {
	var sql []string
	for _, statement := range statements {
		if len(statement.Statements) > 0 {
			sql = append(sql, postgresStatementSQL(statement.Statements)...)
			continue
		}
		sql = append(sql, statement.SQL)
	}
	return sql
}

// statementReads resolves one statement's reads, or none.
func statementReads(sql, routine, kind string, columns map[string][]string) []RoutineRead {
	tokens := tokenize(sql)
	if len(tokens) == 0 {
		return nil
	}
	source, ok := readSource(tokens)
	if !ok {
		return nil
	}
	tableColumns, known := columns[lowerName(source.table)]
	if !known {
		return nil
	}
	return readsOfTable(tokens, source, routine, kind, tableColumns)
}

// readsOfTable names the columns of one table that a statement mentions.
//
// source.written holds the token indexes that are assignment targets rather
// than reads: in `UPDATE t SET c = 1` the column c is written and not read, and
// reporting it as a read would make a column nothing depends on look depended
// upon.
func readsOfTable(tokens []lexer.Token, source readSourceRef, routine, kind string, tableColumns []string) []RoutineRead {
	var reads []RoutineRead
	for index, token := range tokens {
		if source.written[index] || token.Type != lexer.TokenIdentifier {
			continue
		}
		name := unquote(token.Value)
		if !containsFold(tableColumns, name) {
			continue
		}
		reads = append(reads, RoutineRead{
			Table: source.table, Column: name, ByRoutine: routine,
			Kind: kind, Statement: source.statement,
		})
	}
	return reads
}

// containsFold reports whether a name is in the list, ignoring case.
func containsFold(names []string, name string) bool {
	for _, candidate := range names {
		if sameName(candidate, name) {
			return true
		}
	}
	return false
}

// sortRoutineReads orders the reads so two runs over one schema agree.
func sortRoutineReads(reads []RoutineRead) {
	sort.Slice(reads, func(i, j int) bool {
		a, b := reads[i], reads[j]
		if a.ByRoutine != b.ByRoutine {
			return a.ByRoutine < b.ByRoutine
		}
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		if a.Column != b.Column {
			return a.Column < b.Column
		}
		return a.Statement < b.Statement
	})
}

// dedupeReads collapses a column mentioned more than once in one statement.
//
// `WHERE id = 1 AND id < 9` reads id once as far as a caller is concerned; a
// list that counted mentions would answer a question nobody asked.
func dedupeReads(reads []RoutineRead) []RoutineRead {
	deduped := make([]RoutineRead, 0, len(reads))
	var previous RoutineRead
	for index, read := range reads {
		if index > 0 && read == previous {
			continue
		}
		deduped = append(deduped, read)
		previous = read
	}
	return deduped
}

// lowerRoutineWord reads a statement's leading word.
func lowerRoutineWord(tokens []lexer.Token) string {
	if len(tokens) == 0 || tokens[0].Type != lexer.TokenIdentifier {
		return ""
	}
	return strings.ToUpper(unquote(tokens[0].Value))
}
