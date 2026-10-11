package lintscan

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/lintext"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlcompound"
)

// Split cuts sql into the statements of mode's dialect and renders each in
// the forms rules read (see [lintext.Statement]).
//
// The scanner keeps a semicolon inside a string, a comment, an executable
// comment or a dollar-quoted body from ending a statement, and so does a
// semicolon inside a compound routine body. That question is
// [sqlcompound]'s, the same one core/sqlutil asks, because a linter that
// answered it differently cut `CREATE PROCEDURE p() BEGIN … ; … END` into
// fragments no parser could model: the routine reached the change model as
// nothing at all, and every body statement after the first reached the rules
// as a statement the migration performs (stokaro/ptah#2069).
//
// A statement's comments are those directly above it. A blank line detaches
// a comment from whatever comes next, and a comment on the line the previous
// statement ends on belongs to that statement: a suppression directive has to
// sit directly above its statement. Measured against atlas community version
// v1.3.0 on `-- atlas:nolint DS103`, a blank line, then a DROP COLUMN, it
// reports DS103 and exits 1, while the same two lines with no blank line
// between them exit 0.
func Split(sql string, mode Mode) []lintext.Statement {
	var statements []lintext.Statement
	start := -1
	end := 0
	startLine := 0
	lastStatementEndLine := 0
	var pending, active []string
	compound := sqlcompound.New(mode.dialect)
	emit := func() {
		statements = append(statements, statement(sql[start:end], start, end, startLine, active, mode))
	}
	for _, tok := range scanSQL(sql, mode) {
		observeCompoundToken(&compound, tok, mode)
		switch tok.kind {
		case tokSemicolon:
			if compound.KeepSemicolonInsideStatement() {
				// Body text. It extends the statement exactly as a word would,
				// so a routine keeps the semicolons it was written with.
				end = tok.end
				continue
			}
			if compound.TerminatorBelongsToStatement() {
				// PL/SQL: the block's own syntax requires this semicolon, so it
				// is the last byte of the statement rather than the client's
				// terminator.
				end = tok.end
			}
			compound.Reset()
			if start >= 0 {
				emit()
				start = -1
				active = nil
				lastStatementEndLine = tok.line
			}
		case tokWhitespace:
			// Never starts a statement; keep end untouched so trailing
			// comments and whitespace are not part of the statement text.
			if start < 0 && strings.Count(tok.text, "\n") > 1 {
				pending = nil
			}
		case tokComment:
			if start < 0 && tok.line != lastStatementEndLine {
				pending = append(pending, tok.text)
			}
		default:
			if start < 0 {
				start = tok.start
				startLine = tok.line
				active = slices.Clone(pending)
				pending = nil
			}
			end = tok.end
		}
	}
	if start >= 0 {
		emit()
	}
	return statements
}

// statement renders one statement's text in the forms rules read.
func statement(text string, start, end, line int, comments []string, mode Mode) lintext.Statement {
	return lintext.Statement{
		Start: start, End: end, Line: line, SQL: text,
		Canonical:   canonical(text, mode),
		Words:       Words(text, mode),
		SourceWords: sourceWords(text, mode),
		Routine:     routineOf(text, mode),
		Comments:    comments,
	}
}

// observeCompoundToken feeds one token to the compound-body tracker the way
// core/sqlutil feeds it the same text, so the linter and the migrator cut a
// statement at the same semicolon. Words are fed on every dialect. YQL also
// needs its punctuation, for the braces around a lambda's body, and its
// backticked names, which core/sqlutil feeds as words because the YQL lexer
// reads them as identifiers; neither changes the answer for another dialect,
// whose quoted names were never fed.
func observeCompoundToken(compound *sqlcompound.State, tok lintToken, mode Mode) {
	switch {
	case tok.kind == tokWord, mode.yql && tok.kind == tokQuotedIdent:
		compound.Word(tok.text)
	case mode.yql && tok.kind == tokOp:
		compound.Symbol(tok.text)
	}
}

// canonical renders a statement in its display form: comments removed,
// every whitespace run collapsed to one space, everything except string
// literals and quoted identifiers uppercased.
func canonical(sql string, mode Mode) string {
	var b strings.Builder
	pendingSpace := false
	for _, tok := range scanSQL(sql, mode) {
		switch tok.kind {
		case tokComment, tokWhitespace:
			// A comment separates tokens exactly like whitespace does
			// (DROP/*x*/TABLE must not canonicalize to DROPTABLE).
			pendingSpace = b.Len() > 0
		case tokString, tokQuotedIdent:
			if pendingSpace {
				b.WriteByte(' ')
				pendingSpace = false
			}
			b.WriteString(tok.text)
		default:
			if pendingSpace {
				b.WriteByte(' ')
				pendingSpace = false
			}
			b.WriteString(strings.ToUpper(tok.text))
		}
	}
	return b.String()
}

// Words renders sql as the word sequence rules scan (see
// [lintext.Statement.Words]). String literals and quoted identifiers keep
// their quotes and case, so quoted names never collide with SQL keywords in
// the scans.
func Words(sql string, mode Mode) []string {
	var words []string
	for _, tok := range scanSQL(sql, mode) {
		switch tok.kind {
		case tokComment, tokWhitespace:
			continue
		case tokString, tokQuotedIdent:
			words = append(words, tok.text)
		default:
			words = append(words, strings.ToUpper(tok.text))
		}
	}
	return words
}

func sourceWords(sql string, mode Mode) []string {
	var words []string
	for _, tok := range scanSQL(sql, mode) {
		switch tok.kind {
		case tokComment, tokWhitespace:
			continue
		default:
			words = append(words, tok.text)
		}
	}
	return words
}

// routineOf parses a statement that defines a routine, returning nil for one
// that does not and for a mode no dialect was given to.
//
// A cheap word test comes first so that an ordinary migration, which defines
// no routine, pays nothing.
func routineOf(sql string, mode Mode) *lintext.Routine {
	if strings.TrimSpace(mode.dialect) == "" || !MayDefineRoutine(sql) {
		return nil
	}
	list, err := parser.NewParser(sql, parser.WithDialect(mode.dialect)).Parse()
	if err != nil || list == nil {
		return nil
	}
	for _, node := range list.Statements {
		if body, ok := routineBodySQL(node); ok {
			return &lintext.Routine{BodyWords: Words(body, mode)}
		}
	}
	return nil
}

// MayDefineRoutine is the cheap test that keeps the routine parse off every
// statement that cannot define a routine. The lint host asks it too, for a run
// whose statements nothing parsed because it named no dialect.
func MayDefineRoutine(sql string) bool {
	upper := strings.ToUpper(sql)
	return strings.Contains(upper, "FUNCTION") || strings.Contains(upper, "PROCEDURE")
}

// routineBodySQL returns the body text of whichever node a routine arrived as.
//
// Four node types, because four dialect paths claim a routine, and the fifth,
// the opaque node, is the one the parser preserves verbatim when no path
// claimed it, so there is no parsed body to read.
func routineBodySQL(node ast.Node) (string, bool) {
	switch routine := node.(type) {
	case *ast.CreateFunctionNode:
		return routine.Body, true
	case *ast.PostgresRoutineNode:
		return routine.Body.SQL, true
	case *ast.MySQLRoutineNode:
		return routine.Body.SQL, true
	case *ast.SQLServerRoutineNode:
		return routine.Body.SQL, true
	default:
		return "", false
	}
}
