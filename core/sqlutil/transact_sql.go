package sqlutil

import (
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/clientdelimiter"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlbatch"
	"ptah.run/internal/sqlcompound"
	"ptah.run/internal/sqlserverbatch"
)

// splitTransactSQL cuts a script of a dialect whose text is T-SQL
// (sqlserverbatch.Reads) into the statements Ptah sends one by one: sent holds
// each as [SplitSQLStatementsForDialect] returns it, and sources each as
// [SplitSourceStatements] does, so the two lists are cut at the same places.
// sent is never nil, as [SplitSQLStatementsForDialect] promises.
//
// A GO line ends a batch, as [SplitSQLStatementsForDialect] documents. Inside a
// batch a piece never ends inside a statement: statements end where SQL Server
// ends them, as internal/sqlserverbatch reads the batch, so a BEGIN ... END
// block, a TRY block with its CATCH block, and an IF or a WHILE with the
// statements it runs are each one statement. A piece ends at the semicolon
// after a statement. A MERGE keeps its semicolon, which the server requires
// (Msg 10713); every other statement is sent without the one that ends it.
//
// Statements written with no semicolon between them stay in one piece, which
// the server runs as one batch, as sqlcmd sends them. Each piece runs as a
// batch of its own, and a variable lives in its batch, so cutting them apart
// would end the scope of a DECLARE before the statement that reads it.
//
// The server reads the rest of a batch as the body of CREATE PROCEDURE,
// FUNCTION, TRIGGER or VIEW, and as the elements of CREATE SCHEMA, and each
// must open its batch (Msg 111). Such a statement starts a piece of its own,
// and the piece ends as the compound-body reader ends it: a body opened by
// BEGIN at the END that closes it, and any other at the first semicolon
// outside a body, or at the end of the batch. What follows runs as a batch of
// its own, so a migration need not write GO after each routine. A batch whose
// statement boundaries the reader cannot find is cut by the compound-body
// reader alone.
func splitTransactSQL(sql, dialect string, caps capability.Capabilities) ([]string, []SourceStatement) {
	text, origins := clientDelimitersFor(sql, dialect, caps)
	options := dialectlexer.ForSession(dialect, caps)
	sent := make([]string, 0)
	var sources []SourceStatement
	for _, batch := range transactSQLBatches(text, options, batchSeparator(dialect)) {
		pieces := transactSQLPieces{sql: sql, text: text, origins: origins}
		pieces.cut(batch, transactSQLCuts(text, batch, dialect, options))
		for range batch.count {
			sent = append(sent, pieces.sent...)
			sources = append(sources, pieces.sources...)
		}
	}
	return sent, sources
}

// transactSQLBatch is text[start:end], one batch of a script, which runs count
// times: a GO line with a count repeats the batch, and GO 0 discards it.
type transactSQLBatch struct {
	start, end, count int
}

// transactSQLBatches returns the batches of text, which the separator ends
// where it stands alone on its line (see [sqlbatch.SeparatorAt]). The rest of
// the separator's line belongs to no batch.
func transactSQLBatches(text string, options lexer.Options, separator string) []transactSQLBatch {
	var batches []transactSQLBatch
	start := 0
	skipping := false
	lexr := lexer.NewLexerWithOptions(text, options)
	for token := lexr.NextToken(); token.Type != lexer.TokenEOF; token = lexr.NextToken() {
		if skipping {
			if (token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment) && strings.ContainsAny(token.Value, "\r\n") {
				skipping = false
				start = token.End
			}
			continue
		}
		if separator == "" || !token.MatchIdentifierValue(separator) {
			continue
		}
		count, ok := sqlbatch.SeparatorAt(text, token.Start, token.End)
		if !ok {
			continue
		}
		batches = append(batches, transactSQLBatch{start: start, end: token.Start, count: count})
		skipping = true
		start = len(text)
	}
	if start < len(text) {
		batches = append(batches, transactSQLBatch{start: start, end: len(text), count: 1})
	}
	return batches
}

// transactSQLCut is where a piece of a batch ends: end is the offset after
// it, and semicolon the offset of the semicolon that ends it, or -1 for a piece
// that the next statement ends. keep reports that the server requires the
// semicolon, so the piece is sent with it.
type transactSQLCut struct {
	end, semicolon int
	keep           bool
}

// transactSQLCuts returns where the pieces of batch end, in order. A
// statement with no semicolon after it takes no cut of its own, so it joins
// the statement after it, and the comments after the last one stay with it.
// Only a statement that must open its batch cuts the text before it, at the
// end of the statement before it, so the comments between them go with the
// statement that opens the batch.
func transactSQLCuts(text string, batch transactSQLBatch, dialect string, options lexer.Options) []transactSQLCut {
	var cuts []transactSQLCut
	for start := batch.start; start < batch.end; {
		statements, err := sqlserverbatch.Read(text[start:batch.end])
		if err != nil {
			return append(cuts, compoundCuts(text, start, batch.end, dialect, options, 0)...)
		}
		next := batch.end
		gap := start
		pending := -1
		for _, statement := range statements {
			cuts = append(cuts, semicolonCuts(text, gap, start+statement.Start, options)...)
			if statement.Kind == sqlserverbatch.Module || statement.Kind == sqlserverbatch.Schema {
				if pending >= 0 {
					cuts = append(cuts, transactSQLCut{end: pending, semicolon: -1})
				}
				first := compoundCuts(text, start+statement.Start, batch.end, dialect, options, 1)
				cuts = append(cuts, first...)
				if len(first) > 0 {
					next = first[0].end
				}
				gap = next
				break
			}
			gap = start + statement.End
			if !strings.HasSuffix(statement.SQL, ";") {
				pending = gap
				continue
			}
			cuts = append(cuts, transactSQLCut{end: gap, semicolon: gap - 1, keep: statement.KeepsSemicolon})
			pending = -1
		}
		if gap < next {
			cuts = append(cuts, semicolonCuts(text, gap, next, options)...)
		}
		start = next
	}
	return cuts
}

// semicolonCuts returns a cut after each semicolon in text[start:end], which
// holds no statement: the semicolons the reader passes over between two
// statements, which end an empty piece each.
func semicolonCuts(text string, start, end int, options lexer.Options) []transactSQLCut {
	if start >= end {
		return nil
	}
	var cuts []transactSQLCut
	lexr := lexer.NewLexerWithOptions(text[start:end], options)
	for token := lexr.NextToken(); token.Type != lexer.TokenEOF; token = lexr.NextToken() {
		if token.Type == lexer.TokenSemicolon {
			cuts = append(cuts, transactSQLCut{end: start + token.End, semicolon: start + token.Start})
		}
	}
	return cuts
}

// compoundCuts returns the cuts the compound-body reader makes in
// text[start:end], at most limit of them when limit is positive: one after each
// semicolon outside the body of a routine (see [sqlcompound.State]).
func compoundCuts(text string, start, end int, dialect string, options lexer.Options, limit int) []transactSQLCut {
	var cuts []transactSQLCut
	state := sqlcompound.New(dialect)
	lexr := lexer.NewLexerWithOptions(text[start:end], options)
	for token := lexr.NextToken(); token.Type != lexer.TokenEOF; token = lexr.NextToken() {
		if token.Type != lexer.TokenSemicolon {
			observeToken(&state, token)
			continue
		}
		if state.KeepSemicolonInsideStatement() {
			continue
		}
		cuts = append(cuts, transactSQLCut{end: start + token.End, semicolon: start + token.Start})
		state.Reset()
		if len(cuts) == limit {
			break
		}
	}
	return cuts
}

// transactSQLPieces collects the pieces of one batch of text, the script sql
// with its client delimiters rewritten (see [clientDelimitersFor]).
type transactSQLPieces struct {
	sql     string
	text    string
	origins clientdelimiter.Origins
	sent    []string
	sources []SourceStatement
}

// cut adds the pieces cuts make of batch. A piece is sent trimmed and without
// the semicolon that ends it, unless the cut keeps it; a piece with nothing in
// it is not sent. Its source runs from the end of the piece before it through
// its semicolon.
func (p *transactSQLPieces) cut(batch transactSQLBatch, cuts []transactSQLCut) {
	previous := batch.start
	for _, cut := range cuts {
		body := p.text[previous:cut.end]
		if cut.semicolon >= 0 && !cut.keep {
			body = p.text[previous:cut.semicolon]
		}
		p.add(body, SourceStatement{
			Text:            p.text[previous:cut.end],
			Terminated:      cut.semicolon >= 0,
			ClientDelimited: cut.semicolon >= 0 && standsForClientDelimiter(p.sql, p.origins, cut.semicolon),
		})
		previous = cut.end
	}
	p.add(p.text[previous:batch.end], SourceStatement{Text: p.text[previous:batch.end]})
}

func (p *transactSQLPieces) add(body string, source SourceStatement) {
	if body = strings.TrimSpace(body); body != "" {
		p.sent = append(p.sent, body)
	}
	p.sources = appendSourceStatement(p.sources, source)
}
