package lintscan

import (
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlserverbatch"
)

// splitTransactSQL reads each GO-delimited batch before exposing statements
// to the rules. Splitting at semicolons first would break control flow and
// hide a statement whose boundary requires no delimiter.
func splitTransactSQL(sql string, mode Mode) []lintext.Statement {
	tokens := batchEnds(sql, scanSQL(sql, mode), mode)
	locations := transactSQLLocations(tokens)
	start := 0
	if strings.HasPrefix(sql, "\uFEFF") {
		start = len("\uFEFF")
	}
	var statements []lintext.Statement
	for _, token := range tokens {
		if token.kind != tokBatchEnd {
			continue
		}
		statements = append(statements, transactSQLBatch(sql[start:token.start], start, mode, locations)...)
		start = token.end
	}
	return append(statements, transactSQLBatch(sql[start:], start, mode, locations)...)
}

// transactSQLBatch reads the pieces the migrator sends before looking inside
// them. A semicolon after a routine's outer END closes its piece, so the next
// statement is executable even without GO. A routine body within one piece
// stays stored text.
func transactSQLBatch(sql string, offset int, mode Mode, locations map[int]transactSQLLocation) []lintext.Statement {
	pieces, ok := transactSQLPieces(sql, offset, mode, locations)
	if !ok {
		return transactSQLFallback(sql, offset, mode, locations)
	}
	var statements []lintext.Statement
	for _, piece := range pieces {
		statements = append(statements, transactSQLPiece(sql[piece.start:piece.end], offset+piece.start, mode, locations)...)
	}
	return statements
}

// transactSQLPieces projects the emitted source text back onto the file.
// Text is verbatim except for a client delimiter's synthetic final semicolon,
// which belongs to no source range. An unprojectable rewrite keeps the
// delimiter-based analysis rather than losing any input.
func transactSQLPieces(sql string, offset int, mode Mode, locations map[int]transactSQLLocation) ([]transactSQLRange, bool) {
	var pieces []transactSQLRange
	after := 0
	for _, piece := range sqlutil.SplitSourceStatements(sql, mode.dialect) {
		text := piece.Text
		if piece.ClientDelimited {
			text = strings.TrimSuffix(text, ";")
		}
		start := transactSQLPieceStart(sql, text, after, offset, locations)
		if start < 0 {
			return nil, false
		}
		after = start + len(text)
		pieces = append(pieces, transactSQLRange{start: start, end: after})
	}
	return pieces, true
}

// transactSQLPieceStart matches only at a code token, never in a preceding
// comment or quoted value that happens to contain the same SQL text.
func transactSQLPieceStart(sql, text string, after, offset int, locations map[int]transactSQLLocation) int {
	for start := after; start < len(sql); start++ {
		matched := strings.Index(sql[start:], text)
		if matched < 0 {
			return -1
		}
		start += matched
		if _, ok := locations[offset+start]; ok {
			return start
		}
	}
	return -1
}

// transactSQLPiece maps the reader's ranges back to the file. An unreadable
// piece keeps its parse diagnostics through the delimiter-based analysis.
func transactSQLPiece(sql string, offset int, mode Mode, locations map[int]transactSQLLocation) []lintext.Statement {
	parsed, err := sqlserverbatch.Read(sql)
	if err != nil {
		return transactSQLFallback(sql, offset, mode, locations)
	}
	var statements []lintext.Statement
	for _, span := range transactSQLRanges(parsed) {
		start, end := offset+span.start, offset+span.end
		location := locations[start]
		statements = append(statements, statement(sql[span.start:span.end], start, end, location.line, location.comments, mode))
	}
	return statements
}

func transactSQLFallback(sql string, offset int, mode Mode, locations map[int]transactSQLLocation) []lintext.Statement {
	statements := splitSemicolons(sql, mode)
	for i := range statements {
		stmt := &statements[i]
		stmt.Start += offset
		stmt.End += offset
		location := locations[stmt.Start]
		stmt.Line, stmt.Comments = location.line, location.comments
	}
	return statements
}

type transactSQLRange struct {
	start, end int
}

// transactSQLRanges keeps a condition or schema header separate from its
// statements. The parser can report an unread condition, and a directive
// above a header belongs only to that header. INSERT ... EXEC stays whole:
// its prefix is not a complete INSERT. A Module stores its body rather than
// executing it.
func transactSQLRanges(statements []sqlserverbatch.Statement) []transactSQLRange {
	var spans []transactSQLRange
	for _, stmt := range statements {
		switch stmt.Kind {
		case sqlserverbatch.Block:
			spans = append(spans, transactSQLRanges(stmt.Statements)...)
		case sqlserverbatch.If, sqlserverbatch.While, sqlserverbatch.Schema:
			spans = append(spans, transactSQLRange{start: stmt.Start, end: stmt.Start + len(strings.TrimSuffix(stmt.Head, ";"))})
			spans = append(spans, transactSQLRanges(stmt.Statements)...)
		default:
			end := stmt.End
			if stmt.Kind != sqlserverbatch.Module && strings.HasSuffix(stmt.SQL, ";") {
				end--
			}
			spans = append(spans, transactSQLRange{start: stmt.Start, end: end})
		}
	}
	return spans
}

type transactSQLLocation struct {
	line     int
	comments []string
}

// transactSQLLocations attaches comments before ranges are flattened. A code
// token, a blank line or GO detaches pending comments; a comment on the line
// of the preceding code is not the next statement's suppression directive.
func transactSQLLocations(tokens []lintToken) map[int]transactSQLLocation {
	locations := make(map[int]transactSQLLocation)
	var pending []string
	lastCodeLine := 0
	for _, token := range tokens {
		switch token.kind {
		case tokWhitespace:
			if strings.Count(token.text, "\n") > 1 {
				pending = nil
			}
		case tokComment:
			if token.line != lastCodeLine {
				pending = append(pending, token.text)
			}
		case tokBatchEnd:
			pending = nil
			lastCodeLine = token.line
		default:
			locations[token.start] = transactSQLLocation{line: token.line, comments: pending}
			pending = nil
			lastCodeLine = token.line + strings.Count(token.text, "\n")
		}
	}
	return locations
}

// transactSQLTokenKind keeps quoted names and literals atomic, including a
// bracketed name that contains a semicolon or a line spelled GO. Unknown text
// remains a token so an incomplete parse cannot look like an empty file.
func transactSQLTokenKind(token lexer.Token) lintTokenKind {
	switch token.Type {
	case lexer.TokenWhitespace:
		return tokWhitespace
	case lexer.TokenComment:
		return tokComment
	case lexer.TokenString:
		if strings.HasPrefix(token.Value, `"`) {
			return tokQuotedIdent
		}
		return tokString
	case lexer.TokenSemicolon:
		return tokSemicolon
	case lexer.TokenIdentifier:
		if strings.HasPrefix(token.Value, "[") {
			return tokQuotedIdent
		}
		return tokWord
	default:
		return tokOp
	}
}
