package clickhouse

import (
	"strings"

	"ptah.run/internal/lexer"
)

// engineFullClauses is one system.tables.engine_full string, split into the
// engine and the clauses ClickHouse appends after it.
//
// The four key clauses are carried for the same reason the parse has to
// recognize them at all -- they stand between the engine and the two clauses
// this type exists for -- and they are what a test can compare against
// system.tables.partition_key and its neighbors, which the reader takes those
// values from.
type engineFullClauses struct {
	Engine      string
	PartitionBy string
	PrimaryKey  string
	OrderBy     string
	SampleBy    string
	TTL         string
	Settings    string
}

// engineFullClauseSpec is one clause header: the words that introduce it and
// where its body goes.
type engineFullClauseSpec struct {
	words  []string
	assign func(*engineFullClauses, string)
}

// engineFullClauseOrder is the order system.tables.engine_full emits clauses in.
//
// Measured on ClickHouse 26.7.5.10, a table declaring all six reports
//
//	MergeTree PARTITION BY toYYYYMM(d) PRIMARY KEY (id) ORDER BY (id, s)
//	SAMPLE BY id TTL d + toIntervalDay(30) SETTINGS index_granularity = 4096
//
// The order matters to the parse: a clause is only looked for once the ones
// before it have been passed, so a word that could open an earlier clause
// cannot reopen it from inside a later one.
var engineFullClauseOrder = []engineFullClauseSpec{
	{words: []string{"PARTITION", "BY"}, assign: func(c *engineFullClauses, body string) { c.PartitionBy = body }},
	{words: []string{"PRIMARY", "KEY"}, assign: func(c *engineFullClauses, body string) { c.PrimaryKey = body }},
	{words: []string{"ORDER", "BY"}, assign: func(c *engineFullClauses, body string) { c.OrderBy = body }},
	{words: []string{"SAMPLE", "BY"}, assign: func(c *engineFullClauses, body string) { c.SampleBy = body }},
	{words: []string{"TTL"}, assign: func(c *engineFullClauses, body string) { c.TTL = body }},
	{words: []string{"SETTINGS"}, assign: func(c *engineFullClauses, body string) { c.Settings = body }},
}

// parseEngineFull splits engine_full into the engine and its clauses.
//
// It reads tokens rather than searching for keywords in the text, because a
// clause keyword is also a legal column name and the two are only told apart by
// where they stand. Measured on ClickHouse 26.7.5.10:
//
//	CREATE TABLE t (id Int64, settings Int64) ENGINE = MergeTree ORDER BY settings
//	-> engine_full: MergeTree ORDER BY settings SETTINGS index_granularity = 8192
//
// Searching for `SETTINGS` finds the column, so the settings clause was read as
// the text `SETTINGS index_granularity = 8192` and the description replayed
// `SETTINGS SETTINGS index_granularity = 8192`, which no server accepts. The
// mirror case lost a clause instead: with `PARTITION BY settings ORDER BY id`
// the search stopped at the column, the body after it began with `ORDER BY`,
// and the settings clause came back empty (stokaro/ptah#2198).
//
// What separates the two is that a clause keyword never stands where an operand
// is expected. `ORDER BY` is followed by an expression, so the identifier right
// after it is a column however it is spelled; the next word that is *not* part
// of that expression opens the next clause. Nesting is tracked for the same
// reason it was before: `toYYYYMM(ttl)` holds no clause.
func parseEngineFull(engineFull string) engineFullClauses {
	tokens := engineFullTokens(engineFull)

	var out engineFullClauses
	next := 0
	depth := 0
	operandExpected := true

	openSpec := -1
	bodyStart := 0
	engineEnd := len(engineFull)

	closeOpen := func(end int) {
		if openSpec < 0 {
			return
		}
		engineFullClauseOrder[openSpec].assign(&out, strings.TrimSpace(engineFull[bodyStart:end]))
	}

	for i := 0; i < len(tokens); i++ {
		token := tokens[i]

		if token.Type == lexer.TokenOperator {
			switch token.Value {
			case "(":
				depth++
			case ")":
				depth--
			}
			// A closing parenthesis ends an operand; every other operator,
			// including the comma between key expressions, opens one.
			operandExpected = token.Value != ")"
			continue
		}

		if depth == 0 && !operandExpected {
			if spec, consumed := matchEngineFullClause(tokens[i:], next); spec >= 0 {
				if openSpec < 0 {
					engineEnd = token.Start
				}
				closeOpen(token.Start)

				openSpec = spec
				bodyStart = tokens[i+consumed-1].End
				next = spec + 1
				i += consumed - 1
				operandExpected = true
				continue
			}
		}

		operandExpected = false
	}

	closeOpen(len(engineFull))
	out.Engine = strings.TrimSpace(engineFull[:engineEnd])
	return out
}

// matchEngineFullClause reports which clause the tokens open, searching only
// the clauses that may still follow, and how many tokens its header takes.
func matchEngineFullClause(tokens []lexer.Token, from int) (spec, consumed int) {
	for index := from; index < len(engineFullClauseOrder); index++ {
		words := engineFullClauseOrder[index].words
		if len(tokens) < len(words) {
			continue
		}
		matched := true
		for offset, word := range words {
			if !tokens[offset].MatchIdentifierValue(word) {
				matched = false
				break
			}
		}
		if matched {
			return index, len(words)
		}
	}
	return -1, 0
}

// engineFullTokens lexes engine_full, dropping the tokens that carry no
// structure.
//
// The options are ClickHouse's: it doubles a quote to escape it inside a
// literal and also honors a backslash escape, so a settings value holding a
// quote does not run the lexer into the rest of the statement.
func engineFullTokens(engineFull string) []lexer.Token {
	lex := lexer.NewLexerWithOptions(engineFull, lexer.Options{
		StandardStrings:  true,
		BackslashEscapes: true,
	})

	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		tokens = append(tokens, token)
	}
}
