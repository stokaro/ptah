// Package yqlquery splits a YQL text into the queries YDB runs it as.
//
// YDB cannot run an arbitrary script as one query. A scheme statement --
// CREATE, ALTER, DROP, GRANT, REVOKE, TRUNCATE and the rest -- is refused inside
// a transaction and cannot share a query with a data statement, and a query of
// several scheme statements is not atomic and compiles every statement against
// the schema as it stood before the query. A named expression (`$x = ...`), an
// action (DEFINE ACTION), a DECLARE and a PRAGMA, on the other hand, exist only
// in the query that holds them. Measured on YDB 26.2.1.14:
//
//	$x = 9l;                                  one query: accepted
//	UPSERT INTO t (id) VALUES ($x);           the next query: Unknown name: $x
//	UPSERT ...; TRUNCATE TABLE t;             Queries with mixed data and scheme operations are not supported
//	$t = "dir/x"; CREATE TABLE $t (...);      accepted, as one query
//
// So [Split] makes each scheme statement a query of its own and each run of
// consecutive data statements one query, and it carries every definition --
// PRAGMA, DECLARE, a named expression, DEFINE ACTION and DEFINE SUBQUERY -- into
// every query that follows it in the text, which then sees what the text
// declared above it, as it would if the whole text were one query. A
// translation setting at the head of the text (`--!syntax_v1`) is honored by
// YDB only at the head of a query, so it heads every query.
//
// The writer in internal/dbschema/ydb and the versioned migrator both run YQL
// through this split, so a statement the migrator counts and records as one
// query is the query the writer sends.
package yqlquery

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/clientdelimiter"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Kind says how YDB runs a query.
type Kind uint8

const (
	// Data is a query of data statements: it runs as one transaction, which
	// commits whole or not at all.
	Data Kind = iota + 1
	// Scheme is a query of one scheme statement, which YDB runs outside any
	// transaction.
	Scheme
)

// String names the kind for a message.
func (k Kind) String() string {
	switch k {
	case Data:
		return "data"
	case Scheme:
		return "scheme"
	default:
		return "unknown"
	}
}

// Query is one query a YQL text runs as.
type Query struct {
	// Kind says whether the query is a scheme statement or data statements.
	Kind Kind
	// Text is what is sent to YDB: the translation settings at the head of
	// the text, the definitions the text made before this query, and this
	// query's own statements, separated by semicolons.
	Text string
	// Statements are the query's own statements as they run, without the
	// definitions carried in from above it, comments removed and with no
	// terminator.
	Statements []string
	// Source is the source text of the query's own statements, as written
	// from each one's first token through its semicolon, one per line. It
	// is what a digest of the query covers.
	Source string
}

// ErrTranslationSetting is returned for a translation setting other than
// --!syntax_v1 at the head of a text.
var ErrTranslationSetting = errors.New("unsupported YQL translation setting")

// ErrClientDelimiter is returned for a client delimiter directive in a text.
var ErrClientDelimiter = errors.New("client delimiter directive in YQL")

// supportedSetting is the one translation setting Ptah reads a text under.
//
// Measured on YDB 26.2.1.14: --!syntax_v1 is accepted, --!syntax_pg answers
// `PG syntax is disabled`, --!syntax_v0 answers `V0 syntax is disabled`, and a
// name YDB does not know -- --!antlr4 is one -- answers `Unknown SQL
// translation setting`. --!ansi_lexer is accepted and changes how every
// string, name and comment after it is read, which Ptah's lexer does not
// model, so a split under it would not be the server's.
const supportedSetting = "--!syntax_v1"

// Split returns the queries text runs as, in order. A text with no statement
// returns no query.
//
// It refuses what it cannot split the way YDB reads the text: a translation
// setting other than --!syntax_v1 ([ErrTranslationSetting]), and a client
// delimiter directive ([ErrClientDelimiter]). YQL has no client delimiter, so
// `DELIMITER $$` and `-- atlas:delimiter $$` would be ignored -- the text
// split at its semicolons -- and the author's intent with them.
func Split(text string) ([]Query, error) {
	settings, err := translationSettings(text)
	if err != nil {
		return nil, err
	}
	if err := refuseClientDelimiters(text); err != nil {
		return nil, err
	}

	header := ""
	if len(settings) > 0 {
		header = strings.Join(settings, "\n") + "\n"
	}
	builder := splitter{header: header}
	for _, source := range sqlutil.SplitSourceStatements(text, platform.YDB) {
		statement := executable(source.Text)
		if statement == "" {
			continue
		}
		builder.add(statement, strings.TrimSpace(source.Text))
	}
	return builder.finish(), nil
}

// splitter accumulates the queries of one text.
type splitter struct {
	header string
	// carried are the definitions made so far, in order.
	carried []string
	// pendingSources are the sources of the definitions made since the last
	// query closed. They belong to the next query, which the definitions
	// head through carried.
	pendingSources []string
	pendingCount   int
	// open is the run of data statements being collected, if any.
	open   *draft
	drafts []draft
}

// draft is a query being assembled: the definitions that head it, its own
// statements and their source.
type draft struct {
	kind       Kind
	prefix     []string
	statements []string
	source     string
}

func (s *splitter) add(statement, source string) {
	switch classify(statement) {
	case definition:
		s.carried = append(s.carried, statement)
		if s.open != nil {
			s.open.statements = append(s.open.statements, statement)
			s.open.source = joinSource(s.open.source, source)
			return
		}
		s.pendingSources = append(s.pendingSources, source)
		s.pendingCount++
	case Scheme:
		s.close()
		query := s.start(Scheme)
		query.statements = append(query.statements, statement)
		query.source = joinSource(query.source, source)
		s.drafts = append(s.drafts, query)
	case Data:
		if s.open == nil {
			query := s.start(Data)
			s.open = &query
		}
		s.open.statements = append(s.open.statements, statement)
		s.open.source = joinSource(s.open.source, source)
	}
}

// start opens a query of kind, headed by the definitions made before it, and
// holding the pending definitions' source as the start of its own.
func (s *splitter) start(kind Kind) draft {
	query := draft{kind: kind, prefix: s.carried[:len(s.carried):len(s.carried)],
		source: strings.Join(s.pendingSources, "\n")}
	s.pendingSources = nil
	s.pendingCount = 0
	return query
}

// close ends the run of data statements being collected.
func (s *splitter) close() {
	if s.open == nil {
		return
	}
	s.drafts = append(s.drafts, *s.open)
	s.open = nil
}

// finish closes the last run and places definitions that no statement
// followed. They join the last query, after its statements, where they run
// and change nothing; a text of nothing but definitions is one data query.
func (s *splitter) finish() []Query {
	// An open run already holds every definition made inside it, so pending
	// definitions exist only when no run is open.
	s.close()
	if s.pendingCount > 0 {
		trailing := s.carried[len(s.carried)-s.pendingCount:]
		if len(s.drafts) == 0 {
			s.drafts = append(s.drafts, draft{kind: Data})
		}
		last := &s.drafts[len(s.drafts)-1]
		last.statements = append(last.statements, trailing...)
		last.source = joinSource(last.source, strings.Join(s.pendingSources, "\n"))
	}
	queries := make([]Query, 0, len(s.drafts))
	for _, query := range s.drafts {
		parts := append(slices.Clone(query.prefix), query.statements...)
		queries = append(queries, Query{
			Kind:       query.kind,
			Text:       s.header + strings.Join(parts, ";\n"),
			Statements: query.statements,
			Source:     query.source,
		})
	}
	return queries
}

func joinSource(existing, next string) string {
	if existing == "" {
		return next
	}
	return existing + "\n" + next
}

// definition classifies a statement that defines something for the
// statements after it in the same query.
const definition Kind = 0

// schemeVerbs are the first keywords of a YQL scheme statement.
var schemeVerbs = []string{
	"CREATE", "ALTER", "DROP", "GRANT", "REVOKE", "TRUNCATE", "ANALYZE", "BACKUP", "RESTORE",
}

// definitionVerbs are the first keywords of a statement that defines a name or
// a setting for the rest of its query.
var definitionVerbs = []string{"PRAGMA", "DECLARE", "DEFINE", "IMPORT"}

// classify reads a statement's kind from its leading tokens.
func classify(statement string) Kind {
	tokens := significantTokens(statement, 2)
	if len(tokens) == 0 {
		return Data
	}
	first := tokens[0]
	if first.Type == lexer.TokenIdentifier && strings.HasPrefix(first.Value, "$") {
		// A statement that starts with a named expression assigns it; YQL
		// has no other statement that opens with one.
		return definition
	}
	for _, verb := range definitionVerbs {
		if first.MatchIdentifierValue(verb) {
			return definition
		}
	}
	for _, verb := range schemeVerbs {
		if first.MatchIdentifierValue(verb) {
			return Scheme
		}
	}
	if (first.MatchIdentifierValue("UPSERT") || first.MatchIdentifierValue("REPLACE")) &&
		len(tokens) > 1 && tokens[1].MatchIdentifierValue("OBJECT") {
		// UPSERT OBJECT writes a scheme object, not a row.
		return Scheme
	}
	return Data
}

// KindOf returns the kind of query text runs as. It reports false when text
// runs as no query or as more than one.
func KindOf(text string) (Kind, bool) {
	queries, err := Split(text)
	if err != nil || len(queries) != 1 {
		return 0, false
	}
	return queries[0].Kind, true
}

// executable is a statement's text as it runs: comments removed and its
// terminator dropped.
func executable(source string) string {
	stripped := strings.TrimSpace(sqlutil.StripCommentsForDialect(source, platform.YDB))
	stripped = strings.TrimSuffix(stripped, ";")
	return strings.TrimSpace(stripped)
}

// translationSettings returns the --! settings at the head of text, and
// refuses one Ptah does not read text under.
func translationSettings(text string) ([]string, error) {
	lexr := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.YDB))
	var settings []string
	for {
		token := lexr.NextToken()
		switch {
		case token.Type == lexer.TokenWhitespace:
			continue
		case token.Type == lexer.TokenUnknown && strings.HasPrefix(token.Value, "--!"):
			setting := strings.TrimSpace(token.Value)
			if setting != supportedSetting {
				return nil, fmt.Errorf("%w %q: Ptah splits YQL as --!syntax_v1 reads it, "+
					"and YDB would read this text differently or refuse it; remove the line", ErrTranslationSetting, setting)
			}
			settings = append(settings, setting)
		default:
			return settings, nil
		}
	}
}

// refuseClientDelimiters refuses a delimiter directive written as a comment or
// as a statement of its own. A directive inside a string literal is text, and
// the lexer reads it as part of the literal.
func refuseClientDelimiters(text string) error {
	lexr := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.YDB))
	statementStart := true
	for {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return nil
		case lexer.TokenWhitespace:
			continue
		case lexer.TokenComment:
			if _, form, ok := clientdelimiter.Parse(token.Value); ok && form == clientdelimiter.Atlas {
				return delimiterRefusal(token.Value)
			}
			continue
		case lexer.TokenSemicolon:
			statementStart = true
			continue
		}
		if token.Type == lexer.TokenUnknown && strings.HasPrefix(token.Value, "--!") {
			// A translation setting heads the text and starts no statement.
			continue
		}
		if statementStart && token.MatchIdentifierValue("DELIMITER") {
			return delimiterRefusal(lineAt(text, token.Start))
		}
		statementStart = false
	}
}

func delimiterRefusal(line string) error {
	return fmt.Errorf("%w: %q has no meaning in YQL, which ends every statement with a semicolon; "+
		"remove the directive and terminate each statement with a semicolon", ErrClientDelimiter,
		strings.TrimSpace(line))
}

// lineAt returns the line of text that contains offset.
func lineAt(text string, offset int) string {
	start := strings.LastIndexByte(text[:offset], '\n') + 1
	end := strings.IndexByte(text[offset:], '\n')
	if end < 0 {
		return text[start:]
	}
	return text[start : offset+end]
}

// significantTokens returns up to limit tokens of statement that are not
// whitespace or comments.
func significantTokens(statement string, limit int) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.YDB))
	var tokens []lexer.Token
	for len(tokens) < limit {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		default:
			tokens = append(tokens, token)
		}
	}
	return tokens
}
