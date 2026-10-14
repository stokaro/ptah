package clickhouse

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/lexer"
)

// viewStorageLimits records the storage of each materialized view the read
// returns that is not stored the way Ptah creates one, `ENGINE = MergeTree
// ORDER BY tuple()`. statements holds each view's create_table_query, in the
// same order.
//
// Ptah's model of a materialized view has no place for its storage: the
// renderer creates every view with that one clause, and a description of a read
// writes it too. Without the limit, a view the server stores with
// `ENGINE = MergeTree ORDER BY c` reads back as `ORDER BY tuple()`, and a
// comparison against that description reports the database synchronized, so
// the real storage is lost without a word. A view created with `TO <table>`
// sends its rows to a table of its own choosing, and a replacement would
// recreate it with inner storage, so inserts would stop reaching that table.
// Each such view is an unrepresentable subject of [chschema.ViewStorageKind],
// and the reason names the clause the server stores.
//
// The clause comes from create_table_query, the one column that holds it:
// system.tables reports both forms with engine MaterializedView and the same
// as_select. Measured on 24.10 and 26.9, the stored statement writes the
// storage after the column list and before an optional DEFINER, SQL SECURITY
// and COMMENT, and the server adds `SETTINGS index_granularity = 8192` to an
// engine that states no settings, so that setting alone is still Ptah's
// storage.
func viewStorageLimits(views []catalog.MaterializedView, statements []string) []schemaext.SubjectCoverage {
	identities := objectidentity.NewBuilder(identifier.ForDialect(platform.ClickHouse))
	var limits []schemaext.SubjectCoverage
	for i, view := range views {
		reason, described := viewStorageReason(statements[i])
		if described {
			continue
		}
		limits = append(limits, schemaext.SubjectCoverage{
			Kind: chschema.ViewStorageKind, Subject: identities.SchemaScopedParts(objectidentity.KindMatView, view.Schema, view.Name),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: reason},
		})
	}
	return limits
}

// viewStorageReason reports whether a stored CREATE MATERIALIZED VIEW keeps its
// rows the way Ptah creates a view, and otherwise why the read cannot describe
// its storage.
func viewStorageReason(statement string) (string, bool) {
	clause, engine := storedViewStorage(statement)
	switch {
	case clause == "":
		return "the storage clause of the stored CREATE statement could not be read", false
	case !engine:
		return fmt.Sprintf("the view sends its rows to another table with %s, which Ptah does not model", clause), false
	case defaultViewStorage(strings.TrimSpace(strings.TrimPrefix(clause, "ENGINE ="))):
		return "", true
	default:
		return fmt.Sprintf("the view stores its rows with %s, which Ptah does not model", clause), false
	}
}

// storedViewStorage returns the storage clause of a stored CREATE MATERIALIZED
// VIEW, `ENGINE = <engine and its clauses>` or `TO <table>`, and whether it is
// an engine. It returns an empty clause when the statement has neither before
// the AS that begins the query.
func storedViewStorage(statement string) (string, bool) {
	tokens := engineFullTokens(statement)
	depth := 0
	for i, token := range tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case depth != 0:
		case token.MatchIdentifierValue("AS"):
			return "", false
		case token.MatchIdentifierValue("TO") && i+1 < len(tokens):
			return "TO " + strings.TrimSpace(statement[tokens[i+1].Start:tokens[viewClauseEnd(tokens, i+1, true)].Start]), false
		case token.MatchIdentifierValue("ENGINE") && i+2 < len(tokens) && tokens[i+1].MatchOperatorValue("="):
			return "ENGINE = " + strings.TrimSpace(statement[tokens[i+2].Start:tokens[viewClauseEnd(tokens, i+2, false)].Start]), true
		}
	}
	return "", false
}

// viewClauseEnd returns the index of the first token after a storage clause
// that begins at start: the AS before the query, or a DEFINER, SQL SECURITY or
// COMMENT clause, outside parentheses. A TO target also ends at the column
// list the server writes after it. A statement with none of them has no query,
// which a stored view always has, so the scan stops at the last token.
func viewClauseEnd(tokens []lexer.Token, start int, target bool) int {
	depth := 0
	for i := start; i < len(tokens); i++ {
		token := tokens[i]
		next := func(match func(lexer.Token) bool) bool { return i+1 < len(tokens) && match(tokens[i+1]) }
		switch {
		case depth == 0 && target && token.MatchOperatorValue("("):
			return i
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case depth != 0:
		case token.MatchIdentifierValue("AS"),
			token.MatchIdentifierValue("DEFINER") && next(func(t lexer.Token) bool { return t.MatchOperatorValue("=") }),
			token.MatchIdentifierValue("SQL") && next(func(t lexer.Token) bool { return t.MatchIdentifierValue("SECURITY") }),
			token.MatchIdentifierValue("COMMENT") && next(func(t lexer.Token) bool { return t.Type == lexer.TokenString }):
			return i
		}
	}
	return len(tokens) - 1
}

// defaultViewStorage reports whether an engine and its clauses are the
// storage Ptah creates a view with: MergeTree sorted by tuple(), with no other
// clause and no setting but the index_granularity = 8192 the server adds.
func defaultViewStorage(engine string) bool {
	clauses := parseEngineFull(engine)
	return slices.Equal(tokenValues(clauses.Engine), []string{"MergeTree"}) &&
		slices.Equal(tokenValues(clauses.OrderBy), []string{"tuple", "(", ")"}) &&
		clauses.PartitionBy == "" && clauses.PrimaryKey == "" && clauses.SampleBy == "" && clauses.TTL == "" &&
		(clauses.Settings == "" || slices.Equal(tokenValues(clauses.Settings), []string{"index_granularity", "=", "8192"}))
}

func tokenValues(text string) []string {
	var values []string
	for _, token := range engineFullTokens(text) {
		values = append(values, token.Value)
	}
	return values
}
