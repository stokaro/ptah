package migrator

import (
	"context"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/sqlident"
)

// postgresIndexRef is one index a migration's up SQL creates, spelled the way
// the PostgreSQL catalog stores it. Schema is empty when the statement left the
// name unqualified, which means PostgreSQL resolved it through the search path.
type postgresIndexRef struct {
	Schema string
	Name   string
}

// postgresUnusableIndex is a catalog row for an index PostgreSQL will not use:
// indisvalid or indisready is false. A failed CREATE INDEX CONCURRENTLY leaves
// exactly that state behind, and the leftover keeps the name occupied, so a
// re-run of the same IF NOT EXISTS statement is skipped rather than retried.
type postgresUnusableIndex struct {
	Schema string
	Name   string
	Valid  bool
	Ready  bool
}

func (i postgresUnusableIndex) quotedName() string {
	return sqlident.Qualified(platform.Postgres, i.Schema, i.Name)
}

// refuseRepairOverUnusableIndex refuses to record a migration applied while
// PostgreSQL still reports an index that migration creates as unusable.
//
// A concurrent unique index build that fails partway leaves an invalid index
// behind. Because it occupies the name, re-issuing the generated
// CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS is skipped with a notice
// instead of retried, so nothing errors and the revision row would be written
// as applied over a constraint the database is not enforcing. Refusing leaves
// the operator with a dirty state they can still see.
//
// The probe is PostgreSQL-only by nature: no other supported dialect has a
// concurrent index build that can be left half-finished, so every other dialect
// returns here before any query runs and keeps its existing code path. This
// deliberately ignores opts.Force. --force is documented as "Rewrite or create
// the revision row even when it is not dirty" -- it relaxes a precondition
// about the metadata, not a fact about the database. The escape hatch is
// REINDEX INDEX CONCURRENTLY, which fixes the database rather than the
// bookkeeping about it.
func (m *Migrator) refuseRepairOverUnusableIndex(ctx context.Context, migration *Migration) error {
	if m.conn == nil || platform.NormalizeDialect(m.conn.Info().Dialect) != platform.Postgres {
		return nil
	}
	refs := postgresCreatedIndexNames(migration.UpSQL)
	if len(refs) == 0 {
		return nil
	}
	unusable, err := m.postgresUnusableIndexes(ctx, refs)
	if err != nil {
		return err
	}
	if len(unusable) == 0 {
		return nil
	}
	return unusableIndexRepairError(migration.Version, unusable)
}

// unusableIndexRepairError renders the refusal. It names every index that is
// unusable together with the catalog flags that say so, and points at the
// PostgreSQL command that rebuilds one without holding writes.
func unusableIndexRepairError(version int64, unusable []postgresUnusableIndex) error {
	details := make([]string, 0, len(unusable))
	rebuild := make([]string, 0, len(unusable))
	for _, index := range unusable {
		details = append(details, fmt.Sprintf(
			"%s (indisvalid=%t, indisready=%t)",
			index.quotedName(), index.Valid, index.Ready,
		))
		rebuild = append(rebuild, "REINDEX INDEX CONCURRENTLY "+index.quotedName())
	}
	noun := "index"
	if len(unusable) > 1 {
		noun = "indexes"
	}
	return fmt.Errorf(
		"migration %d cannot be repaired: PostgreSQL reports %s %s unusable, "+
			"so recording the migration applied would report a constraint that is not enforced; "+
			"run %s, or drop the %s and rerun the migration, then repair again",
		version,
		noun,
		strings.Join(details, ", "),
		strings.Join(rebuild, "; "),
		noun,
	)
}

// postgresUnusableIndexes returns the subset of refs the catalog reports as
// unusable. An unqualified ref is resolved through pg_table_is_visible, which
// is the same search-path rule PostgreSQL applied when the statement ran, so a
// same-named invalid index parked in an unrelated schema is not this repair's
// business and does not block it.
func (m *Migrator) postgresUnusableIndexes(ctx context.Context, refs []postgresIndexRef) ([]postgresUnusableIndex, error) {
	predicates := make([]string, 0, len(refs))
	args := make([]any, 0, len(refs)*2)
	for _, ref := range refs {
		if ref.Schema == "" {
			predicates = append(predicates, "(i.relname = ? AND pg_catalog.pg_table_is_visible(i.oid))")
			args = append(args, ref.Name)
			continue
		}
		predicates = append(predicates, "(i.relname = ? AND n.nspname = ?)")
		args = append(args, ref.Name, ref.Schema)
	}

	query := sqlutil.Rebind(m.conn.Info().Dialect, fmt.Sprintf(`
		SELECT n.nspname, i.relname, ix.indisvalid, ix.indisready
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE NOT (ix.indisvalid AND ix.indisready)
		AND (%s)
		ORDER BY n.nspname, i.relname`, strings.Join(predicates, " OR ")))

	rows, err := m.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect index validity: %w", err)
	}
	defer rows.Close()

	var unusable []postgresUnusableIndex
	for rows.Next() {
		var index postgresUnusableIndex
		if err := rows.Scan(&index.Schema, &index.Name, &index.Valid, &index.Ready); err != nil {
			return nil, fmt.Errorf("failed to scan index validity: %w", err)
		}
		unusable = append(unusable, index)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read index validity: %w", err)
	}
	return unusable, nil
}

// postgresCreatedIndexNames returns every index name created by a statement in
// sqlText, in order and without duplicates.
//
// The scan is token-based rather than textual: names are read as whole
// identifier tokens, so idx_members can never be recognized in a statement that
// creates idx_members_email, and a name that appears in a comment or inside a
// string literal is not a name at all. Both spellings this repository generates
// are handled -- the quoted "idx_members_email" keeps its bytes, and a bare
// idx_members_email folds to lower case exactly as PostgreSQL folds it when
// storing the catalog entry.
func postgresCreatedIndexNames(sqlText string) []postgresIndexRef {
	tokens := significantSQLTokens(sqlText, platform.Postgres)
	var (
		refs []postgresIndexRef
		seen = make(map[postgresIndexRef]struct{})
	)
	for i, token := range tokens {
		// CREATE must open a statement. Anywhere else the word is part of some
		// larger construct, not a DDL verb whose object name follows.
		if i > 0 && tokens[i-1].Type != lexer.TokenSemicolon {
			continue
		}
		if !token.MatchIdentifierValue("CREATE") {
			continue
		}
		ref, ok := parseCreateIndexName(tokens[i+1:])
		if !ok {
			continue
		}
		if _, duplicate := seen[ref]; duplicate {
			continue
		}
		seen[ref] = struct{}{}
		refs = append(refs, ref)
	}
	return refs
}

// parseCreateIndexName reads the index name out of the tail of a CREATE
// statement, given the tokens that follow the CREATE keyword. It follows the
// PostgreSQL grammar
//
//	CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] name ON ...
//
// and reports false for any other CREATE, including the name-less
// CREATE INDEX ON table (...) form where PostgreSQL derives the name itself.
func parseCreateIndexName(tokens []lexer.Token) (postgresIndexRef, bool) {
	rest, ok := consumeCreateIndexPrefix(tokens)
	if !ok {
		return postgresIndexRef{}, false
	}
	return parseIndexNameTokens(rest)
}

// consumeCreateIndexPrefix consumes the keywords that stand between CREATE and
// an index name -- [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] -- and returns
// the tokens that follow them. It reports false for a CREATE that is not a
// CREATE INDEX, and for a CREATE INDEX that runs out of tokens before naming
// anything.
func consumeCreateIndexPrefix(tokens []lexer.Token) ([]lexer.Token, bool) {
	tokens = skipKeywordToken(tokens, "UNIQUE")
	if len(tokens) == 0 || !tokens[0].MatchIdentifierValue("INDEX") {
		return nil, false
	}
	tokens = skipKeywordToken(tokens[1:], "CONCURRENTLY")
	if len(tokens) > 0 && tokens[0].MatchIdentifierValue("IF") {
		if len(tokens) < 3 || !tokens[1].MatchIdentifierValue("NOT") || !tokens[2].MatchIdentifierValue("EXISTS") {
			return nil, false
		}
		tokens = tokens[3:]
	}
	return tokens, len(tokens) > 0
}

// skipKeywordToken drops a leading optional keyword when it is present.
func skipKeywordToken(tokens []lexer.Token, keyword string) []lexer.Token {
	if len(tokens) > 0 && tokens[0].MatchIdentifierValue(keyword) {
		return tokens[1:]
	}
	return tokens
}

// parseIndexNameTokens reads one optionally schema-qualified index name from
// the head of a non-empty token slice.
func parseIndexNameTokens(tokens []lexer.Token) (postgresIndexRef, bool) {
	// CREATE INDEX ON t (c) names nothing; ON here is the clause, not a name.
	if tokens[0].MatchIdentifierValue("ON") {
		return postgresIndexRef{}, false
	}
	first, ok := postgresIdentifierValue(tokens[0])
	if !ok {
		return postgresIndexRef{}, false
	}
	if len(tokens) < 3 || !tokens[1].MatchOperatorValue(".") {
		return postgresIndexRef{Name: first}, true
	}
	second, ok := postgresIdentifierValue(tokens[2])
	if !ok {
		return postgresIndexRef{}, false
	}
	return postgresIndexRef{Schema: first, Name: second}, true
}

// postgresIdentifierValue returns the catalog spelling of one identifier token.
// A double-quoted identifier keeps its bytes with "" collapsed to a single
// quote; an unquoted one folds to lower case, matching how PostgreSQL stores
// it. The lexer emits a double-quoted identifier as a string token, so both
// token types are accepted here.
func postgresIdentifierValue(token lexer.Token) (string, bool) {
	switch token.Type {
	case lexer.TokenIdentifier:
		return strings.ToLower(token.Value), true
	case lexer.TokenString:
		value := token.Value
		if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
			return "", false
		}
		return strings.ReplaceAll(value[1:len(value)-1], `""`, `"`), true
	default:
		return "", false
	}
}

// significantSQLTokens tokenizes sqlText for dialect and drops whitespace and
// comments, leaving the token stream a grammar scan can walk by position.
func significantSQLTokens(sqlText, dialect string) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(sqlText, checkLexerOptions(dialect))
	var tokens []lexer.Token
	for {
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
}
