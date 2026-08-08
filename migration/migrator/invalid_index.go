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
// The probe is PostgreSQL-only by nature -- see
// [Migrator.unusableIndexesCreatedBy]. This deliberately ignores opts.Force.
// --force is documented as "Rewrite or create the revision row even when it is
// not dirty" -- it relaxes a precondition about the metadata, not a fact about
// the database. The escape hatch is REINDEX INDEX CONCURRENTLY, which fixes the
// database rather than the bookkeeping about it. No flag on any surface relaxes
// either refusal, for the same reason: a flag can only change what Ptah records
// about the database, and the problem is the database.
//
// [Migrator.refuseUpOverUnusableIndex] is the same refusal on the up path.
func (m *Migrator) refuseRepairOverUnusableIndex(ctx context.Context, migration *Migration) error {
	return m.refuseRepairOverUnusableIndexSQL(ctx, migration, migration.UpSQL, unusableIndexRepairError)
}

func (m *Migrator) refuseRollbackCompletionOverUnusableIndex(ctx context.Context, migration *Migration) error {
	return m.refuseRepairOverUnusableIndexSQL(ctx, migration, migration.DownSQL, unusableIndexRollbackError)
}

func (m *Migrator) refuseRepairOverUnusableIndexSQL(
	ctx context.Context,
	migration *Migration,
	sqlText string,
	buildError func(int64, []postgresUnusableIndex) error,
) error {
	unusable, err := m.unusableIndexesCreatedBySQL(ctx, sqlText)
	if err != nil {
		return err
	}
	if len(unusable) == 0 {
		return nil
	}
	return buildError(migration.Version, unusable)
}

// refuseUpOverUnusableIndex refuses to run a migration while PostgreSQL already
// reports an index that migration creates as unusable.
//
// This is the automatic half of the defect refuseRepairOverUnusableIndex covers.
// An operator does not have to reach for repair at all: `migrations up
// --allow-dirty` (and `ptah-compat migrate apply --allow-dirty`, which reaches
// the same code) re-runs the body over the dirty row a failed concurrent build
// left, and meets the leftover on exactly the same terms -- the generated
// statement is CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS, the invalid index
// still occupies the name, PostgreSQL skips it with a notice, nothing errors,
// and the run clears the dirty state over an index that enforces nothing.
//
// It is not restricted to that retry. Measured on PostgreSQL 17.10 with the
// refusal scoped to a dirty retry: an invalid index left behind by any other
// route -- a hand-run build, a restored dump, a migration whose revision row was
// cleaned up out of band -- is skipped by the same IF NOT EXISTS on a FIRST
// attempt, the migration is recorded applied, and a duplicate write is accepted.
// The question worth asking is whether the object the migration creates is
// usable, and a revision row cannot answer it.
//
// Two limits are deliberate. A dry run is exempt, because it records nothing to
// be wrong about and no surface's dry-run exit code was measured for this. And
// an index deliberately left invalid -- CREATE INDEX ON ONLY a partitioned
// parent, which stays invalid until every partition's index is attached -- is
// refused if a later attempt at the same migration meets it. Telling that shape
// apart from residue is stokaro/ptah#997's partitioned-parent awareness, which
// is measured there rather than guessed at here.
//
// Nothing is written when it refuses. The probe runs before any revision
// bookkeeping, so a dirty row still holds the failed attempt's own error and its
// applied/total counters: `migrations status` reports the same state it did
// before, and a later retry resumes from the same statement. Recording a second
// failure over it would reset those counters from something that is not a
// statement failure, and the next retry would re-run SQL that already committed.
func (m *Migrator) refuseUpOverUnusableIndex(ctx context.Context, migration *Migration) error {
	if m.conn == nil || m.conn.Writer().IsDryRun() {
		return nil
	}
	unusable, err := m.unusableIndexesCreatedBy(ctx, migration)
	if err != nil || len(unusable) == 0 {
		return err
	}
	return unusableIndexApplyError(migration.Version, unusable)
}

// unusableIndexesCreatedBy returns the indexes migration's up SQL creates that
// PostgreSQL currently reports as unusable.
//
// It is empty on every other dialect before any query runs: no other supported
// dialect has a concurrent index build that can be left half-finished, so they
// keep their existing code paths rather than growing a no-op probe.
func (m *Migrator) unusableIndexesCreatedBy(ctx context.Context, migration *Migration) ([]postgresUnusableIndex, error) {
	return m.unusableIndexesCreatedBySQL(ctx, migration.UpSQL)
}

func (m *Migrator) unusableIndexesCreatedBySQL(ctx context.Context, sqlText string) ([]postgresUnusableIndex, error) {
	if m.conn == nil || platform.NormalizeDialect(m.conn.Info().Dialect) != platform.Postgres {
		return nil, nil
	}
	refs := postgresCreatedIndexNames(sqlText)
	if len(refs) == 0 {
		return nil, nil
	}
	return m.postgresUnusableIndexes(ctx, refs)
}

// unusableIndexRepairError renders the repair refusal. It names every index that
// is unusable together with the catalog flags that say so, and points at the
// PostgreSQL command that rebuilds one without holding writes.
func unusableIndexRepairError(version int64, unusable []postgresUnusableIndex) error {
	return unusableIndexStateError(
		version,
		unusable,
		"recording the migration applied would report a constraint that is not enforced",
		"rerun the migration",
	)
}

func unusableIndexRollbackError(version int64, unusable []postgresUnusableIndex) error {
	return unusableIndexStateError(
		version,
		unusable,
		"completing the rollback would hide an unusable index behind a deleted revision",
		"resume the rollback",
	)
}

func unusableIndexStateError(
	version int64,
	unusable []postgresUnusableIndex,
	unsafeOutcome string,
	retryAction string,
) error {
	details, rebuild, noun := unusableIndexPhrases(unusable)
	return fmt.Errorf(
		"migration %d cannot be repaired: PostgreSQL reports %s %s unusable, "+
			"so %s; run %s, or drop the %s and %s, then repair again",
		version,
		noun,
		details,
		unsafeOutcome,
		rebuild,
		noun,
		retryAction,
	)
}

// unusableIndexApplyError renders the up-path refusal. It says why running the
// body is not itself the fix, which is what separates this refusal from an
// ordinary migration failure the operator could simply run again -- and running
// it again is exactly what an operator holding --allow-dirty has already chosen
// to do.
func unusableIndexApplyError(version int64, unusable []postgresUnusableIndex) error {
	details, rebuild, noun := unusableIndexPhrases(unusable)
	return fmt.Errorf(
		"migration %d cannot be applied: PostgreSQL reports %s %s unusable, "+
			"and CREATE INDEX ... IF NOT EXISTS finds the name taken and skips it rather than rebuilding it, "+
			"so this run would record the migration applied over a constraint that is not enforced; "+
			"run %s, or drop the %s, then run the migration again",
		version,
		noun,
		details,
		rebuild,
		noun,
	)
}

// unusableIndexPhrases renders the three parts both refusals share: the named
// indexes with the catalog flags that condemn them, the REINDEX commands that
// rebuild them, and the noun that agrees with how many there are.
func unusableIndexPhrases(unusable []postgresUnusableIndex) (details, rebuild, noun string) {
	detailParts := make([]string, 0, len(unusable))
	rebuildParts := make([]string, 0, len(unusable))
	for _, index := range unusable {
		detailParts = append(detailParts, fmt.Sprintf(
			"%s (indisvalid=%t, indisready=%t)",
			index.quotedName(), index.Valid, index.Ready,
		))
		rebuildParts = append(rebuildParts, "REINDEX INDEX CONCURRENTLY "+index.quotedName())
	}
	noun = "index"
	if len(unusable) > 1 {
		noun = "indexes"
	}
	return strings.Join(detailParts, ", "), strings.Join(rebuildParts, "; "), noun
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
