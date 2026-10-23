package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/pgsystemschema"
	"ptah.run/internal/sqlident"
)

// indexGuard is PostgreSQL's unusable-index guard. A concurrent unique index
// build that fails partway leaves an invalid index behind; because it occupies
// the name, re-issuing CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS is
// skipped with a notice instead of retried, so nothing errors and a revision
// row would be written as applied over a constraint the database is not
// enforcing. The guard reads what a migration's conditional creates name, asks
// the catalog what holds each name, and words the refusals.
type indexGuard struct{}

func (indexGuard) ConditionalCreates(sqlText string) []migratorext.IndexRef {
	return postgresConditionalCreatedIndexNames(sqlText)
}

func (indexGuard) Drops(sqlText string) []migratorext.IndexRef {
	return postgresDroppedIndexNames(sqlText)
}

func (indexGuard) Attachments(statement string) []migratorext.IndexAttachment {
	return postgresIndexConstraintAttachments(statement)
}

func (indexGuard) SearchPath(statement string) (changed, known bool) {
	return searchPathReplayState(statement)
}

func (indexGuard) ResolveTarget(
	ctx context.Context,
	q migratorext.RowsQueryer,
	ref migratorext.IndexRef,
) (migratorext.IndexState, error) {
	var target migratorext.IndexState
	err := resolveTarget(ctx, q, ref, &target)
	return target, err
}

func (indexGuard) State(ctx context.Context, q migratorext.RowsQueryer, ref migratorext.IndexRef) (migratorext.IndexState, error) {
	return indexState(ctx, q, ref)
}

func (indexGuard) TargetsAcrossSchemas(
	ctx context.Context,
	q migratorext.RowsQueryer,
	ref migratorext.IndexRef,
) ([]migratorext.IndexState, error) {
	return targetsAcrossSchemas(ctx, q, ref)
}

func (indexGuard) ResolveDropped(
	ctx context.Context,
	q migratorext.RowsQueryer,
	ref migratorext.IndexRef,
) (migratorext.IndexName, bool, error) {
	return resolveDropped(ctx, q, ref)
}

func (indexGuard) Usable(state migratorext.IndexState) bool {
	return isUsableForTarget(state)
}

func (indexGuard) ApplyError(version int64, unsafe []migratorext.IndexState) error {
	return postgresIndexApplyError(version, unsafe)
}

func (indexGuard) RepairError(version int64, unsafe []migratorext.IndexState, refusal migratorext.IndexRefusal) error {
	unusable := postgresUnusableIndexes(unsafe)
	if len(unusable) == len(unsafe) {
		return unusableIndexStateError(version, unusable, refusal.UnusableOutcome, refusal.RetryAction)
	}
	return postgresIndexRepairError(version, unsafe, refusal.UnsafeOutcome)
}

func quotedName(s migratorext.IndexState) string {
	return sqlident.Qualified(platform.Postgres, s.TargetSchema, s.Name)
}

func quotedTarget(s migratorext.IndexState) string {
	return sqlident.Qualified(platform.Postgres, s.TargetSchema, s.TargetTable)
}

func isIndex(s migratorext.IndexState) bool {
	return s.RelationKind == "i" || s.RelationKind == "I"
}

func isAttachedToTarget(s migratorext.IndexState) bool {
	return s.IndexedTableSchema == s.TargetSchema && s.IndexedTable == s.TargetTable
}

func isExpectedPartitionedIndex(s migratorext.IndexState) bool {
	return s.TargetKind == "p" && s.RelationKind == "I" && s.Ready
}

func isUsableForTarget(s migratorext.IndexState) bool {
	return s.TargetFound && s.RelationExists && isIndex(s) && isAttachedToTarget(s) &&
		((s.Valid && s.Ready) || isExpectedPartitionedIndex(s))
}

func isUnusableTargetIndex(s migratorext.IndexState) bool {
	return s.TargetFound && s.RelationExists && isIndex(s) && isAttachedToTarget(s) &&
		!isUsableForTarget(s)
}

func (i unusableIndex) quotedName() string {
	return sqlident.Qualified(platform.Postgres, i.Schema, i.Name)
}

// significantTokens tokenizes sqlText as PostgreSQL reads it and drops
// whitespace and comments, leaving the token stream a grammar scan can walk by
// position.
func significantTokens(sqlText string) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(sqlText, dialectlexer.Options(platform.Postgres))
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

func matchesAnyKeyword(token lexer.Token, keywords ...string) bool {
	return slices.ContainsFunc(keywords, token.MatchIdentifierValue)
}

type createIndexPrefix struct {
	tokens      []lexer.Token
	ifNotExists bool
}

// postgresDroppedIndexNames returns the explicitly named indexes removed by
// DROP INDEX statements. PostgreSQL permits several comma-separated names;
// each is retained so a cleanup suppresses only its matching create.
func postgresDroppedIndexNames(sqlText string) []migratorext.IndexRef {
	tokens := significantTokens(sqlText)
	var refs []migratorext.IndexRef
	for i, token := range tokens {
		if i > 0 && tokens[i-1].Type != lexer.TokenSemicolon {
			continue
		}
		if !token.MatchIdentifierValue("DROP") {
			continue
		}
		refs = append(refs, parseDropIndexNames(tokens[i+1:])...)
	}
	return refs
}

func parseDropIndexNames(tokens []lexer.Token) []migratorext.IndexRef {
	if len(tokens) == 0 || !tokens[0].MatchIdentifierValue("INDEX") {
		return nil
	}
	tokens = skipKeywordToken(tokens[1:], "CONCURRENTLY")
	if len(tokens) > 0 && tokens[0].MatchIdentifierValue("IF") {
		if len(tokens) < 2 || !tokens[1].MatchIdentifierValue("EXISTS") {
			return nil
		}
		tokens = tokens[2:]
	}

	var refs []migratorext.IndexRef
	for len(tokens) > 0 {
		ref, consumed, ok := parseQualifiedIndexName(tokens)
		if !ok {
			return refs
		}
		refs = append(refs, ref)
		tokens = tokens[consumed:]
		if len(tokens) == 0 || !tokens[0].MatchOperatorValue(",") {
			return refs
		}
		tokens = tokens[1:]
	}
	return refs
}

func parseQualifiedIndexName(tokens []lexer.Token) (migratorext.IndexRef, int, bool) {
	name, ok := postgresIdentifierValue(tokens[0])
	if !ok {
		return migratorext.IndexRef{}, 0, false
	}
	if len(tokens) < 3 || !tokens[1].MatchOperatorValue(".") {
		return migratorext.IndexRef{Name: name}, 1, true
	}
	qualifiedName, ok := postgresIdentifierValue(tokens[2])
	if !ok {
		return migratorext.IndexRef{}, 0, false
	}
	return migratorext.IndexRef{Schema: name, Name: qualifiedName}, 3, true
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
func postgresCreatedIndexNames(sqlText string) []migratorext.IndexRef {
	tokens := significantTokens(sqlText)
	var (
		refs []migratorext.IndexRef
		seen = make(map[migratorext.IndexRef]struct{})
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

func postgresConditionalCreatedIndexNames(sqlText string) []migratorext.IndexRef {
	refs := postgresCreatedIndexNames(sqlText)
	conditional := make([]migratorext.IndexRef, 0, len(refs))
	for _, ref := range refs {
		if ref.IfNotExists {
			conditional = append(conditional, ref)
		}
	}
	return conditional
}

// parseCreateIndexName reads the index name and target-table schema out of the
// tail of a CREATE statement, given the tokens that follow the CREATE keyword.
// It follows the
// PostgreSQL grammar
//
//	CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] name ON [ONLY] table ...
//
// and reports false for any other CREATE, including the name-less
// CREATE INDEX ON table (...) form where PostgreSQL derives the name itself.
func parseCreateIndexName(tokens []lexer.Token) (migratorext.IndexRef, bool) {
	prefix, ok := consumeCreateIndexPrefix(tokens)
	if !ok {
		return migratorext.IndexRef{}, false
	}
	ref, ok := parseIndexNameTokens(prefix.tokens)
	ref.IfNotExists = prefix.ifNotExists
	return ref, ok
}

// consumeCreateIndexPrefix consumes the keywords that stand between CREATE and
// an index name -- [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] -- and returns
// the tokens that follow them. It reports false for a CREATE that is not a
// CREATE INDEX, and for a CREATE INDEX that runs out of tokens before naming
// anything.
func consumeCreateIndexPrefix(tokens []lexer.Token) (createIndexPrefix, bool) {
	tokens = skipKeywordToken(tokens, "UNIQUE")
	if len(tokens) == 0 || !tokens[0].MatchIdentifierValue("INDEX") {
		return createIndexPrefix{}, false
	}
	tokens = skipKeywordToken(tokens[1:], "CONCURRENTLY")
	ifNotExists := false
	if len(tokens) > 0 && tokens[0].MatchIdentifierValue("IF") {
		if len(tokens) < 3 || !tokens[1].MatchIdentifierValue("NOT") || !tokens[2].MatchIdentifierValue("EXISTS") {
			return createIndexPrefix{}, false
		}
		tokens = tokens[3:]
		ifNotExists = true
	}
	return createIndexPrefix{tokens: tokens, ifNotExists: ifNotExists}, len(tokens) > 0
}

// skipKeywordToken drops a leading optional keyword when it is present.
func skipKeywordToken(tokens []lexer.Token, keyword string) []lexer.Token {
	if len(tokens) > 0 && tokens[0].MatchIdentifierValue(keyword) {
		return tokens[1:]
	}
	return tokens
}

// parseIndexNameTokens reads the index name and the schema of its target table.
// PostgreSQL does not allow the index name itself to carry a schema: the index
// is created in the table's schema. Binding the catalog probe to the table is
// therefore what keeps a schema-qualified target visible even when that schema
// is not on search_path.
func parseIndexNameTokens(tokens []lexer.Token) (migratorext.IndexRef, bool) {
	// CREATE INDEX ON t (c) names nothing; ON here is the clause, not a name.
	if tokens[0].MatchIdentifierValue("ON") {
		return migratorext.IndexRef{}, false
	}
	name, ok := postgresIdentifierValue(tokens[0])
	if !ok {
		return migratorext.IndexRef{}, false
	}
	tokens = tokens[1:]
	if len(tokens) == 0 || !tokens[0].MatchIdentifierValue("ON") {
		return migratorext.IndexRef{}, false
	}
	tokens = skipKeywordToken(tokens[1:], "ONLY")
	if len(tokens) == 0 {
		return migratorext.IndexRef{}, false
	}
	firstTablePart, ok := postgresIdentifierValue(tokens[0])
	if !ok {
		return migratorext.IndexRef{}, false
	}
	if len(tokens) < 3 || !tokens[1].MatchOperatorValue(".") {
		return migratorext.IndexRef{Table: firstTablePart, Name: name}, true
	}
	table, ok := postgresIdentifierValue(tokens[2])
	if !ok {
		return migratorext.IndexRef{}, false
	}
	return migratorext.IndexRef{Schema: firstTablePart, Table: table, Name: name}, true
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

func postgresIndexConstraintAttachments(statement string) []migratorext.IndexAttachment {
	tokens := significantTokens(statement)
	if len(tokens) < 3 || !tokens[0].MatchIdentifierValue("ALTER") || !tokens[1].MatchIdentifierValue("TABLE") {
		return nil
	}
	tokens = tokens[2:]
	if len(tokens) >= 2 && tokens[0].MatchIdentifierValue("IF") && tokens[1].MatchIdentifierValue("EXISTS") {
		tokens = tokens[2:]
	}
	tokens = skipKeywordToken(tokens, "ONLY")
	target, tail, ok := postgresAttachmentTarget(tokens)
	if !ok {
		return nil
	}
	var attachments []migratorext.IndexAttachment
	depth := 0
	actionStart := true
	for i, token := range tail {
		if token.MatchOperatorValue(";") {
			break
		}
		if actionStart {
			if attachment, ok := parsePostgresIndexConstraintAttachment(target, tail[i:]); ok {
				attachments = append(attachments, attachment)
			}
			actionStart = false
		}
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case token.MatchOperatorValue(",") && depth == 0:
			actionStart = true
		}
	}
	return attachments
}

func postgresAttachmentTarget(tokens []lexer.Token) (migratorext.IndexRef, []lexer.Token, bool) {
	if len(tokens) == 0 {
		return migratorext.IndexRef{}, nil, false
	}
	name, ok := postgresIdentifierValue(tokens[0])
	if !ok {
		return migratorext.IndexRef{}, nil, false
	}
	target := migratorext.IndexRef{Table: name}
	tokens = tokens[1:]
	if len(tokens) >= 2 && tokens[0].MatchOperatorValue(".") {
		table, ok := postgresIdentifierValue(tokens[1])
		if !ok {
			return migratorext.IndexRef{}, nil, false
		}
		target.Schema, target.Table = name, table
		tokens = tokens[2:]
	}
	if len(tokens) > 0 && tokens[0].MatchOperatorValue("*") {
		tokens = tokens[1:]
	}
	return target, tokens, true
}

func parsePostgresIndexConstraintAttachment(
	target migratorext.IndexRef,
	tokens []lexer.Token,
) (migratorext.IndexAttachment, bool) {
	if len(tokens) < 7 || !tokens[0].MatchIdentifierValue("ADD") || !tokens[1].MatchIdentifierValue("CONSTRAINT") {
		return migratorext.IndexAttachment{}, false
	}
	name, ok := postgresIdentifierValue(tokens[2])
	if !ok {
		return migratorext.IndexAttachment{}, false
	}
	tokens = tokens[3:]
	switch {
	case tokens[0].MatchIdentifierValue("UNIQUE"):
		tokens = tokens[1:]
	case tokens[0].MatchIdentifierValue("PRIMARY") && tokens[1].MatchIdentifierValue("KEY"):
		tokens = tokens[2:]
	default:
		return migratorext.IndexAttachment{}, false
	}
	if len(tokens) < 3 || !tokens[0].MatchIdentifierValue("USING") || !tokens[1].MatchIdentifierValue("INDEX") {
		return migratorext.IndexAttachment{}, false
	}
	index, ok := postgresIdentifierValue(tokens[2])
	if !ok {
		return migratorext.IndexAttachment{}, false
	}
	target.Name = index
	return migratorext.IndexAttachment{Index: target, Name: name}, true
}

// searchPathReplayState reports whether a replayed statement changes
// search_path, and whether the value it leaves is known. Only the PostgreSQL
// index observation reads the answer.
func searchPathReplayState(statement string) (changed, known bool) {
	tokens := significantTokens(statement)
	if len(tokens) < 2 {
		return false, false
	}
	if tokens[0].MatchIdentifierValue("RESET") {
		if tokens[1].MatchIdentifierValue("ALL") || tokens[1].MatchIdentifierValue("SEARCH_PATH") {
			return true, false
		}
		return false, false
	}
	if !tokens[0].MatchIdentifierValue("SET") {
		return false, false
	}
	setting := 1
	local := tokens[setting].MatchIdentifierValue("LOCAL")
	if local || tokens[setting].MatchIdentifierValue("SESSION") {
		setting++
	}
	if setting >= len(tokens) || !tokens[setting].MatchIdentifierValue("SEARCH_PATH") {
		return false, false
	}
	if local {
		return true, false
	}
	for _, token := range tokens[setting+1:] {
		if matchesAnyKeyword(token, "DEFAULT", "CURRENT") || strings.Contains(strings.ToLower(token.Value), "$user") {
			return true, false
		}
	}
	return true, true
}

func unusableIndexStateError(
	version int64,
	unusable []unusableIndex,
	unsafeOutcome,
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
func unusableIndexApplyError(version int64, unusable []unusableIndex) error {
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

func postgresIndexApplyError(version int64, unsafe []migratorext.IndexState) error {
	unusable := make([]unusableIndex, 0, len(unsafe))
	for _, state := range unsafe {
		if isUnusableTargetIndex(state) {
			unusable = append(unusable, unusableIndex{
				Schema: state.TargetSchema,
				Name:   state.Name,
				Valid:  state.Valid,
				Ready:  state.Ready,
			})
		}
	}
	if len(unusable) == len(unsafe) {
		return unusableIndexApplyError(version, unusable)
	}

	details := make([]string, 0, len(unsafe))
	for _, state := range unsafe {
		details = append(details, postgresIndexStateProblem(state))
	}
	return fmt.Errorf(
		"migration %d cannot be applied: PostgreSQL cannot provide the intended index state: %s; "+
			"CREATE INDEX ... IF NOT EXISTS can skip when any relation owns the name, so resolve the conflicting or missing relation and run the migration again",
		version,
		strings.Join(details, "; "),
	)
}

func postgresIndexRepairError(version int64, unsafe []migratorext.IndexState, unsafeOutcome string) error {
	details := make([]string, 0, len(unsafe))
	for _, state := range unsafe {
		details = append(details, postgresIndexStateProblem(state))
	}
	return fmt.Errorf(
		"migration %d cannot be repaired: PostgreSQL cannot provide the intended index state: %s; "+
			"%s; resolve the conflicting or missing relation, then repair again",
		version,
		strings.Join(details, "; "),
		unsafeOutcome,
	)
}

func postgresIndexStateProblem(state migratorext.IndexState) string {
	if !state.TargetFound {
		return fmt.Sprintf("target table %s cannot be resolved", quotedTarget(state))
	}
	if !state.RelationExists {
		return fmt.Sprintf("index %s is missing from target table %s", quotedName(state), quotedTarget(state))
	}
	if !isIndex(state) {
		return fmt.Sprintf(
			"relation %s has relkind=%q instead of being an index on target table %s",
			quotedName(state),
			state.RelationKind,
			quotedTarget(state),
		)
	}
	if isUsableForTarget(state) {
		return fmt.Sprintf(
			"index %s on target table %s has a different definition than the migration declares",
			quotedName(state),
			quotedTarget(state),
		)
	}
	if !isAttachedToTarget(state) {
		actual := sqlident.Qualified(platform.Postgres, state.IndexedTableSchema, state.IndexedTable)
		return fmt.Sprintf(
			"relation %s is an index on %s instead of target table %s",
			quotedName(state),
			actual,
			quotedTarget(state),
		)
	}
	return fmt.Sprintf(
		"index %s on target table %s has indisvalid=%t and indisready=%t",
		quotedName(state),
		quotedTarget(state),
		state.Valid,
		state.Ready,
	)
}

// unusableIndexPhrases renders the three parts both refusals share: the named
// indexes with the catalog flags that condemn them, the REINDEX commands that
// rebuild them, and the noun that agrees with how many there are.
func unusableIndexPhrases(unusable []unusableIndex) (details, rebuild, noun string) {
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

func postgresUnusableIndexes(states []migratorext.IndexState) []unusableIndex {
	unusable := make([]unusableIndex, 0, len(states))
	for _, state := range states {
		if isUnusableTargetIndex(state) {
			unusable = append(unusable, unusableIndex{
				Schema: state.TargetSchema,
				Name:   state.Name,
				Valid:  state.Valid,
				Ready:  state.Ready,
			})
		}
	}
	return unusable
}

func targetsAcrossSchemas(
	ctx context.Context,
	q migratorext.RowsQueryer,
	ref migratorext.IndexRef,
) ([]migratorext.IndexState, error) {
	query := sqlutil.Rebind(platform.Postgres, `
		SELECT n.nspname, t.relname, t.relkind::text
		FROM pg_class t
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE t.relkind IN ('r', 'p', 'm')
		  AND t.relname = ?
		  AND `+pgsystemschema.PostgresNonSystemSchemasPredicate(platform.Postgres)+`
		ORDER BY n.nspname`)
	rows, err := q.QueryContext(ctx, query, ref.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve unqualified index targets across PostgreSQL schemas: %w", err)
	}
	defer rows.Close()
	var identities []migratorext.IndexState
	for rows.Next() {
		identity := migratorext.IndexState{TargetFound: true, Name: ref.Name}
		if err := rows.Scan(&identity.TargetSchema, &identity.TargetTable, &identity.TargetKind); err != nil {
			return nil, fmt.Errorf("failed to scan unqualified PostgreSQL index target: %w", err)
		}
		identities = append(identities, identity)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read unqualified PostgreSQL index targets: %w", err)
	}
	return identities, nil
}

func indexState(
	ctx context.Context,
	q migratorext.RowsQueryer,
	ref migratorext.IndexRef,
) (migratorext.IndexState, error) {
	state := migratorext.IndexState{
		TargetSchema: ref.Schema,
		TargetTable:  ref.Table,
		Name:         ref.Name,
	}
	if err := resolveTarget(ctx, q, ref, &state); err != nil {
		return migratorext.IndexState{}, err
	}
	if state.TargetSchema == "" {
		return state, nil
	}
	if err := resolveNamedRelation(ctx, q, &state); err != nil {
		return migratorext.IndexState{}, err
	}
	return state, nil
}

func resolveTarget(
	ctx context.Context,
	q migratorext.RowsQueryer,
	ref migratorext.IndexRef,
	state *migratorext.IndexState,
) error {
	predicate := "t.relname = ? AND pg_catalog.pg_table_is_visible(t.oid)"
	args := []any{ref.Table}
	if ref.Schema != "" {
		predicate = "t.relname = ? AND n.nspname = ?"
		args = append(args, ref.Schema)
	}
	query := sqlutil.Rebind(platform.Postgres, fmt.Sprintf(`
		SELECT n.nspname, t.relname, t.relkind::text
		FROM pg_class t
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE t.relkind IN ('r', 'p', 'm') AND %s
		LIMIT 1`, predicate))
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to resolve index target table: %w", err)
	}
	if !rows.Next() {
		readErr := rows.Err()
		_ = rows.Close()
		if readErr != nil {
			return fmt.Errorf("failed to read index target table: %w", readErr)
		}
		return nil
	}
	if err := rows.Scan(&state.TargetSchema, &state.TargetTable, &state.TargetKind); err != nil {
		_ = rows.Close()
		return fmt.Errorf("failed to scan index target table: %w", err)
	}
	state.TargetFound = true
	if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close index target query: %w", err)
	}
	return nil
}

func resolveNamedRelation(
	ctx context.Context,
	q migratorext.RowsQueryer,
	state *migratorext.IndexState,
) error {
	query := sqlutil.Rebind(platform.Postgres, `
		SELECT c.relkind::text,
		       COALESCE(tn.nspname, ''),
		       COALESCE(t.relname, ''),
		       COALESCE(ix.indisvalid, false),
		       COALESCE(ix.indisready, false)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_index ix ON ix.indexrelid = c.oid
		LEFT JOIN pg_class t ON t.oid = ix.indrelid
		LEFT JOIN pg_namespace tn ON tn.oid = t.relnamespace
		WHERE n.nspname = ? AND c.relname = ?
		LIMIT 1`)
	rows, err := q.QueryContext(ctx, query, state.TargetSchema, state.Name)
	if err != nil {
		return fmt.Errorf("failed to inspect index relation: %w", err)
	}
	if !rows.Next() {
		readErr := rows.Err()
		_ = rows.Close()
		if readErr != nil {
			return fmt.Errorf("failed to read index relation: %w", readErr)
		}
		return nil
	}
	if err := rows.Scan(
		&state.RelationKind,
		&state.IndexedTableSchema,
		&state.IndexedTable,
		&state.Valid,
		&state.Ready,
	); err != nil {
		_ = rows.Close()
		return fmt.Errorf("failed to scan index relation: %w", err)
	}
	state.RelationExists = true
	if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close index relation query: %w", err)
	}
	return nil
}

func resolveDropped(
	ctx context.Context,
	q migratorext.RowsQueryer,
	ref migratorext.IndexRef,
) (migratorext.IndexName, bool, error) {
	if ref.Schema != "" {
		return ref.IndexName(), true, nil
	}
	query := sqlutil.Rebind(platform.Postgres, `
		SELECT n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = ? AND pg_catalog.pg_table_is_visible(c.oid)
		LIMIT 1`)
	rows, err := q.QueryContext(ctx, query, ref.Name)
	if err != nil {
		return migratorext.IndexName{}, false, fmt.Errorf("failed to resolve dropped index: %w", err)
	}
	if !rows.Next() {
		readErr := rows.Err()
		_ = rows.Close()
		if readErr != nil {
			return migratorext.IndexName{}, false, fmt.Errorf("failed to read dropped index: %w", readErr)
		}
		return migratorext.IndexName{}, false, nil
	}
	var name migratorext.IndexName
	if err := rows.Scan(&name.Schema, &name.Name); err != nil {
		_ = rows.Close()
		return migratorext.IndexName{}, false, fmt.Errorf("failed to scan dropped index: %w", err)
	}
	if err := rows.Close(); err != nil {
		return migratorext.IndexName{}, false, fmt.Errorf("failed to close dropped index query: %w", err)
	}
	return name, true, nil
}

// unusableIndex is a catalog row for an index PostgreSQL will not use:
// indisvalid or indisready is false. A failed CREATE INDEX CONCURRENTLY leaves
// exactly that state behind, and the leftover keeps the name occupied, so a
// re-run of the same IF NOT EXISTS statement is skipped rather than retried.
type unusableIndex struct {
	Schema string
	Name   string
	Valid  bool
	Ready  bool
}
