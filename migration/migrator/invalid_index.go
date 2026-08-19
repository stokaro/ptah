package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/sqlident"
)

// postgresIndexRef is one index a migration's up SQL creates, spelled the way
// the PostgreSQL catalog stores it. Schema comes from the target table because
// PostgreSQL always creates an index in its table's schema. It is empty when
// the statement left the table unqualified, which means PostgreSQL resolved it
// through the search path.
type postgresIndexRef struct {
	Schema      string
	Table       string
	Name        string
	IfNotExists bool
}

type postgresIndexName struct {
	Schema string
	Name   string
}

type createIndexPrefix struct {
	tokens      []lexer.Token
	ifNotExists bool
}

func (r postgresIndexRef) indexName() postgresIndexName {
	return postgresIndexName{Schema: r.Schema, Name: r.Name}
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

type postgresIndexQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// postgresIndexState is the catalog identity behind one CREATE INDEX target.
// PostgreSQL shares one relation-name namespace between tables, views,
// sequences, and indexes. Checking only pg_index is therefore insufficient:
// IF NOT EXISTS also skips when the name belongs to a different table's index
// or to a non-index relation.
type postgresIndexState struct {
	TargetFound        bool
	TargetSchema       string
	TargetTable        string
	TargetKind         string
	Name               string
	RelationExists     bool
	RelationKind       string
	IndexedTableSchema string
	IndexedTable       string
	Valid              bool
	Ready              bool
}

type postgresIndexObservation struct {
	identities []postgresIndexState
}

func (s postgresIndexState) indexName() postgresIndexName {
	return postgresIndexName{Schema: s.TargetSchema, Name: s.Name}
}

func (s postgresIndexState) quotedName() string {
	return sqlident.Qualified(platform.Postgres, s.TargetSchema, s.Name)
}

func (s postgresIndexState) quotedTarget() string {
	return sqlident.Qualified(platform.Postgres, s.TargetSchema, s.TargetTable)
}

func (s postgresIndexState) isIndex() bool {
	return s.RelationKind == "i" || s.RelationKind == "I"
}

func (s postgresIndexState) isAttachedToTarget() bool {
	return s.IndexedTableSchema == s.TargetSchema && s.IndexedTable == s.TargetTable
}

func (s postgresIndexState) isExpectedPartitionedIndex() bool {
	return s.TargetKind == "p" && s.RelationKind == "I" && s.Ready
}

func (s postgresIndexState) isUsableForTarget() bool {
	return s.TargetFound && s.RelationExists && s.isIndex() && s.isAttachedToTarget() &&
		((s.Valid && s.Ready) || s.isExpectedPartitionedIndex())
}

func (s postgresIndexState) isUnusableTargetIndex() bool {
	return s.TargetFound && s.RelationExists && s.isIndex() && s.isAttachedToTarget() &&
		!s.isUsableForTarget()
}

func (m *Migrator) startPostgresIndexObservation() {
	if platform.NormalizeDialect(m.connectionDialect()) != platform.Postgres || m.postgresIndexObservation != nil {
		return
	}
	m.postgresIndexObservation = &postgresIndexObservation{}
}

func (m *Migrator) withPostgresIndexObservation(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) context.Context {
	m.startPostgresIndexObservation()
	if m.postgresIndexObservation == nil {
		return ctx
	}
	return withInternalStatementObserver(ctx, func(ctx context.Context, event StatementEvent) error {
		return m.observePostgresIndexStatement(ctx, conn, event.Statement)
	})
}

func (m *Migrator) observePostgresIndexStatement(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statement string,
) error {
	if m.postgresIndexObservation == nil {
		return nil
	}
	for _, ref := range postgresConditionalCreatedIndexNames(statement) {
		state, err := m.postgresIndexStateOn(ctx, conn, ref)
		if err != nil {
			return err
		}
		m.appendPostgresIndexIdentity(postgresIndexIdentityState(state, ref))
	}
	return nil
}

func (m *Migrator) observePostgresIndexStatementForReplay(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statement string,
	searchPathKnowledge postgresSearchPathKnowledge,
) error {
	if m.postgresIndexObservation == nil {
		return nil
	}
	for _, ref := range postgresConditionalCreatedIndexNames(statement) {
		if ref.Schema != "" || searchPathKnowledge == postgresSearchPathKnown {
			state, err := m.postgresIndexStateOn(ctx, conn, ref)
			if err != nil {
				return err
			}
			m.appendPostgresIndexIdentity(postgresIndexIdentityState(state, ref))
			continue
		}
		identities, err := m.postgresIndexIdentitiesAcrossUserSchemas(ctx, conn, ref)
		if err != nil {
			return err
		}
		if len(identities) == 0 {
			m.appendPostgresIndexIdentity(postgresIndexIdentityState(postgresIndexState{}, ref))
			continue
		}
		for _, identity := range identities {
			m.appendPostgresIndexIdentity(identity)
		}
	}
	return nil
}

func (m *Migrator) appendPostgresIndexIdentity(identity postgresIndexState) {
	if slices.ContainsFunc(m.postgresIndexObservation.identities, func(existing postgresIndexState) bool {
		return samePostgresIndexIdentity(existing, identity)
	}) {
		return
	}
	m.postgresIndexObservation.identities = append(m.postgresIndexObservation.identities, identity)
}

func (m *Migrator) postgresIndexIdentitiesAcrossUserSchemas(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	ref postgresIndexRef,
) ([]postgresIndexState, error) {
	query := sqlutil.Rebind(conn.Info().Dialect, `
		SELECT n.nspname, t.relname, t.relkind::text
		FROM pg_class t
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE t.relkind IN ('r', 'p', 'm')
		  AND t.relname = ?
		  AND n.nspname !~ '^pg_'
		  AND n.nspname <> 'information_schema'
		ORDER BY n.nspname`)
	rows, err := queryPostgresIndexes(ctx, conn, query, ref.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve unqualified index targets across PostgreSQL schemas: %w", err)
	}
	defer rows.Close()
	var identities []postgresIndexState
	for rows.Next() {
		identity := postgresIndexState{TargetFound: true, Name: ref.Name}
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

func postgresIndexIdentityState(state postgresIndexState, ref postgresIndexRef) postgresIndexState {
	if !state.TargetFound {
		return postgresIndexState{
			TargetSchema: ref.Schema,
			TargetTable:  ref.Table,
			Name:         ref.Name,
		}
	}
	return postgresIndexState{
		TargetFound:  true,
		TargetSchema: state.TargetSchema,
		TargetTable:  state.TargetTable,
		TargetKind:   state.TargetKind,
		Name:         ref.Name,
	}
}

func samePostgresIndexIdentity(left, right postgresIndexState) bool {
	return left.TargetFound == right.TargetFound &&
		left.TargetSchema == right.TargetSchema &&
		left.TargetTable == right.TargetTable &&
		left.Name == right.Name
}

func (i postgresUnusableIndex) quotedName() string {
	return sqlident.Qualified(platform.Postgres, i.Schema, i.Name)
}

// refuseRepairOverUnsafeIndex refuses to record a migration applied while
// PostgreSQL cannot prove that every CREATE INDEX ... IF NOT EXISTS left an
// acceptable index on its intended target table.
//
// A concurrent unique index build that fails partway leaves an invalid index
// behind. Because it occupies the name, re-issuing the generated
// CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS is skipped with a notice
// instead of retried, so nothing errors and the revision row would be written
// as applied over a constraint the database is not enforcing. Refusing leaves
// the operator with a dirty state they can still see.
//
// The probe is PostgreSQL-only by nature -- see
// [Migrator.postgresIndexStatesCreatedBySQLOn]. This deliberately ignores
// opts.Force.
// --force is documented as "Rewrite or create the revision row even when it is
// not dirty" -- it relaxes a precondition about the metadata, not a fact about
// the database. The escape hatch is REINDEX INDEX CONCURRENTLY, which fixes the
// database rather than the bookkeeping about it. No flag on any surface relaxes
// either refusal, for the same reason: a flag can only change what Ptah records
// about the database, and the problem is the database.
//
// [Migrator.refuseUpOverUnsafeIndex] is the same refusal on the up path.
func (m *Migrator) refuseRepairOverUnsafeIndex(ctx context.Context, migration *Migration) error {
	check := func(scoped *Migrator) error {
		return scoped.refuseRepairOverUnsafeIndexSQL(
			ctx,
			scoped.noTransactionConnection(),
			migration,
			migration.UpSQL,
			unusableIndexRepairError,
			"recording the migration applied would report an index state the database does not provide",
		)
	}
	if m.postgresIndexObservation != nil || !m.needsPostgresIndexPostcheck(migration, MigrationDirectionUp) {
		return check(m)
	}
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		statementCount := migrationStatementCountForDialect(migration.UpSQL, scoped.connectionDialect())
		if err := scoped.restoreNoTransactionSessionPrefix(
			ctx,
			migration,
			MigrationDirectionUp,
			statementCount+1,
		); err != nil {
			return err
		}
		return check(scoped)
	})
}

func (m *Migrator) needsPostgresIndexPostcheck(migration *Migration, direction MigrationDirection) bool {
	return platform.NormalizeDialect(m.connectionDialect()) == platform.Postgres &&
		len(postgresConditionalCreatedIndexNames(migrationSQLForDirection(migration, direction))) > 0
}

func (m *Migrator) refuseRollbackCompletionOverUnsafeIndex(ctx context.Context, migration *Migration) error {
	return m.refuseRollbackCompletionOverUnsafeIndexOn(ctx, m.noTransactionConnection(), migration)
}

func (m *Migrator) refuseRollbackCompletionOverUnsafeIndexOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
) error {
	return m.refuseRepairOverUnsafeIndexSQL(
		ctx,
		conn,
		migration,
		migration.DownSQL,
		unusableIndexRollbackError,
		"completing the rollback would hide an index state the database does not provide behind a deleted revision",
	)
}

func (m *Migrator) refuseRepairOverUnsafeIndexSQL(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	sqlText string,
	buildUnusableError func(int64, []postgresUnusableIndex) error,
	unsafeOutcome string,
) error {
	states, err := m.postgresIndexStatesCreatedBySQLOn(ctx, conn, sqlText)
	if err != nil {
		return err
	}
	unsafe := postgresUnsafeCompletionStates(states)
	if len(unsafe) == 0 {
		return nil
	}
	unusable := postgresUnusableIndexes(unsafe)
	if len(unusable) == len(unsafe) {
		return buildUnusableError(migration.Version, unusable)
	}
	return postgresIndexRepairError(migration.Version, unsafe, unsafeOutcome)
}

// refuseUpOverUnsafeIndex refuses to run a migration while PostgreSQL already
// reports a relation that prevents the migration from creating its intended
// usable index.
//
// This is the automatic half of the defect refuseRepairOverUnsafeIndex covers.
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
// A dry run is exempt because it records nothing to be wrong about. A
// partitioned parent index created with CREATE INDEX ... ON ONLY is accepted
// when PostgreSQL reports the expected partitioned-index catalog shape
// (relkind='I', indisready=true); it is intentionally incomplete until every
// partition index is attached and is not failed concurrent-build residue.
//
// Nothing is written when it refuses. The probe runs before any revision
// bookkeeping, so a dirty row still holds the failed attempt's own error and its
// applied/total counters: `migrations status` reports the same state it did
// before, and a later retry resumes from the same statement. Recording a second
// failure over it would reset those counters from something that is not a
// statement failure, and the next retry would re-run SQL that already committed.
func (m *Migrator) refuseUpOverUnsafeIndex(
	ctx context.Context,
	migration *Migration,
	resumeFrom int,
	txMode MigrationTxMode,
) error {
	if m.conn == nil || m.conn.Writer().IsDryRun() {
		return nil
	}
	if platform.NormalizeDialect(m.connectionDialect()) != platform.Postgres {
		return nil
	}
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		executionConn := scoped.noTransactionConnection()
		if txMode == MigrationTxModeNone {
			return scoped.refuseUpOverUnsafeIndexOn(ctx, executionConn, migration, resumeFrom)
		}
		tx, err := executionConn.SchemaWriter().BeginTransaction(ctx)
		if err != nil {
			return fmt.Errorf("failed to open PostgreSQL index recovery preflight: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		return scoped.refuseUpOverUnsafeIndexOn(ctx, executionConn.WithExecutor(tx), migration, resumeFrom)
	})
}

func (m *Migrator) refuseUpOverUnsafeIndexOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	resumeFrom int,
) error {
	unsafe, err := m.postgresIndexStatesBlockingAttempt(ctx, conn, migration.UpSQL, resumeFrom)
	if err != nil || len(unsafe) == 0 {
		return err
	}
	return postgresIndexApplyError(migration.Version, unsafe)
}

// refuseUpCompletionOverUnsafeIndex is the post-execution half of the up guard.
// The preflight permits a migration to remove residue before it recreates the
// index; this probe positively proves that every conditional CREATE INDEX left
// an acceptable index on its intended table before the revision is marked clean.
func (m *Migrator) refuseUpCompletionOverUnsafeIndex(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
) error {
	if conn == nil || conn.Writer().IsDryRun() {
		return nil
	}
	states, err := m.postgresIndexStatesCreatedBySQLOn(ctx, conn, migration.UpSQL)
	if err != nil {
		return err
	}
	unsafe := postgresUnsafeCompletionStates(states)
	if len(unsafe) == 0 {
		return nil
	}
	return postgresIndexApplyError(migration.Version, unsafe)
}

func completedMigrationIndexObservationError(
	migration *Migration,
	dialect string,
	direction MigrationDirection,
	err error,
) error {
	statements := splitSQLStatementsForDialect(migrationSQLForDirection(migration, direction), dialect)
	last := len(statements) - 1
	sourcePath := migration.upSourcePath
	if direction == MigrationDirectionDown {
		sourcePath = migration.downSourcePath
	}
	return &StatementObservationError{
		Err: err,
		Event: StatementEvent{
			Statement:  strings.TrimSpace(statements[last]),
			Index:      len(statements),
			Total:      len(statements),
			SourcePath: sourcePath,
		},
	}
}

func (m *Migrator) postgresIndexStatesCreatedBySQLOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	sqlText string,
) ([]postgresIndexState, error) {
	if conn == nil || platform.NormalizeDialect(conn.Info().Dialect) != platform.Postgres {
		return nil, nil
	}
	if m.postgresIndexObservation != nil {
		if len(postgresConditionalCreatedIndexNames(sqlText)) > 0 && len(m.postgresIndexObservation.identities) == 0 {
			return nil, fmt.Errorf(
				"cannot verify conditional PostgreSQL index creates because their statement-local target identities were not observed",
			)
		}
		return m.refreshPostgresIndexObservations(ctx, conn)
	}
	return m.postgresIndexStatesOn(ctx, conn, postgresConditionalCreatedIndexNames(sqlText))
}

func (m *Migrator) refreshPostgresIndexObservations(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) ([]postgresIndexState, error) {
	states := make([]postgresIndexState, 0, len(m.postgresIndexObservation.identities))
	for _, identity := range m.postgresIndexObservation.identities {
		if !identity.TargetFound {
			states = append(states, identity)
			continue
		}
		state, err := m.postgresIndexStateOn(ctx, conn, postgresIndexRef{
			Schema: identity.TargetSchema,
			Table:  identity.TargetTable,
			Name:   identity.Name,
		})
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
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

func postgresIndexApplyError(version int64, unsafe []postgresIndexState) error {
	unusable := make([]postgresUnusableIndex, 0, len(unsafe))
	for _, state := range unsafe {
		if state.isUnusableTargetIndex() {
			unusable = append(unusable, postgresUnusableIndex{
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

func postgresIndexRepairError(version int64, unsafe []postgresIndexState, unsafeOutcome string) error {
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

func postgresIndexStateProblem(state postgresIndexState) string {
	if !state.TargetFound {
		return fmt.Sprintf("target table %s cannot be resolved", state.quotedTarget())
	}
	if !state.RelationExists {
		return fmt.Sprintf("index %s is missing from target table %s", state.quotedName(), state.quotedTarget())
	}
	if !state.isIndex() {
		return fmt.Sprintf(
			"relation %s has relkind=%q instead of being an index on target table %s",
			state.quotedName(),
			state.RelationKind,
			state.quotedTarget(),
		)
	}
	if !state.isAttachedToTarget() {
		actual := sqlident.Qualified(platform.Postgres, state.IndexedTableSchema, state.IndexedTable)
		return fmt.Sprintf(
			"relation %s is an index on %s instead of target table %s",
			state.quotedName(),
			actual,
			state.quotedTarget(),
		)
	}
	return fmt.Sprintf(
		"index %s on target table %s has indisvalid=%t and indisready=%t",
		state.quotedName(),
		state.quotedTarget(),
		state.Valid,
		state.Ready,
	)
}

func postgresUnsafeCompletionStates(states []postgresIndexState) []postgresIndexState {
	unsafe := make([]postgresIndexState, 0, len(states))
	for _, state := range states {
		if !state.isUsableForTarget() {
			unsafe = append(unsafe, state)
		}
	}
	return unsafe
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

func postgresUnusableIndexes(states []postgresIndexState) []postgresUnusableIndex {
	unusable := make([]postgresUnusableIndex, 0, len(states))
	for _, state := range states {
		if state.isUnusableTargetIndex() {
			unusable = append(unusable, postgresUnusableIndex{
				Schema: state.TargetSchema,
				Name:   state.Name,
				Valid:  state.Valid,
				Ready:  state.Ready,
			})
		}
	}
	return unusable
}

func (m *Migrator) postgresIndexStatesOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	refs []postgresIndexRef,
) ([]postgresIndexState, error) {
	if len(refs) == 0 || platform.NormalizeDialect(conn.Info().Dialect) != platform.Postgres {
		return nil, nil
	}
	states := make([]postgresIndexState, 0, len(refs))
	for _, ref := range refs {
		state, err := m.postgresIndexStateOn(ctx, conn, ref)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
}

func (m *Migrator) postgresIndexStateOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	ref postgresIndexRef,
) (postgresIndexState, error) {
	state := postgresIndexState{
		TargetSchema: ref.Schema,
		TargetTable:  ref.Table,
		Name:         ref.Name,
	}
	if err := m.resolvePostgresIndexTarget(ctx, conn, ref, &state); err != nil {
		return postgresIndexState{}, err
	}
	if state.TargetSchema == "" {
		return state, nil
	}
	if err := m.resolvePostgresNamedRelation(ctx, conn, &state); err != nil {
		return postgresIndexState{}, err
	}
	return state, nil
}

func (m *Migrator) resolvePostgresIndexTarget(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	ref postgresIndexRef,
	state *postgresIndexState,
) error {
	predicate := "t.relname = ? AND pg_catalog.pg_table_is_visible(t.oid)"
	args := []any{ref.Table}
	if ref.Schema != "" {
		predicate = "t.relname = ? AND n.nspname = ?"
		args = append(args, ref.Schema)
	}
	query := sqlutil.Rebind(conn.Info().Dialect, fmt.Sprintf(`
		SELECT n.nspname, t.relname, t.relkind::text
		FROM pg_class t
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE t.relkind IN ('r', 'p', 'm') AND %s
		LIMIT 1`, predicate))
	rows, err := queryPostgresIndexes(ctx, conn, query, args...)
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

func (m *Migrator) resolvePostgresNamedRelation(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	state *postgresIndexState,
) error {
	query := sqlutil.Rebind(conn.Info().Dialect, `
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
	rows, err := queryPostgresIndexes(ctx, conn, query, state.TargetSchema, state.Name)
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

func queryPostgresIndexes(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	query string,
	args ...any,
) (*sql.Rows, error) {
	if queryer, ok := conn.Writer().(postgresIndexQueryer); ok {
		return queryer.QueryContext(ctx, query, args...)
	}
	return conn.QueryContext(ctx, query, args...)
}

// postgresIndexStatesBlockingAttempt returns existing relations that would keep
// an attempted CREATE INDEX from leaving a usable index on its target. A DROP
// suppresses a conflict only after PostgreSQL's catalog and search path resolve
// it to the same schema-level relation name. Statements before resumeFrom were
// committed by an earlier attempt and are deliberately excluded.
func (m *Migrator) postgresIndexStatesBlockingAttempt(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	sqlText string,
	resumeFrom int,
) ([]postgresIndexState, error) {
	if platform.NormalizeDialect(conn.Info().Dialect) != platform.Postgres {
		return nil, nil
	}
	statements := splitSQLStatementsForDialect(sqlText, platform.Postgres)
	dropped := make(map[postgresIndexName]struct{})
	seen := make(map[postgresIndexName]struct{})
	var unsafe []postgresIndexState
	for i, statement := range statements {
		if noTransactionResumeAction(statement, platform.Postgres) == noTransactionPrefixReplay {
			if err := executeSQLOutsideTransaction(ctx, conn, statement); err != nil {
				return nil, fmt.Errorf("failed to replay PostgreSQL session state for index recovery preflight: %w", err)
			}
			continue
		}
		if i+1 < max(resumeFrom, 1) {
			continue
		}
		if err := m.collectPostgresDroppedIndexNames(ctx, conn, statement, dropped); err != nil {
			return nil, err
		}
		states, err := m.postgresUnsafeCreateStates(ctx, conn, statement, dropped, seen)
		if err != nil {
			return nil, err
		}
		unsafe = append(unsafe, states...)
	}
	return unsafe, nil
}

func (m *Migrator) collectPostgresDroppedIndexNames(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statement string,
	dropped map[postgresIndexName]struct{},
) error {
	for _, ref := range postgresDroppedIndexNames(statement) {
		name, found, err := m.resolvePostgresDroppedIndexName(ctx, conn, ref)
		if err != nil {
			return err
		}
		if found {
			dropped[name] = struct{}{}
		}
	}
	return nil
}

func (m *Migrator) postgresUnsafeCreateStates(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statement string,
	dropped,
	seen map[postgresIndexName]struct{},
) ([]postgresIndexState, error) {
	var unsafe []postgresIndexState
	for _, ref := range postgresConditionalCreatedIndexNames(statement) {
		state, err := m.postgresIndexStateOn(ctx, conn, ref)
		if err != nil {
			return nil, err
		}
		if !state.TargetFound && !state.RelationExists {
			continue
		}
		name := state.indexName()
		if _, cleaned := dropped[name]; cleaned {
			continue
		}
		if !state.RelationExists || state.isUsableForTarget() {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		unsafe = append(unsafe, state)
	}
	return unsafe, nil
}

func (m *Migrator) resolvePostgresDroppedIndexName(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	ref postgresIndexRef,
) (postgresIndexName, bool, error) {
	if ref.Schema != "" {
		return ref.indexName(), true, nil
	}
	query := sqlutil.Rebind(conn.Info().Dialect, `
		SELECT n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = ? AND pg_catalog.pg_table_is_visible(c.oid)
		LIMIT 1`)
	rows, err := queryPostgresIndexes(ctx, conn, query, ref.Name)
	if err != nil {
		return postgresIndexName{}, false, fmt.Errorf("failed to resolve dropped index: %w", err)
	}
	if !rows.Next() {
		readErr := rows.Err()
		_ = rows.Close()
		if readErr != nil {
			return postgresIndexName{}, false, fmt.Errorf("failed to read dropped index: %w", readErr)
		}
		return postgresIndexName{}, false, nil
	}
	var name postgresIndexName
	if err := rows.Scan(&name.Schema, &name.Name); err != nil {
		_ = rows.Close()
		return postgresIndexName{}, false, fmt.Errorf("failed to scan dropped index: %w", err)
	}
	if err := rows.Close(); err != nil {
		return postgresIndexName{}, false, fmt.Errorf("failed to close dropped index query: %w", err)
	}
	return name, true, nil
}

// postgresDroppedIndexNames returns the explicitly named indexes removed by
// DROP INDEX statements. PostgreSQL permits several comma-separated names;
// each is retained so a cleanup suppresses only its matching create.
func postgresDroppedIndexNames(sqlText string) []postgresIndexRef {
	tokens := significantSQLTokens(sqlText, platform.Postgres)
	var refs []postgresIndexRef
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

func parseDropIndexNames(tokens []lexer.Token) []postgresIndexRef {
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

	var refs []postgresIndexRef
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

func parseQualifiedIndexName(tokens []lexer.Token) (postgresIndexRef, int, bool) {
	name, ok := postgresIdentifierValue(tokens[0])
	if !ok {
		return postgresIndexRef{}, 0, false
	}
	if len(tokens) < 3 || !tokens[1].MatchOperatorValue(".") {
		return postgresIndexRef{Name: name}, 1, true
	}
	qualifiedName, ok := postgresIdentifierValue(tokens[2])
	if !ok {
		return postgresIndexRef{}, 0, false
	}
	return postgresIndexRef{Schema: name, Name: qualifiedName}, 3, true
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

func postgresConditionalCreatedIndexNames(sqlText string) []postgresIndexRef {
	refs := postgresCreatedIndexNames(sqlText)
	conditional := make([]postgresIndexRef, 0, len(refs))
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
func parseCreateIndexName(tokens []lexer.Token) (postgresIndexRef, bool) {
	prefix, ok := consumeCreateIndexPrefix(tokens)
	if !ok {
		return postgresIndexRef{}, false
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
func parseIndexNameTokens(tokens []lexer.Token) (postgresIndexRef, bool) {
	// CREATE INDEX ON t (c) names nothing; ON here is the clause, not a name.
	if tokens[0].MatchIdentifierValue("ON") {
		return postgresIndexRef{}, false
	}
	name, ok := postgresIdentifierValue(tokens[0])
	if !ok {
		return postgresIndexRef{}, false
	}
	tokens = tokens[1:]
	if len(tokens) == 0 || !tokens[0].MatchIdentifierValue("ON") {
		return postgresIndexRef{}, false
	}
	tokens = skipKeywordToken(tokens[1:], "ONLY")
	if len(tokens) == 0 {
		return postgresIndexRef{}, false
	}
	firstTablePart, ok := postgresIdentifierValue(tokens[0])
	if !ok {
		return postgresIndexRef{}, false
	}
	if len(tokens) < 3 || !tokens[1].MatchOperatorValue(".") {
		return postgresIndexRef{Table: firstTablePart, Name: name}, true
	}
	table, ok := postgresIdentifierValue(tokens[2])
	if !ok {
		return postgresIndexRef{}, false
	}
	return postgresIndexRef{Schema: firstTablePart, Table: table, Name: name}, true
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
