package migrator

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/dbschema"
	"ptah.run/internal/lexer"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/builtin"
)

// The guard against an index a failed build left unusable is an engine
// service (see internal/migratorext.IndexGuard): the engine reads what a
// migration's conditional creates name and what its catalog holds, and words
// the refusal. This file runs it at the points a revision row could record a
// migration over such an index: before an up run, when the body completes,
// before a repair, and before a rollback completes.

type postgresIndexObservation struct {
	identities []migratorext.IndexState
}

func (m *Migrator) startPostgresIndexObservation() {
	if m.services().Indexes == nil || m.postgresIndexObservation != nil {
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
	guard := m.services().Indexes
	for _, ref := range guard.ConditionalCreates(statement) {
		state, err := guard.State(ctx, indexQueryer(conn), ref)
		if err != nil {
			return err
		}
		m.appendPostgresIndexIdentity(postgresIndexIdentityState(state, ref))
	}
	return m.observePostgresIndexConstraintAttachments(ctx, conn, statement, postgresSearchPathKnown)
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
	guard := m.services().Indexes
	for _, ref := range guard.ConditionalCreates(statement) {
		if ref.Schema != "" || searchPathKnowledge == postgresSearchPathKnown {
			state, err := guard.State(ctx, indexQueryer(conn), ref)
			if err != nil {
				return err
			}
			m.appendPostgresIndexIdentity(postgresIndexIdentityState(state, ref))
			continue
		}
		identities, err := guard.TargetsAcrossSchemas(ctx, indexQueryer(conn), ref)
		if err != nil {
			return err
		}
		if len(identities) == 0 {
			m.appendPostgresIndexIdentity(postgresIndexIdentityState(migratorext.IndexState{}, ref))
			continue
		}
		for _, identity := range identities {
			m.appendPostgresIndexIdentity(identity)
		}
	}
	return m.observePostgresIndexConstraintAttachments(ctx, conn, statement, searchPathKnowledge)
}

func (m *Migrator) appendPostgresIndexIdentity(identity migratorext.IndexState) {
	if slices.ContainsFunc(m.postgresIndexObservation.identities, func(existing migratorext.IndexState) bool {
		return samePostgresIndexIdentity(existing, identity)
	}) {
		return
	}
	m.postgresIndexObservation.identities = append(m.postgresIndexObservation.identities, identity)
}

func postgresIndexIdentityState(state migratorext.IndexState, ref migratorext.IndexRef) migratorext.IndexState {
	if !state.TargetFound {
		return migratorext.IndexState{
			TargetSchema: ref.Schema,
			TargetTable:  ref.Table,
			Name:         ref.Name,
		}
	}
	return migratorext.IndexState{
		TargetFound:  true,
		TargetSchema: state.TargetSchema,
		TargetTable:  state.TargetTable,
		TargetKind:   state.TargetKind,
		Name:         ref.Name,
	}
}

func samePostgresIndexIdentity(left, right migratorext.IndexState) bool {
	return left.TargetFound == right.TargetFound &&
		left.TargetSchema == right.TargetSchema &&
		left.TargetTable == right.TargetTable &&
		left.Name == right.Name
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
			migratorext.IndexRefusal{
				UnusableOutcome: "recording the migration applied would report a constraint that is not enforced",
				RetryAction:     "rerun the migration",
				UnsafeOutcome:   "recording the migration applied would report an index state the database does not provide",
			},
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
	guard := m.services().Indexes
	return guard != nil && len(guard.ConditionalCreates(migrationSQLForDirection(migration, direction))) > 0
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
		migratorext.IndexRefusal{
			UnusableOutcome: "completing the rollback would hide an unusable index behind a deleted revision",
			RetryAction:     "resume the rollback",
			UnsafeOutcome:   "completing the rollback would hide an index state the database does not provide behind a deleted revision",
		},
	)
}

func (m *Migrator) refuseRepairOverUnsafeIndexSQL(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	sqlText string,
	refusal migratorext.IndexRefusal,
) error {
	states, err := m.postgresIndexStatesCreatedBySQLOn(ctx, conn, sqlText)
	if err != nil {
		return err
	}
	unsafe := postgresUnsafeCompletionStates(m.services().Indexes, states)
	if len(unsafe) == 0 {
		return nil
	}
	return m.services().Indexes.RepairError(migration.Version, unsafe, refusal)
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
	run ownTransactionRun,
) error {
	if m.conn == nil || m.conn.Writer().IsDryRun() {
		return nil
	}
	if m.services().Indexes == nil {
		return nil
	}
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		executionConn := scoped.noTransactionConnection()
		if txMode == MigrationTxModeNone {
			return scoped.refuseUpOverUnsafeIndexOn(ctx, executionConn, migration, resumeFrom, run)
		}
		tx, err := executionConn.SchemaWriter().BeginTransaction(ctx)
		if err != nil {
			return fmt.Errorf("failed to open PostgreSQL index recovery preflight: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		return scoped.refuseUpOverUnsafeIndexOn(ctx, executionConn.WithExecutor(tx), migration, resumeFrom, run)
	})
}

func (m *Migrator) refuseUpOverUnsafeIndexOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	resumeFrom int,
	run ownTransactionRun,
) error {
	unsafe, err := m.postgresIndexStatesBlockingAttempt(ctx, conn, migration.UpSQL, resumeFrom, run)
	if err != nil || len(unsafe) == 0 {
		return err
	}
	return m.services().Indexes.ApplyError(migration.Version, unsafe)
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
	unsafe := postgresUnsafeCompletionStates(m.services().Indexes, states)
	if len(unsafe) == 0 {
		return nil
	}
	return m.services().Indexes.ApplyError(migration.Version, unsafe)
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
) ([]migratorext.IndexState, error) {
	if conn == nil {
		return nil, nil
	}
	guard := builtin.Lookup(conn.Info().Dialect).Indexes
	if guard == nil {
		return nil, nil
	}
	if m.postgresIndexObservation != nil {
		if len(guard.ConditionalCreates(sqlText)) > 0 && len(m.postgresIndexObservation.identities) == 0 {
			return nil, fmt.Errorf(
				"cannot verify conditional PostgreSQL index creates because their statement-local target identities were not observed",
			)
		}
		return m.refreshPostgresIndexObservations(ctx, conn)
	}
	return m.postgresIndexStatesOn(ctx, conn, guard.ConditionalCreates(sqlText))
}

func (m *Migrator) refreshPostgresIndexObservations(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) ([]migratorext.IndexState, error) {
	states := make([]migratorext.IndexState, 0, len(m.postgresIndexObservation.identities))
	for _, identity := range m.postgresIndexObservation.identities {
		if !identity.TargetFound {
			states = append(states, identity)
			continue
		}
		state, err := m.services().Indexes.State(ctx, indexQueryer(conn), migratorext.IndexRef{
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

func postgresUnsafeCompletionStates(guard migratorext.IndexGuard, states []migratorext.IndexState) []migratorext.IndexState {
	unsafe := make([]migratorext.IndexState, 0, len(states))
	for _, state := range states {
		if !guard.Usable(state) {
			unsafe = append(unsafe, state)
		}
	}
	return unsafe
}

func (m *Migrator) postgresIndexStatesOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	refs []migratorext.IndexRef,
) ([]migratorext.IndexState, error) {
	guard := builtin.Lookup(conn.Info().Dialect).Indexes
	if len(refs) == 0 || guard == nil {
		return nil, nil
	}
	states := make([]migratorext.IndexState, 0, len(refs))
	for _, ref := range refs {
		state, err := guard.State(ctx, indexQueryer(conn), ref)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
}

// indexQueryer is what an index guard reads the catalog through: the
// connection's writer when it can query, so a read inside the migration's
// transaction sees what the transaction did, and the connection otherwise.
func indexQueryer(conn *dbschema.DatabaseConnection) migratorext.RowsQueryer {
	if queryer, ok := conn.Writer().(migratorext.RowsQueryer); ok {
		return queryer
	}
	return conn
}

// postgresIndexStatesBlockingAttempt returns existing relations that would keep
// an attempted CREATE INDEX from leaving a usable index on its target. A DROP
// suppresses a conflict only after PostgreSQL's catalog and search path resolve
// it to the same schema-level relation name. Statements before resumeFrom were
// committed by an earlier attempt and are deliberately excluded. run
// describes a body that opens and closes its own transactions: the session
// state that ends with one of them, such as LOCK, is not replayed.
func (m *Migrator) postgresIndexStatesBlockingAttempt(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	sqlText string,
	resumeFrom int,
	run ownTransactionRun,
) ([]migratorext.IndexState, error) {
	if builtin.Lookup(conn.Info().Dialect).Indexes == nil {
		return nil, nil
	}
	statements := splitSQLStatementsForDialect(sqlText, conn.Info().Dialect)
	dropped := make(map[migratorext.IndexName]struct{})
	seen := make(map[migratorext.IndexName]struct{})
	var unsafe []migratorext.IndexState
	for i, statement := range statements {
		// A lock changes no name a statement resolves, and outside a
		// transaction PostgreSQL refuses it: replaying it here would refuse
		// the migration for a reason that is not the preflight's, before
		// any revision row exists. The statement meets the server's own
		// answer when it runs.
		if ownTransactionResumeAction(i, statement, conn.Info().Dialect, run) == noTransactionPrefixReplay &&
			!locksTables(statement, conn.Info().Dialect) {
			if err := executeSQLOn(ctx, conn, statement); err != nil {
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

// locksTables reports a LOCK statement.
func locksTables(statement, dialect string) bool {
	tokens := significantSQLTokens(statement, dialect)
	return len(tokens) > 0 && tokens[0].MatchIdentifierValue("LOCK")
}

func (m *Migrator) collectPostgresDroppedIndexNames(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statement string,
	dropped map[migratorext.IndexName]struct{},
) error {
	guard := m.services().Indexes
	for _, ref := range guard.Drops(statement) {
		name, found, err := guard.ResolveDropped(ctx, indexQueryer(conn), ref)
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
	seen map[migratorext.IndexName]struct{},
) ([]migratorext.IndexState, error) {
	guard := m.services().Indexes
	var unsafe []migratorext.IndexState
	for _, ref := range guard.ConditionalCreates(statement) {
		state, err := guard.State(ctx, indexQueryer(conn), ref)
		if err != nil {
			return nil, err
		}
		if !state.TargetFound && !state.RelationExists {
			continue
		}
		name := state.IndexName()
		if _, cleaned := dropped[name]; cleaned {
			continue
		}
		if !state.RelationExists {
			continue
		}
		if guard.Usable(state) {
			same, err := guard.SameDefinition(ctx, indexQueryer(conn), statement, state)
			if err != nil {
				return nil, err
			}
			if same {
				continue
			}
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		unsafe = append(unsafe, state)
	}
	return unsafe, nil
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

// PostgreSQL renames an index to the explicit constraint name when attaching
// it with ADD CONSTRAINT ... USING INDEX. Both execution and committed-prefix
// reconstruction must follow that name before checking the catalog.
func (m *Migrator) observePostgresIndexConstraintAttachments(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statement string,
	searchPathKnowledge postgresSearchPathKnowledge,
) error {
	guard := m.services().Indexes
	for _, attachment := range guard.Attachments(statement) {
		if attachment.Index.Schema == "" && searchPathKnowledge == postgresSearchPathKnown {
			target, err := guard.ResolveTarget(ctx, indexQueryer(conn), attachment.Index)
			if err != nil {
				return err
			}
			if !target.TargetFound {
				continue
			}
			attachment.Index.Schema = target.TargetSchema
		}
		m.postgresIndexObservation.attachConstraint(attachment)
	}
	return nil
}

func (o *postgresIndexObservation) attachConstraint(attachment migratorext.IndexAttachment) {
	for i := range o.identities {
		identity := &o.identities[i]
		if identity.TargetTable == attachment.Index.Table && identity.Name == attachment.Index.Name &&
			(attachment.Index.Schema == "" || identity.TargetSchema == attachment.Index.Schema) {
			identity.Name = attachment.Name
		}
	}
}
