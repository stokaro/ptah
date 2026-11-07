package migrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"ptah.run/core/platform/capability"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/migrationsplit"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/builtin"
)

// How a migration body runs on an engine with query units.
//
// Some engines cannot run a body statement by statement: a definition exists
// only in the query that holds it, or a schema statement runs only outside a
// transaction and never beside a data statement. Such an engine runs a body as
// the units it divides it into, and those units are what the migrator counts,
// records progress over, digests in partial hashes and resumes from, in place
// of statements. The units come from a provider that negotiated
// units.migration-split (internal/migrationsplit), asked once per body before
// a run touches anything. A database/sql connection runs a body statement by
// statement.
//
// A unit the engine runs only outside a transaction is marked in flight
// before it runs and checkpointed after, as every statement outside a
// transaction is: an interruption between the two leaves an outcome nobody
// knows, and a resume refuses to guess it. A unit that changes data only runs
// in one serializable transaction together with the checkpoint that records
// it, so the two commit together or not at all.
//
// The revision row is therefore the record of what a body committed, and a
// failure never records less than the row holds. A resume after an
// interruption starts at the first unit the row does not record, which for a
// unit that commits with its checkpoint is exactly the one that did not
// commit.

// queryUnit is one unit a body runs as.
type queryUnit struct {
	// text is what the engine executes, with every definition and setting
	// the unit needs carried in from earlier statements.
	text string
	// source is the unit's own statements as written, which partial hashes
	// digest and a failure records.
	source string
	// data reports a unit that changes data only: it commits together with
	// the checkpoint that records it.
	data bool
}

// queryUnits divides a body into the units an engine runs it as.
type queryUnits interface {
	// split returns the units of sqlText, or an error for a body the engine
	// cannot run as units.
	split(sqlText string) ([]queryUnit, error)
	// refuse returns why a run must not start sqlText, or nil.
	refuse(sqlText string) error
}

// bodyGrammar is how the migrator divides a body on one target: into the
// units of an engine that has them, and into statements by the dialect's
// rules otherwise.
type bodyGrammar struct {
	dialect string
	units   queryUnits
}

// grammarForDialect is the grammar of a database/sql connection to dialect,
// which runs a body statement by statement.
func grammarForDialect(dialect string) bodyGrammar {
	return bodyGrammar{dialect: dialect}
}

// grammarFor is the grammar of conn. A provider's session splits by its
// provider when the provider negotiated units.migration-split, and by the
// dialect's statement rules otherwise, whatever its engine is named.
func grammarFor(conn *dbschema.DatabaseConnection) bodyGrammar {
	splitter, provider := providerSplitter(conn)
	switch {
	case !provider:
		return grammarForDialect(databaseConnectionDialect(conn))
	case splitter.SplitsMigrations():
		return bodyGrammar{dialect: databaseConnectionDialect(conn), units: providerUnits{splitter: splitter}}
	default:
		return bodyGrammar{dialect: databaseConnectionDialect(conn)}
	}
}

// grammar is the grammar of the migrator's connection.
func (m *Migrator) grammar() bodyGrammar { return grammarFor(m.conn) }

// providerSplitter returns the splitter of conn's schema writer, and reports
// whether conn is a provider's session at all. Every provider session
// implements migrationsplit.Splitter; whether its provider splits is
// [migrationsplit.Splitter.SplitsMigrations].
func providerSplitter(conn *dbschema.DatabaseConnection) (migrationsplit.Splitter, bool) {
	if conn == nil {
		return nil, false
	}
	splitter, provider := conn.SchemaWriter().(migrationsplit.Splitter)
	return splitter, provider
}

// migrationSplitter returns the splitter of conn when its provider splits
// migration bodies.
func migrationSplitter(conn *dbschema.DatabaseConnection) (migrationsplit.Splitter, bool) {
	splitter, provider := providerSplitter(conn)
	if !provider || !splitter.SplitsMigrations() {
		return nil, false
	}
	return splitter, true
}

// servicesFor returns what conn's engine gives the migrator beyond its
// descriptor; see internal/migratorext. A provider's session takes no check
// rule from a built-in engine of the same name: a provider's split and
// verdicts come through its session (see [grammarFor] and
// [Migrator.checkAssertionRule]), and the provider proves what a check reads
// when it runs it.
func servicesFor(conn *dbschema.DatabaseConnection) migratorext.Services {
	services := builtin.Lookup(databaseConnectionDialect(conn))
	if _, provider := providerSplitter(conn); provider {
		services.Checks = nil
		services.RollbackSurvivors = nil
	}
	return services
}

func (g bodyGrammar) hasUnits() bool { return g.units != nil }

// statements returns the texts of the units sqlText runs as, or its
// statements on a target without units. A body the split refuses is refused
// before anything runs (see [bodyGrammar.refuse]), so the statements returned
// for it here are counted but never executed.
func (g bodyGrammar) statements(sqlText string) []string {
	if g.units == nil {
		return statementsAsWritten(sqlText, g.dialect)
	}
	units, err := g.units.split(sqlText)
	if err != nil {
		return statementsAsWritten(sqlText, g.dialect)
	}
	texts := make([]string, 0, len(units))
	for _, unit := range units {
		texts = append(texts, unit.text)
	}
	return texts
}

// count is the number of units, or statements, sqlText runs as.
func (g bodyGrammar) count(sqlText string) int { return len(g.statements(sqlText)) }

// sourceStatements returns the source text of each unit sqlText runs as,
// which is what partial hashes digest and what a failure records as the
// failing statement.
func (g bodyGrammar) sourceStatements(sqlText string) []sqlutil.SourceStatement {
	if g.units == nil {
		return sqlutil.SplitSourceStatements(sqlText, g.dialect)
	}
	units, err := g.units.split(sqlText)
	if err != nil {
		return sqlutil.SplitSourceStatements(sqlText, g.dialect)
	}
	statements := make([]sqlutil.SourceStatement, 0, len(units))
	for _, unit := range units {
		statements = append(statements, sqlutil.SourceStatement{
			Text:       unit.source,
			Terminated: strings.HasSuffix(unit.source, ";"),
		})
	}
	return statements
}

// refuse returns why a run must not start sqlText on a target with units, and
// nil on every other target.
func (g bodyGrammar) refuse(sqlText string) error {
	if g.units == nil {
		return nil
	}
	return g.units.refuse(sqlText)
}

// commitsWithCheckpoint reports whether the unit at index, counted from one,
// of sqlText changes data only and so commits together with its checkpoint.
func (g bodyGrammar) commitsWithCheckpoint(sqlText string, index int) bool {
	if g.units == nil {
		return false
	}
	units, err := g.units.split(sqlText)
	if err != nil || index < 1 || index > len(units) {
		return false
	}
	return units[index-1].data
}

// outsideUnit returns the first unit of sqlText that runs outside any
// transaction, counted from one, or zero when every unit commits with its
// checkpoint.
func (g bodyGrammar) outsideUnit(sqlText string) int {
	if g.units == nil {
		return 0
	}
	units, err := g.units.split(sqlText)
	if err != nil {
		return 0
	}
	for i, unit := range units {
		if !unit.data {
			return i + 1
		}
	}
	return 0
}

// refuseUnsplittableMigrations refuses, before a run touches anything, a
// migration whose body in direction the target refuses to run as units. A
// provider is asked for the split of every body here, with its check
// assertions, so a refusal comes before the first statement of the run.
func (m *Migrator) refuseUnsplittableMigrations(ctx context.Context, migrations []*Migration, direction MigrationDirection) error {
	if err := m.askForSplits(ctx, migrations, direction); err != nil {
		return err
	}
	grammar := m.grammar()
	for _, migration := range migrations {
		if err := grammar.refuse(migrationSQLForDirection(migration, direction)); err != nil {
			return fmt.Errorf("migration %d cannot run %s on %s: %w",
				migration.Version, direction, m.connectionDialect(), err)
		}
	}
	return nil
}

// askForSplits asks a provider that splits migration bodies for the split of
// each body in direction and the verdicts on its check assertions. The
// provider's session keeps each answer, so every later question about the same
// body, a count or a partial hash among them, is answered without asking
// again. A target without a provider split asks nothing.
func (m *Migrator) askForSplits(ctx context.Context, migrations []*Migration, direction MigrationDirection) error {
	splitter, splits := migrationSplitter(m.conn)
	if !splits {
		return nil
	}
	for _, migration := range migrations {
		body := migrationSQLForDirection(migration, direction)
		if _, err := splitter.SplitMigration(ctx, body, m.checkAssertions(migration, direction)); err != nil {
			return fmt.Errorf("migration %d cannot run %s on %s: %w", migration.Version, direction, m.connectionDialect(), err)
		}
	}
	return nil
}

// refuseUnsplitBody refuses a migration that would run outside a transaction
// on a provider that does not split migration bodies, when its body holds more
// than one statement. The host's generic grammar cannot be trusted to divide
// a provider engine's text into what the engine runs, and outside a
// transaction a statement that ran stays applied when a later one fails, so
// only a body of one statement runs.
func (m *Migrator) refuseUnsplitBody(migration *Migration, direction MigrationDirection, mode MigrationTxMode) error {
	if mode != MigrationTxModeNone {
		return nil
	}
	splitter, provider := providerSplitter(m.conn)
	if !provider || splitter.SplitsMigrations() {
		return nil
	}
	count := m.migrationStatementCount(migrationSQLForDirection(migration, direction))
	if count <= 1 {
		return nil
	}
	return fmt.Errorf("migration %d cannot run %s on %s: the provider does not split a migration body into the units "+
		"its engine runs (%s), so a body of %d statements cannot run outside a transaction; upgrade the provider",
		migration.Version, direction, m.connectionDialect(), migrationSplitFeature, count)
}

// migrationSplitFeature names the contract feature a refusal points to.
const migrationSplitFeature = "units.migration-split"

// refuseOutsideUnitInTransaction refuses a migration that would run in one
// transaction, under file or all, on a target with transactional DDL whose
// split places one of its units outside every transaction. The engine does
// not run that unit in a transaction, so the transaction cannot hold the body;
// the migrator refuses rather than move the unit out and run the file as
// several transactions. Without transactional DDL, file runs as none (see
// [Migrator.runsQueriesOnTheirOwn]) and all is refused by the capability.
func (m *Migrator) refuseOutsideUnitInTransaction(migration *Migration, direction MigrationDirection, mode MigrationTxMode) error {
	if mode == MigrationTxModeNone || m.conn == nil || !m.conn.Info().Capabilities.Has(capability.TransactionalDDL) {
		return nil
	}
	unit := m.grammar().outsideUnit(migrationSQLForDirection(migration, direction))
	if unit == 0 {
		return nil
	}
	return fmt.Errorf("migration %d cannot run %s on %s with tx-mode %s: its unit %d runs outside any transaction, "+
		"so one transaction cannot hold the body; run it with tx-mode none, which commits each unit on its own",
		migration.Version, direction, m.connectionDialect(), mode, unit)
}

// refusePlacement refuses, before a run touches anything, a migration whose
// units cannot run in mode on this target; see [Migrator.refuseUnsplitBody]
// and [Migrator.refuseOutsideUnitInTransaction].
func (m *Migrator) refusePlacement(migration *Migration, direction MigrationDirection, mode MigrationTxMode) error {
	if err := m.refuseUnsplitBody(migration, direction, mode); err != nil {
		return err
	}
	return m.refuseOutsideUnitInTransaction(migration, direction, mode)
}

// refuseUpPlacements applies [Migrator.refusePlacement] to every migration of
// an up run, in the mode each resolves to. A migration whose mode does not
// resolve is left to the step that runs it, which reports why.
func (m *Migrator) refuseUpPlacements(migrations []*Migration) error {
	for _, migration := range migrations {
		mode, err := m.resolveUpMigrationTxMode(migration)
		if err != nil {
			continue
		}
		if err := m.refusePlacement(migration, MigrationDirectionUp, mode); err != nil {
			return err
		}
	}
	return nil
}

// runsQueriesOnTheirOwn reports whether the target runs every unit of a
// migration on its own, whatever transaction mode the run asked for: an
// engine with units and without transactional DDL has no transaction a schema
// unit can join, so a file's transaction would hold nothing but data units,
// which already commit one by one together with their checkpoints. `file` is
// therefore run the way `none` is, with progress recorded after each unit,
// and `all` is refused by the transactional-DDL capability before anything
// runs. On an engine with transactional DDL a file's transaction holds every
// unit, and a split that places one outside every transaction is refused (see
// [Migrator.refuseOutsideUnitInTransaction]).
func (m *Migrator) runsQueriesOnTheirOwn() bool {
	if m.conn == nil || !m.grammar().hasUnits() {
		return false
	}
	return !m.conn.Info().Capabilities.Has(capability.TransactionalDDL)
}

// effectiveTxMode is the mode a migration runs in on this target, given the
// mode it resolved to.
func (m *Migrator) effectiveTxMode(mode MigrationTxMode) MigrationTxMode {
	if mode == MigrationTxModeFile && m.runsQueriesOnTheirOwn() {
		return MigrationTxModeNone
	}
	return mode
}

// withRecordedStatementProgress installs what records a body's progress while
// it runs outside a transaction: a mark before each statement, a checkpoint
// after it, and on a target with units the committer that runs a unit that
// changes data only together with its checkpoint in one transaction.
func (m *Migrator) withRecordedStatementProgress(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	direction MigrationDirection,
) context.Context {
	ctx = withStatementProgressRecorder(
		ctx,
		func(ctx context.Context, event StatementEvent) error {
			return m.markMigrationStatementInFlight(ctx, migration, startedAt, event, direction)
		},
		func(ctx context.Context, event StatementEvent) error {
			return m.checkpointMigrationRevision(ctx, migration, startedAt, event, direction)
		},
	)
	grammar := m.grammar()
	if !grammar.hasUnits() || m.conn.Writer().IsDryRun() {
		return ctx
	}
	sqlText := migrationSQLForDirection(migration, direction)
	return withStatementCommitter(ctx, statementCommitter{
		claims: func(event StatementEvent) bool { return grammar.commitsWithCheckpoint(sqlText, event.Index) },
		run: func(ctx context.Context, event StatementEvent) error {
			return m.commitDataQuery(ctx, migration, startedAt, event, direction)
		},
	})
}

// conflictAttempts bounds how often a transaction the engine aborted for a
// conflicting one is run again.
const conflictAttempts = 5

// commitDataQuery runs one unit that changes data only and the checkpoint
// that records it in one serializable transaction; see [dataQueryCommit].
func (m *Migrator) commitDataQuery(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	event StatementEvent,
	direction MigrationDirection,
) error {
	recorded := func(ctx context.Context) (bool, error) {
		revision, err := m.getMigrationRevision(ctx, migration)
		if err != nil {
			return false, err
		}
		return checkpointRecorded(revision, event.Index), nil
	}
	splitter, splits := migrationSplitter(m.conn)
	if !splits {
		return fmt.Errorf("migration %d: unit %d changes data only, and only a provider that splits migration bodies "+
			"commits such a unit with its checkpoint", migration.Version, event.Index)
	}
	return providerDataQueryCommit(m.conn, splitter, event.Statement, func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		return m.checkpointMigrationRevisionOn(ctx, conn, migration, startedAt, event, direction)
	}, recorded).run(ctx)
}

// dataQueryCommit runs a unit that changes data only and the checkpoint that
// records it in one serializable transaction.
//
// An engine with optimistic locking aborts the transaction of two conflicting
// ones that commits second, and nothing of an aborted transaction is applied,
// so the whole transaction runs again, up to conflictAttempts times. Any other
// failure leaves the transaction uncommitted and is returned, and the caller
// records the unit as not applied.
//
// A commit that fails without saying whether it took effect is answered by
// recorded, which reads the revision back: the checkpoint commits with the
// unit, so a recorded checkpoint is the proof that the unit did too.
type dataQueryCommit struct {
	// begin opens the transaction of one attempt.
	begin func(context.Context) (dataQueryAttempt, error)
	// retryable reports a failure the engine answered by aborting the
	// transaction for a conflict, after which a new attempt may succeed.
	retryable func(error) bool
	recorded  func(context.Context) (bool, error)
	wait      func(context.Context, int) error
}

// dataQueryAttempt is the transaction of one attempt.
type dataQueryAttempt interface {
	// execute runs the unit.
	execute(ctx context.Context) error
	// checkpoint records the unit in the revision table. It is built after
	// the unit ran in each attempt, so the execution time it records
	// includes the unit and is not carried over from an aborted attempt.
	checkpoint(ctx context.Context) error
	commit() error
	rollback()
}

func (d dataQueryCommit) run(ctx context.Context) error {
	var err error
	for attempt := range conflictAttempts {
		var committing bool
		committing, err = d.try(ctx)
		if err == nil {
			return nil
		}
		if committing && !d.retryable(err) {
			return d.outcome(ctx, err)
		}
		if !d.retryable(err) || attempt == conflictAttempts-1 {
			return err
		}
		if waitErr := d.wait(ctx, attempt); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}
	return err
}

// try is one attempt. committing reports that the failure, if any, was the
// commit's own, whose outcome the engine may not have reported.
func (d dataQueryCommit) try(ctx context.Context) (committing bool, err error) {
	tx, err := d.begin(ctx)
	if err != nil {
		return false, err
	}
	if err := tx.execute(ctx); err != nil {
		tx.rollback()
		return false, err
	}
	if err := tx.checkpoint(ctx); err != nil {
		tx.rollback()
		return false, fmt.Errorf("record the query in the revision table: %w", err)
	}
	// The migration lock is asked for last, right before the commit, which
	// is the step that cannot be taken back.
	if err := migrationLockLost(ctx); err != nil {
		tx.rollback()
		return false, err
	}
	if err := tx.commit(); err != nil {
		return true, err
	}
	return false, nil
}

// outcome answers a commit that failed without saying whether it took effect.
func (d dataQueryCommit) outcome(ctx context.Context, commitErr error) error {
	readCtx, cancel := durableRevisionWriteContext(ctx)
	defer cancel()
	recorded, err := d.recorded(readCtx)
	if err != nil {
		return errors.Join(commitErr, fmt.Errorf("read back whether the query committed: %w", err))
	}
	if recorded {
		return nil
	}
	return commitErr
}

// checkpointRecorded reports whether revision records the unit at index as
// committed: its progress reaches the unit, and no failure was written over
// the checkpoint, which clears the failure columns.
func checkpointRecorded(revision *MigrationRevision, index int) bool {
	return revision != nil && revision.Applied >= index && revision.Error == ""
}

// waitForConflictRetry waits longer after each aborted attempt.
func waitForConflictRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt+1) * 50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
