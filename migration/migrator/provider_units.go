package migrator

import (
	"context"
	"errors"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/migrationsplit"
)

// providerUnits are the units a provider that negotiated
// units.migration-split divides a body into. The provider's session keeps
// each answer, and a run asks for every body with its own context before it
// touches anything (see [Migrator.askForSplits]), so a split here is answered
// from what the session kept; the background context only reaches a provider
// for a body no run asked about, which a status count can be.
type providerUnits struct{ splitter migrationsplit.Splitter }

func (p providerUnits) split(sqlText string) ([]queryUnit, error) {
	split, err := p.splitter.SplitMigration(context.Background(), sqlText, nil)
	if err != nil {
		return nil, err
	}
	if split.Refusal != nil {
		return nil, providerRefusal(sqlText, *split.Refusal)
	}
	units := make([]queryUnit, 0, len(split.Units))
	for _, unit := range split.Units {
		units = append(units, queryUnit{text: unit.SQL, source: unit.Source, data: unit.Placement == migrationsplit.WithCheckpoint})
	}
	return units, nil
}

func (p providerUnits) refuse(sqlText string) error {
	_, err := p.split(sqlText)
	return err
}

// providerRefusal words a provider's refusal of a body: the text it names,
// quoted, and the provider's reason, which quotes nothing.
func providerRefusal(body string, refusal migrationsplit.Refusal) error {
	start, end := max(refusal.Start, 0), min(refusal.End, len(body))
	return fmt.Errorf("the provider refuses %q: %s", body[min(start, end):end], refusal.Reason)
}

// providerDataQueryCommit is the [dataQueryCommit] of a provider that splits
// migration bodies: each attempt opens a unit transaction, runs the unit in it
// and writes its checkpoint through the history store bound to it. The
// statement timeout reaches the provider with the unit, which stops a unit
// that runs longer and never commits its transaction; the migrator puts no
// deadline of its own on the unit. An attempt runs again only when the
// provider says the engine aborted the transaction for a conflict.
func providerDataQueryCommit(
	conn *dbschema.DatabaseConnection,
	splitter migrationsplit.Splitter,
	unit string,
	checkpoint func(context.Context, *dbschema.DatabaseConnection) error,
	recorded func(context.Context) (bool, error),
) dataQueryCommit {
	return dataQueryCommit{
		begin: func(ctx context.Context) (dataQueryAttempt, error) {
			tx, err := splitter.BeginUnitTransaction(ctx)
			if err != nil {
				return nil, err
			}
			return providerDataQuery{tx: tx, conn: conn.WithExecutor(tx), unit: unit, record: checkpoint}, nil
		},
		retryable: func(err error) bool { return errors.Is(err, migrationsplit.ErrTransactionAborted) },
		recorded:  recorded,
		wait:      waitForConflictRetry,
	}
}

// providerDataQuery is one attempt's unit transaction.
type providerDataQuery struct {
	tx     catalog.SchemaTransaction
	conn   *dbschema.DatabaseConnection
	unit   string
	record func(context.Context, *dbschema.DatabaseConnection) error
}

func (q providerDataQuery) execute(ctx context.Context) error {
	return executeSQLOn(ctx, q.conn, q.unit)
}
func (q providerDataQuery) checkpoint(ctx context.Context) error {
	return q.record(ctx, q.conn)
}
func (q providerDataQuery) commit() error { return q.tx.Commit() }

// rollback ends a transaction the attempt will not commit. A failed unit has
// already left nothing the rollback could fail to undo, and the unit's own
// failure is the one the caller reports.
func (q providerDataQuery) rollback() { _ = q.tx.Rollback() }

// checkAssertions returns the assertion of every check a migration declares
// for direction, in the order its groups list them, or none when checks are
// skipped. A malformed check directive gives none here; the check run reports
// it.
func (m *Migrator) checkAssertions(migration *Migration, direction MigrationDirection) []string {
	if m.skipChecks {
		return nil
	}
	groups, err := m.migrationCheckGroups(migration, direction)
	if err != nil {
		return nil
	}
	var assertions []string
	for _, group := range groups {
		for _, check := range group.checks {
			assertions = append(assertions, check.Assert)
		}
	}
	return assertions
}

// assertionRule proves one check assertion read-only from its text, before
// anything runs it.
type assertionRule func(assertion string) error

// staticAssertionRule is the rule of a target whose provider gives no
// verdict: the shared text rules for the connection's dialect and server.
func staticAssertionRule(conn *dbschema.DatabaseConnection) assertionRule {
	info := conn.Info()
	return func(assertion string) error {
		return validateCheckAssertionStatically(assertion, info.Dialect, info.Version)
	}
}

// checkAssertionRule returns the rule a migration's check assertions are held
// to on conn. A provider that splits migration bodies judges each assertion
// itself, and its verdict replaces the shared text rules, which read the
// engine's language with another grammar. RunStatement with READ_ONLY still
// proves what the assertion reads when it runs.
func (m *Migrator) checkAssertionRule(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration, direction MigrationDirection) (assertionRule, error) {
	splitter, splits := migrationSplitter(conn)
	if !splits {
		return staticAssertionRule(conn), nil
	}
	assertions := m.checkAssertions(migration, direction)
	split, err := splitter.SplitMigration(ctx, migrationSQLForDirection(migration, direction), assertions)
	if err != nil {
		return nil, fmt.Errorf("ask the provider to judge the check assertions of migration %d: %w", migration.Version, err)
	}
	verdicts := make(map[string]migrationsplit.Verdict, len(assertions))
	for i, assertion := range assertions {
		verdicts[assertion] = split.Verdicts[i]
	}
	return func(assertion string) error {
		verdict, judged := verdicts[assertion]
		switch {
		case !judged:
			return fmt.Errorf("check assertion was not judged by the provider")
		case !verdict.ReadOnly:
			return fmt.Errorf("check assertion must be one read-only SELECT statement: %s", verdict.Reason)
		default:
			return nil
		}
	}, nil
}
