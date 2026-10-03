package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/pgsnapshot"
)

// CaptureStartingPoint records the database as it stands, for a dev database
// an atlas.hcl docker block provisioned, whose state after the provisioning
// is its starting point. A reset handed the record in [dbreset.Kept] returns
// the database to it; see [pgsnapshot].
func (w *PostgreSQLWriter) CaptureStartingPoint(ctx context.Context) (*pgsnapshot.Snapshot, error) {
	if w.db == nil {
		return nil, fmt.Errorf("no database connection")
	}
	start, err := pgsnapshot.Read(ctx, w.db)
	if err != nil {
		return nil, fmt.Errorf("record the dev database's starting point: %w", err)
	}
	return &start, nil
}

// returnToStartingPoint returns the database to kept.StartingPoint in one
// transaction, and refuses to commit a database that does not match it.
//
// It refuses a database-scoped object the run created, as every realm cleanup
// does, a starting-point object the run dropped, which no statement can
// create again, and a setting the run changed that no statement restores,
// such as a column's type: committed, the next run would start from a
// different state without saying so. The checks on the starting point run
// again after the statements, since a CASCADE drop of something the run added
// can take a starting-point object that the run made depend on it.
func (w *PostgreSQLWriter) returnToStartingPoint(ctx context.Context, kept dbreset.Kept) (resultErr error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		finishPostgresCleanupTransaction(tx, &resultErr)
	}()
	capabilities, err := inspectCleanupCapabilities(ctx, tx)
	if err != nil {
		return err
	}
	if w.readsDatabaseArtifacts(capabilities) {
		if err := rejectPostgresDatabaseScopedArtifacts(ctx, tx, kept.Artifacts); err != nil {
			return err
		}
	}
	start := *kept.StartingPoint
	current, err := pgsnapshot.Read(ctx, tx)
	if err != nil {
		return err
	}
	if err := refuseMissingStartingPoint(start, current); err != nil {
		return err
	}
	for _, statement := range pgsnapshot.Plan(start, current) {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("return the dev database to its starting point: %w\nSQL: %s", err, statement)
		}
	}
	after, err := pgsnapshot.Read(ctx, tx)
	if err != nil {
		return err
	}
	if err := refuseMissingStartingPoint(start, after); err != nil {
		return err
	}
	if changed := pgsnapshot.Changed(start, after); len(changed) > 0 {
		return fmt.Errorf("%w: %s", errStartingPointChanged, strings.Join(changed, ", "))
	}
	if left := pgsnapshot.Plan(start, after); len(left) > 0 {
		return fmt.Errorf("the dev database still differs from its starting point after the reset: %s", left[0])
	}
	if w.readsDatabaseArtifacts(capabilities) {
		if err := verifyKeptDatabaseScopedArtifacts(ctx, tx, kept.Artifacts); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// errStartingPointGone is wrapped by a reset that found part of the starting
// point gone.
var errStartingPointGone = errors.New("the run dropped part of the dev database's starting point, which the reset cannot create again")

// errStartingPointChanged is wrapped by a reset that found a setting of the
// starting point changed, and no statement to change it back.
var errStartingPointChanged = errors.New("the run changed part of the dev database's starting point, which the reset cannot change back")

// refuseMissingStartingPoint refuses current when it lacks an object or a
// column start holds.
func refuseMissingStartingPoint(start, current pgsnapshot.Snapshot) error {
	missing := pgsnapshot.Missing(start, current)
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s", errStartingPointGone, strings.Join(missing, ", "))
}
