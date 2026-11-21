// Package probetx checks that a probe transaction is still the one its owner
// began.
//
// A probe runs declared text inside a transaction its owner rolls back, so
// nothing the probe creates outlives it. Text that ends the transaction and
// begins another defeats that without an error: the probe's own savepoint can
// be created again in the new transaction, its rollback succeeds there, and the
// final rollback undoes only the new transaction. ptah.run/internal/probetext keeps
// such text out of a probe; this package is the check behind it, which notices
// when one got through anyway and stops the command.
//
// The owner of the transaction, dbschema's rolled-back transaction, records the
// transaction it began with [Watch] and checks it again with [Settle] when the
// body returns.
// A body that runs probes one after another checks after each with [Check], so
// the error names the probe and nothing runs after it.
package probetx

import (
	"context"
	"errors"
	"fmt"
)

// ErrTransactionChanged reports that a probe ended the rolled-back transaction
// it ran in. Statements it ran may be committed, so a command that sees it
// stops rather than reading anything the probe answered.
var ErrTransactionChanged = errors.New("a probe ended the transaction it ran in; statements it ran may be committed")

// watch is the check a context carries: how to name the transaction now, and
// the name it had when it began.
type watch struct {
	identify func(context.Context) (string, error)
	began    string
}

type watchKey struct{}

// Watch names the transaction identify names now and returns a context that
// carries the check against that name. identify answers the engine's name for
// the transaction the caller holds, or empty for an engine that names none, in
// which case ctx is returned as it is and nothing is checked.
func Watch(ctx context.Context, identify func(context.Context) (string, error)) (context.Context, error) {
	began, err := identify(ctx)
	if err != nil {
		return ctx, err
	}
	if began == "" {
		return ctx, nil
	}
	return context.WithValue(ctx, watchKey{}, watch{identify: identify, began: began}), nil
}

// Check names the transaction again and reports [ErrTransactionChanged] when it
// is not the one [Watch] recorded. It answers nil for a context [Watch] did not
// return, and the error of identify when the server cannot be asked.
func Check(ctx context.Context) error {
	watched, ok := ctx.Value(watchKey{}).(watch)
	if !ok {
		return nil
	}
	now, err := watched.identify(ctx)
	if err != nil {
		return err
	}
	if now != watched.began {
		return fmt.Errorf("%w (transaction %s, now %s)", ErrTransactionChanged, watched.began, now)
	}
	return nil
}

// Settle is [Check] for the owner of the transaction once the body returns.
// The body may leave the transaction failed, so the server answers nothing in
// it: a statement PostgreSQL refuses aborts the transaction, and a body that
// runs one probe answers the refusal without a savepoint of its own. Where the
// transaction cannot be named, Settle runs restore, which rolls back to a
// savepoint the owner set when it began watching, and names the transaction
// again. The transaction the owner began holds that savepoint; one a probe
// began does not, and then Settle answers both errors, the second saying
// that a probe may have ended the transaction. A nil restore leaves Settle
// the same as [Check].
func Settle(ctx context.Context, restore func(context.Context) error) error {
	err := Check(ctx)
	if err == nil || errors.Is(err, ErrTransactionChanged) || restore == nil {
		return err
	}
	if restoreErr := restore(ctx); restoreErr != nil {
		return fmt.Errorf("%w; the savepoint set when the transaction began cannot be rolled back to either, "+
			"so a probe may have ended the transaction: %w", err, restoreErr)
	}
	return Check(ctx)
}

// Watched reports whether ctx carries a check from [Watch], so that an owner
// sets up what [Settle] needs only where the engine names its transactions.
func Watched(ctx context.Context) bool {
	_, ok := ctx.Value(watchKey{}).(watch)
	return ok
}
