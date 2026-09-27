package shadowdb

import (
	"context"
	"errors"
	"fmt"

	"ptah.run/dbschema"
	"ptah.run/internal/devclean"
	"ptah.run/internal/migrateclean"
)

// Lease is a disposable database a verification has claimed: it held nothing
// the reset would drop when the verification started, and [Lease.Release]
// hands it back that way.
//
// A verification resets its database before it replays into it. Without the
// claim, that reset emptied a database holding a table the operator meant to
// keep. The claim needs the release: a verification that left its replay
// behind, revision table included, would find it on the next run and refuse
// the database it had filled itself (stokaro/ptah#3811).
type Lease struct {
	conn     *dbschema.DatabaseConnection
	baseline devclean.Baseline
	metadata []string
}

// Claim refuses a database that holds anything the reset would drop, before
// anything resets it, and returns the lease whose Release empties it again.
// The predicate is the dev database's, [migrateclean.DevRefusal], and so is
// the scope the lease resets over; see [devclean.Reset].
//
// The refusal names no flag: one verification reads its URL from --shadow-db
// on the native command and from --dev-url on the Atlas-compatible one.
//
// It belongs before the verification's first reset, and before the caller
// defers Release: deferred first, the release would run on the refusal too.
func Claim(ctx context.Context, conn *dbschema.DatabaseConnection) (*Lease, error) {
	baseline, err := devclean.Claim(ctx, conn)
	if notClean, ok := errors.AsType[*migrateclean.NotCleanError](err); ok {
		return nil, fmt.Errorf("%w; Ptah resets this database before and after the replay, so point the URL at an empty database", notClean)
	}
	if err != nil {
		return nil, err
	}
	return &Lease{conn: conn, baseline: baseline}, nil
}

// DropsMetadata names revision tables Release drops as well. A reset keeps a
// revision table on some dialects, since it is the migrator's bookkeeping
// rather than the schema under test, and one left behind is a table the next
// claim refuses.
func (l *Lease) DropsMetadata(tableIdentifiers ...string) {
	l.metadata = append(l.metadata, tableIdentifiers...)
}

// Release empties the database over the scope the claim judged and drops the
// revision tables named with DropsMetadata. It runs on a context of its own,
// so a canceled command still hands the database back; see
// [devclean.CleanupContext].
func (l *Lease) Release(ctx context.Context) error {
	cleanupCtx, cancel := devclean.CleanupContext(ctx, devclean.CleanupGrace)
	defer cancel()
	if err := devclean.Reset(cleanupCtx, l.conn, l.baseline); err != nil {
		return fmt.Errorf("empty the database after the replay: %w", err)
	}
	if err := DropMigrationMetadata(cleanupCtx, l.conn, l.metadata...); err != nil {
		return fmt.Errorf("empty the database after the replay: %w", err)
	}
	return nil
}
