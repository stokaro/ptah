package shadowdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/dbschema"
	"ptah.run/internal/devclean"
	"ptah.run/internal/migrateclean"
	"ptah.run/internal/replaytrust"
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
//
// A lease also carries the guard every replay into the database runs through,
// installed by [ReplayMigrator] and [GuardedMigrator].
type Lease struct {
	conn     *dbschema.DatabaseConnection
	baseline devclean.Baseline
	metadata []string
	guard    *devclean.ReplayGuard
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
//
// The claim also builds the lease's replay guard, with the caller's trust, so
// a session the guard refuses, such as a MySQL client character set it cannot
// read statements in, refuses the claim before anything resets the database;
// see [devclean.NewShadowReplayGuardFor].
func Claim(ctx context.Context, conn *dbschema.DatabaseConnection, trust devclean.ShadowTrust) (*Lease, error) {
	baseline, err := devclean.Claim(ctx, conn)
	if notClean, ok := errors.AsType[*migrateclean.NotCleanError](err); ok {
		return nil, fmt.Errorf("%w; Ptah resets this database before and after the replay, so point the URL at an empty database", notClean)
	}
	if err != nil {
		return nil, err
	}
	guard, err := replayGuard(ctx, conn, baseline, trust)
	if err != nil {
		return nil, err
	}
	return &Lease{conn: conn, baseline: baseline, guard: guard}, nil
}

// DefaultTrustRemedy ends a refusal that trust would lift when the caller
// names no remedy of its own: the field of the public options that trusts the
// SQL.
const DefaultTrustRemedy = "if you trust this SQL to run with the shadow connection's privileges, " +
	"set ReplayTrust.Trusted in the options of the verification"

// Trust is the caller's trust in the SQL a replay runs on the shadow database,
// and the remedies a refusal ends with, in the form [Claim] takes. An empty
// remedy is [DefaultTrustRemedy]; an empty serverRemedy appends none.
func Trust(trusted bool, remedy, serverRemedy string) devclean.ShadowTrust {
	if remedy == "" {
		remedy = DefaultTrustRemedy
	}
	return devclean.ShadowTrust{Trusted: trusted, Remedy: remedy, ServerRemedy: serverRemedy}
}

// RecordTrust notes that the caller trusts the SQL replayed through rawURL, so
// an engine a provider serves opens its session for trusted SQL, and the
// provider's own checks lift what the replay guard lifts under trust; see
// [replaytrust.Trusted]. It belongs before the connection to rawURL opens, and
// returns the release that ends the note. An untrusted replay records nothing,
// and neither does an empty URL, which names the temporary database [Open]
// provisions.
func RecordTrust(rawURL string, trusted bool) func() {
	if !trusted || strings.TrimSpace(rawURL) == "" {
		return func() {}
	}
	return replaytrust.Record(rawURL)
}

// replayGuard is the guard of a replay into conn, a database claimed with
// baseline: the dev replay guard every replay of a migration history runs
// through, with the realm and the session reading
// [devclean.NewShadowReplayGuardFor] gives it, and trust.
//
// The lease's release resets the database with baseline, so the guard accepts
// what that reset takes back: a comment on a schema it returns to its recorded
// state, and a publication the claim did not find. A body the migrator runs
// with the transactions it opens itself, as golang-migrate sends it, has its
// transaction control read the way that runner reads it; the migrator asks
// for that reading of no other body.
func replayGuard(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	baseline devclean.Baseline,
	trust devclean.ShadowTrust,
) (*devclean.ReplayGuard, error) {
	guard, err := devclean.NewShadowReplayGuardFor(ctx, conn, trust)
	if err != nil {
		return nil, err
	}
	return guard.WithOwnTransactions().WithUnwrappedTransactions().
		WithSchemaCommentReturned(baseline.ReturnsSchemaState).WithPublications(baseline.Publications), nil
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
