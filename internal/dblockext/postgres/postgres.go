// Package postgres takes the session advisory lock of PostgreSQL and
// YugabyteDB for internal/dblock: pg_try_advisory_lock, asked again until the
// wait ends, and pg_advisory_unlock. It also proves on a second session that
// the lock excludes other sessions, which a transaction-pooling proxy in front
// of the server can defeat.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"ptah.run/core/platform"
	"ptah.run/internal/dblockext"
)

// pollInterval is how often a wait asks for the lock again.
const pollInterval = 25 * time.Millisecond

// Services returns the advisory lock of PostgreSQL and YugabyteDB.
// CockroachDB and Spanner are in the family and are not served here: Ptah
// takes no advisory lock on either, so a run there takes a no-op lock.
func Services() dblockext.Services {
	return dblockext.Services{
		Dialects:  []string{platform.Postgres, platform.YugabyteDB},
		Session:   advisory{},
		Exclusion: advisory{},
	}
}

// Key returns the 64-bit pg_advisory_lock key derived from name.
func Key(name string) int64 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(name))
	return int64(hash.Sum32())
}

type advisory struct{}

// Acquire asks for the lock every pollInterval until it is granted, the
// timeout passes or ctx ends. A negative timeout asks once.
func (advisory) Acquire(ctx context.Context, session dblockext.Session, request dblockext.Request) error {
	lockCtx := ctx
	var cancel context.CancelFunc
	if request.Timeout > 0 {
		lockCtx, cancel = context.WithTimeout(ctx, request.Timeout)
		defer cancel()
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var acquired bool
		if err := session.QueryRowContext(
			lockCtx,
			"SELECT pg_try_advisory_lock($1)",
			Key(request.Name),
		).Scan(&acquired); err != nil {
			return &dblockext.AmbiguousError{Err: waitError(ctx, lockCtx, request, err)}
		}
		if acquired {
			return nil
		}
		if request.Timeout < 0 {
			return request.Held
		}

		select {
		case <-lockCtx.Done():
			return waitError(ctx, lockCtx, request, lockCtx.Err())
		case <-ticker.C:
		}
	}
}

// waitError is request.Held when the wait ended because its timeout passed
// while ctx was still live, and fallback otherwise.
func waitError(ctx, lockCtx context.Context, request dblockext.Request, fallback error) error {
	if request.Timeout > 0 &&
		errors.Is(lockCtx.Err(), context.DeadlineExceeded) &&
		ctx.Err() == nil {
		return request.Held
	}
	return fallback
}

// Release gives the lock back, and fails when the session did not hold it.
func (advisory) Release(ctx context.Context, session dblockext.Session, request dblockext.Request) error {
	var released bool
	if err := session.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", Key(request.Name)).Scan(&released); err != nil {
		return err
	}
	if !released {
		return fmt.Errorf("%s advisory lock was not held", request.Dialect)
	}
	return nil
}

// Excludes asks a second connection for the lock just taken.
//
// A PostgreSQL advisory lock is SESSION-scoped, and a transaction pooler hands
// a client whichever backend is free between transactions. So a second client
// can be handed the SAME backend the first one locked on, where the lock is
// reentrant and answers true again. Measured through PgBouncer 1.25.2 in
// transaction mode, two independent client connections and the same key:
//
//	first handle         acquired=true  backend=113
//	second handle        acquired=true  backend=113
//	first handle again   acquired=true  backend=113
//
// Every attempt succeeded and every one landed on backend 113. So the lock is
// not merely weakened there -- it excludes nothing at all, and two concurrent
// migration runs would both believe they held it. It fails OPEN, which is why
// internal/dblock refuses the run rather than warning (stokaro/ptah#1029).
//
// The check asks the property rather than identifying the proxy. Ptah does not
// need to know what sits in front of the database; it needs to know whether the
// lock it just took keeps anybody out, and that is one query from a second
// connection. On a direct server the second attempt answers false and the check
// costs a round trip.
func (advisory) Excludes(ctx context.Context, pool dblockext.Pool, request dblockext.Request) error {
	witness, err := pool.Conn(ctx)
	if err != nil {
		// A pool that cannot hand out a second connection cannot answer the
		// question. Refusing on that would fail every single-connection
		// topology, so the lock stands and the caller is not told a story
		// about it.
		return nil
	}
	defer func() { _ = witness.Close() }()

	var acquiredAgain bool
	if err := witness.QueryRowContext(
		ctx,
		"SELECT pg_try_advisory_lock($1)",
		Key(request.Name),
	).Scan(&acquiredAgain); err != nil {
		// The witness could not ask. That is not evidence of a broken lock.
		return nil
	}
	if !acquiredAgain {
		return nil
	}

	// The witness got the same lock, so nothing is excluded. Give its own claim
	// back before refusing, or the backend keeps a lock nobody will release.
	var released bool
	_ = witness.QueryRowContext(
		context.WithoutCancel(ctx),
		"SELECT pg_advisory_unlock($1)",
		Key(request.Name),
	).Scan(&released)

	return fmt.Errorf(
		"%s advisory lock %q excludes nothing on this connection: a second connection took the same "+
			"lock, which is what a transaction-pooling proxy such as PgBouncer produces -- it hands both "+
			"clients one backend session, where the lock is reentrant. Two runs would both believe they "+
			"held it",
		request.Dialect, request.Name)
}
