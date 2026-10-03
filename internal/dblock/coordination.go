package dblock

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"time"

	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination/options"

	"ptah.run/core/platform"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// YDBLockNode is the coordination node, at the root of a YDB database, whose
// semaphores are Ptah's locks: one semaphore per lock name.
//
// YQL has no advisory lock. A coordination-node semaphore acquired
// exclusively by a session is the closest YDB has: another session that asks
// for it waits in line, and the server releases it when the session ends,
// whether the holder released it or died. Measured through ydb-go-sdk v3.153.2
// on YDB 26.2.1.14, a second session waiting on a held semaphore acquired it
// one second after the first released it, and 3.6 seconds after the first
// process's driver closed without releasing, its session timeout being three.
//
// The node is created on first use and kept. It holds no data: each semaphore
// is ephemeral, so the server deletes it when the last session holding or
// waiting for it is gone. Dropping the node while no Ptah run is active is
// safe, and the next run creates it again. It sits at the root rather than
// under a dot-directory, which belong to the server and which Ptah never
// writes, and it is shared by every lock name, as an advisory lock's key
// space is shared by the database on the other engines.
const YDBLockNode = "ptah_locks"

// ydbLockSessionDescription is how the session that holds a lock names itself
// to the server, so an operator who describes the semaphore sees whose it is.
const ydbLockSessionDescription = "ptah"

// acquireYDBLock takes the semaphore name on Ptah's coordination node through
// the SDK driver behind pool, and returns how to release it.
//
// A positive timeout bounds the wait in the server's queue, a zero one waits
// until ctx ends, and a negative one does not wait. The semaphore is exclusive
// and ephemeral, held by a coordination session of its own rather than by any
// SQL session, and released by giving it back and then ending that session.
func acquireYDBLock(
	ctx context.Context,
	pool connOpener,
	name string,
	timeout time.Duration,
) (func(context.Context) error, error) {
	session, err := pool.Conn(ctx)
	if err != nil {
		return nil, err
	}
	sdk, err := ydbschema.DriverOf(session)
	if closeErr := session.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return nil, fmt.Errorf("reach the YDB coordination service: %w", err)
	}
	return acquireYDBSemaphore(ctx, sdk, name, timeout)
}

func acquireYDBSemaphore(
	ctx context.Context,
	sdk *ydbsdk.Driver,
	name string,
	timeout time.Duration,
) (func(context.Context) error, error) {
	node := path.Join(sdk.Name(), YDBLockNode)
	// Creating a node that exists succeeds and changes nothing, measured on
	// 26.2.1.14, so every acquisition creates it rather than racing a check.
	if err := sdk.Coordination().CreateNode(ctx, node, coordination.NodeConfig{}); err != nil {
		return nil, fmt.Errorf("create the YDB coordination node %s: %w", node, err)
	}
	session, err := sdk.Coordination().Session(ctx, node, options.WithDescription(ydbLockSessionDescription))
	if err != nil {
		return nil, fmt.Errorf("open a session on the YDB coordination node %s: %w", node, err)
	}
	lease, err := session.AcquireSemaphore(ctx, name, coordination.Exclusive,
		options.WithEphemeral(true), ydbAcquireTimeout(timeout))
	if err != nil {
		closeErr := session.Close(context.WithoutCancel(ctx))
		if errors.Is(err, coordination.ErrAcquireTimeout) {
			return nil, errors.Join(&TimeoutError{Dialect: platform.YDB, Name: name, Timeout: timeout}, closeErr)
		}
		return nil, errors.Join(fmt.Errorf("acquire the YDB semaphore %q on %s: %w", name, node, err), closeErr)
	}
	return func(ctx context.Context) error {
		// The lease goes back first: closing the session alone leaves the
		// semaphore held until the server notices the session is gone, which
		// took five seconds in the measurement above.
		return errors.Join(lease.Release(), session.Close(ctx))
	}, nil
}

// ydbAcquireTimeout is how long the server keeps an acquisition in its queue.
// The server counts whole milliseconds and reads zero as "do not wait", so a
// positive timeout is rounded up rather than down to it.
func ydbAcquireTimeout(timeout time.Duration) options.AcquireSemaphoreOption {
	switch {
	case timeout < 0:
		return options.WithAcquireTimeout(0)
	case timeout == 0:
		return options.WithAcquireInfiniteTimeout()
	default:
		milliseconds := math.Ceil(float64(timeout) / float64(time.Millisecond))
		return options.WithAcquireTimeout(time.Duration(milliseconds) * time.Millisecond)
	}
}
