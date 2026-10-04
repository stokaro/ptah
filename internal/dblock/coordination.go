package dblock

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
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
// on YDB 26.2.1.14, with the session settings below: a second session waiting
// on a held semaphore acquired it at once when the first released it, 10.0
// seconds after the holder's process was killed, and 16.4 to 16.6 seconds
// after the holder's process was stopped, the session timeout being ten.
//
// The node is created by the first run that takes a lock and kept. It holds no
// data: each semaphore is ephemeral, so the server deletes it when the last
// session holding or waiting for it is gone. Dropping the node while no Ptah
// run is active is safe, and the next run creates it again. It sits at the
// root rather than under a dot-directory, which belong to the server and which
// Ptah never writes, and it is shared by every lock name, as an advisory lock's
// key space is shared by the database on the other engines. The schema reader
// leaves it out of every schema it reads.
const YDBLockNode = ydbschema.LockNode

// The settings of the coordination session that holds a lock.
//
// The server ends a session it has not heard from for ydbSessionTimeout, and
// with it every semaphore the session holds. The client reconnects a stream
// that has been silent for ydbKeepAliveTimeout, which is shorter, so a stream
// that went quiet is replaced while the server still keeps the session; the
// SDK's defaults are the other way round, ten seconds against five.
//
// Neither setting tells the holder in time that it lost the lock. Measured on
// 26.2.1.14 with the SDK's defaults: a holder whose process was stopped lost
// the semaphore to a waiter eight seconds later, and once resumed it went on
// believing it held the lock for five seconds more before the SDK ended the
// lease. So the holder asks the server every ydbHoldCheckInterval whether its
// session still owns the semaphore, and trusts an answer for ydbHoldValidity,
// half the session timeout: the server keeps the session for a full session
// timeout after the question reached it, so a lock is still held for at least
// half of one after its check passes. With this watch, the same stopped holder
// read the lock as lost before its next step, two milliseconds after it
// resumed.
const (
	ydbSessionTimeout    = 10 * time.Second
	ydbKeepAliveTimeout  = 2 * time.Second
	ydbHoldCheckInterval = time.Second
	ydbHoldValidity      = ydbSessionTimeout / 2
)

// ydbSessionStartTimeout bounds how long opening the lock's session may take.
// The SDK retries a session start the server refuses until its context ends,
// and does not say why: measured on 26.2.1.14, a user who may describe the
// node but not use it waited on it without end.
const ydbSessionStartTimeout = 10 * time.Second

// lockNodeMissing says what acquiring a YDB lock does when the coordination
// node it lives on does not exist.
type lockNodeMissing int

const (
	// createLockNode creates the node and takes the lock.
	createLockNode lockNodeMissing = iota
	// skipTheLock creates nothing and takes no lock, for a run that writes
	// nothing: no run that took a lock has used the database, so there is
	// nobody to wait for.
	skipTheLock
)

// ydbLockSessionDescription is how the session that holds a lock names itself
// to the server, so an operator who describes the semaphore sees whose it is.
const ydbLockSessionDescription = "ptah"

// acquireYDBLock takes the semaphore name on Ptah's coordination node through
// the SDK driver behind pool.
//
// A positive timeout bounds the wait in the server's queue, a zero one waits
// until ctx ends, and a negative one does not wait. The semaphore is exclusive
// and ephemeral, held by a coordination session of its own rather than by any
// SQL session.
//
// When the node does not exist, missing says whether to create it or to take
// no lock, in which case the hold is nil.
func acquireYDBLock(
	ctx context.Context,
	pool connOpener,
	name string,
	timeout time.Duration,
	missing lockNodeMissing,
) (*ydbHold, error) {
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
	return acquireYDBSemaphore(ctx, sdk, name, timeout, missing)
}

func acquireYDBSemaphore(
	ctx context.Context,
	sdk *ydbsdk.Driver,
	name string,
	timeout time.Duration,
	missing lockNodeMissing,
) (*ydbHold, error) {
	node := path.Join(sdk.Name(), YDBLockNode)
	exists, err := ydbLockNodeExists(ctx, sdk, node)
	if err != nil {
		return nil, err
	}
	if !exists && missing == skipTheLock {
		return nil, nil
	}
	if !exists {
		err := sdk.Coordination().CreateNode(ctx, node, coordination.NodeConfig{})
		switch {
		case ydbsdk.IsOperationError(err, Ydb.StatusIds_UNAUTHORIZED):
			// The SDK's error carries the masked credentials of the
			// connection, which a refusal has no need to repeat.
			return nil, fmt.Errorf("Ptah's locks live on the YDB coordination node %s, which does not exist, "+
				"and this user may not create it (UNAUTHORIZED). Run once as a user who may create a "+
				"coordination node at the database root, which creates it, and grant this user the right "+
				"to use it", node)
		case err != nil:
			return nil, fmt.Errorf("create the YDB coordination node %s, which holds Ptah's locks: %w",
				node, ydbschema.WithoutStackFrames(err))
		}
	}
	session, err := openYDBLockSession(ctx, sdk, node)
	if err != nil {
		return nil, err
	}
	lease, err := session.AcquireSemaphore(ctx, name, coordination.Exclusive,
		options.WithEphemeral(true), ydbAcquireTimeout(timeout))
	if err != nil {
		closeErr := ydbschema.WithoutStackFrames(session.Close(context.WithoutCancel(ctx)))
		if errors.Is(err, coordination.ErrAcquireTimeout) {
			return nil, errors.Join(&TimeoutError{Dialect: platform.YDB, Name: name, Timeout: timeout}, closeErr)
		}
		return nil, errors.Join(fmt.Errorf("acquire the YDB semaphore %q on %s: %w",
			name, node, ydbschema.WithoutStackFrames(err)), closeErr)
	}
	return newYDBHold(session, lease, name, time.Now), nil
}

// openYDBLockSession opens the coordination session that holds a lock, and
// gives up after ydbSessionStartTimeout.
func openYDBLockSession(ctx context.Context, sdk *ydbsdk.Driver, node string) (coordination.Session, error) {
	startCtx, cancel := context.WithTimeout(ctx, ydbSessionStartTimeout)
	defer cancel()
	session, err := sdk.Coordination().Session(startCtx, node,
		options.WithDescription(ydbLockSessionDescription),
		options.WithSessionTimeout(ydbSessionTimeout),
		options.WithSessionKeepAliveTimeout(ydbKeepAliveTimeout),
	)
	switch {
	case err == nil:
		return session, nil
	case ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded):
		return nil, fmt.Errorf("open a session on the YDB coordination node %s, which holds Ptah's locks: "+
			"the server opened none within %s. It refuses one to a user who may not use the node without "+
			"saying so, so check that this user may use it", node, ydbSessionStartTimeout)
	default:
		return nil, fmt.Errorf("open a session on the YDB coordination node %s: %w", node, ydbschema.WithoutStackFrames(err))
	}
}

// ydbLockNodeExists reports whether the coordination node exists. Asking
// first, rather than creating it on every run, is what lets a user who may
// use the node but not create one at the root take the lock.
func ydbLockNodeExists(ctx context.Context, sdk *ydbsdk.Driver, node string) (bool, error) {
	_, _, err := sdk.Coordination().DescribeNode(ctx, node)
	switch {
	case err == nil:
		return true, nil
	case ydbsdk.IsOperationError(err, Ydb.StatusIds_SCHEME_ERROR):
		// Measured on 26.2.1.14: a node that does not exist answers
		// SCHEME_ERROR, `Path not found`.
		return false, nil
	case ydbsdk.IsOperationError(err, Ydb.StatusIds_UNAUTHORIZED):
		return false, fmt.Errorf("this user may not use the YDB coordination node %s, which holds Ptah's "+
			"locks (UNAUTHORIZED); grant the user the right to use it", node)
	default:
		return false, fmt.Errorf("describe the YDB coordination node %s: %w", node, ydbschema.WithoutStackFrames(err))
	}
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

// semaphoreSession is the part of a coordination session a held lock uses.
// coordination.Session satisfies it.
type semaphoreSession interface {
	DescribeSemaphore(
		ctx context.Context,
		name string,
		opts ...options.DescribeSemaphoreOption,
	) (*coordination.SemaphoreDescription, error)
	SessionID() uint64
	Close(ctx context.Context) error
}

// semaphoreLease is the part of a lease a held lock uses. coordination.Lease
// satisfies it.
type semaphoreLease interface {
	Context() context.Context
	Release() error
}

// ydbHold is a held semaphore, and the watch that says whether it is still
// held.
//
// A watch goroutine asks the server, every ydbHoldCheckInterval, whether the
// session still owns the semaphore, and records when an answer that it does
// was asked for. The lock is lost when the server answers that it does not,
// when the SDK ends the lease, or when no such answer is younger than
// ydbHoldValidity -- which [ydbHold.held] also checks itself, so a holder that
// was stopped and resumes learns it before the watch has run again.
type ydbHold struct {
	session semaphoreSession
	lease   semaphoreLease
	name    string
	now     func() time.Time

	mu        sync.Mutex
	confirmed time.Time

	lostOnce sync.Once
	lostCh   chan struct{}
	released atomic.Bool

	stopWatch context.CancelFunc
	watching  chan struct{}
}

func newYDBHold(session semaphoreSession, lease semaphoreLease, name string, now func() time.Time) *ydbHold {
	watchCtx, stop := context.WithCancel(context.Background())
	hold := &ydbHold{
		session:   session,
		lease:     lease,
		name:      name,
		now:       now,
		confirmed: now(),
		lostCh:    make(chan struct{}),
		stopWatch: stop,
		watching:  make(chan struct{}),
	}
	go hold.watch(watchCtx)
	return hold
}

// lost returns a channel that is closed when the lock is lost.
func (h *ydbHold) lost() <-chan struct{} { return h.lostCh }

// held reports whether the lock may still be held: it has not been lost, and
// the last answer that it is held is younger than ydbHoldValidity. A released
// lock reads as held, since it was not taken away.
func (h *ydbHold) held() bool {
	if h.released.Load() {
		return true
	}
	select {
	case <-h.lostCh:
		return false
	default:
	}
	if h.lease.Context().Err() != nil {
		h.markLost()
		return false
	}
	h.mu.Lock()
	confirmed := h.confirmed
	h.mu.Unlock()
	if h.now().Sub(confirmed) > ydbHoldValidity {
		h.markLost()
		return false
	}
	return true
}

func (h *ydbHold) markLost() {
	h.lostOnce.Do(func() { close(h.lostCh) })
}

func (h *ydbHold) watch(ctx context.Context) {
	defer close(h.watching)
	ticker := time.NewTicker(ydbHoldCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.lease.Context().Done():
			h.markLost()
			return
		case <-ticker.C:
			h.check(ctx)
			if ctx.Err() == nil && !h.held() {
				return
			}
		}
	}
}

// check asks the server whether this session owns the semaphore. An answer
// that it does is recorded with the time the question was asked, an answer
// that it does not loses the lock, and no answer leaves the last one standing
// until it is too old.
func (h *ydbHold) check(ctx context.Context) {
	asked := h.now()
	checkCtx, cancel := context.WithTimeout(ctx, ydbHoldCheckInterval)
	defer cancel()
	description, err := h.session.DescribeSemaphore(checkCtx, h.name, options.WithDescribeOwners(true))
	if err != nil {
		return
	}
	id := h.session.SessionID()
	for _, owner := range description.Owners {
		if owner.SessionID == id {
			h.mu.Lock()
			h.confirmed = asked
			h.mu.Unlock()
			return
		}
	}
	h.markLost()
}

// release gives the semaphore back and ends the session that held it.
//
// Lease.Release takes no context, so ctx cannot bound it by itself. The lease
// is released on a goroutine, and when ctx ends first the session is closed,
// which ends a release still waiting for its answer; the server frees what the
// session held once it notices the session is gone. A lock already lost has
// nothing to give back, and its release only ends the session: measured on
// 26.2.1.14, giving back a lease the server had taken waited until the release
// context ended.
func (h *ydbHold) release(ctx context.Context) error {
	wasHeld := h.held()
	h.released.Store(true)
	h.stopWatch()
	<-h.watching
	if !wasHeld {
		return ydbschema.WithoutStackFrames(h.session.Close(ctx))
	}
	released := make(chan error, 1)
	go func() { released <- h.lease.Release() }()
	select {
	case err := <-released:
		if err != nil {
			err = fmt.Errorf("release the YDB semaphore %q: %w", h.name, ydbschema.WithoutStackFrames(err))
		}
		return errors.Join(err, ydbschema.WithoutStackFrames(h.session.Close(ctx)))
	case <-ctx.Done():
		_ = h.session.Close(ctx)
		return fmt.Errorf("release the YDB semaphore %q: %w", h.name, ctx.Err())
	}
}
