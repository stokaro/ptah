package conformance

import (
	"context"
	"fmt"
	"strconv"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

const (
	defaultLockName = "ptah-conformance"
	// releaseWait bounds how long the suite waits for a lock a closed
	// session held to become free: a provider may release it after Close
	// answers.
	releaseWait = 10 * time.Second
	releasePoll = 200 * time.Millisecond
)

func (r *run) lockName() string {
	if r.config.Live != nil && r.config.Live.LockName != "" {
		return r.config.Live.LockName
	}
	return defaultLockName
}

// lockStep is one lock call and the outcomes it may end with. A refusal of
// the call satisfies a step that only forbids an acquisition.
type lockStep struct {
	what      string
	operation provider.LockOperation
	want      []protocol.LockResult_Outcome
	// refusable says a refusal of the call satisfies the step.
	refusable bool
}

// lockHolder is a session of one process, the side of the lock checks that
// takes the lock or waits for it.
type lockHolder struct {
	c       *conn
	session *protocol.Session
	next    int
}

// lock sends one lock call and checks the receipt as the SDK's client does.
func (r *run) lock(ctx context.Context, h *lockHolder, lockID string, step lockStep) string {
	h.next++
	request := &protocol.LockRequest{Scope: &protocol.LockScope{SessionId: h.session.GetId(), LockId: lockID, Name: r.lockName()},
		RequestId: "ptah-conformance-lock-" + strconv.Itoa(h.next)}
	method := map[provider.LockOperation]string{provider.LockTry: "TryLock", provider.LockCheck: "CheckLock", provider.LockRelease: "ReleaseLock"}[step.operation]
	result, err := invoke(ctx, r, h.c, method, lockCall(h.c, method), request)
	if err != nil {
		if step.refusable {
			return ""
		}
		return fmt.Sprintf("%s: %s refused: %s", step.what, method, errorText(err))
	}
	r.observeResultReason(h.c, method, result.GetDiagnostic())
	if err := provider.ValidateLockResult(step.operation, request, result); err != nil {
		return fmt.Sprintf("%s: the receipt fails the SDK's validator: %v", step.what, err)
	}
	for _, outcome := range step.want {
		if result.GetOutcome() == outcome {
			return ""
		}
	}
	return fmt.Sprintf("%s: %s answered %s", step.what, method, result.GetOutcome())
}

// liveLocks takes one lock from two sessions in two processes.
func (r *run) liveLocks(ctx context.Context) {
	if !provider.EngineOffers(r.full, r.engine, provider.LocksFeature) {
		r.add(checkLocks, NotApplicable, "the engine does not offer sessions.locks")
		return
	}
	other := r.second
	if other == nil {
		var err error
		if other, err = r.connect(ctx, listedAll()); err != nil {
			r.add(checkLocks, Failed, "a second process: "+err.Error())
			return
		}
	}
	first, err := r.openSession(ctx, r.all, r.config.Live.Address)
	if err != nil {
		r.add(checkLocks, Failed, err.Error())
		return
	}
	defer func() { _ = r.closeSession(ctx, r.all, first.GetId()) }()
	second, err := r.openSession(ctx, other, r.config.Live.Address)
	if err != nil {
		r.add(checkLocks, Failed, "a session of a second process: "+err.Error())
		return
	}
	a, b := &lockHolder{c: r.all, session: first}, &lockHolder{c: other, session: second}
	failures := r.lockSequence(ctx, a, b)
	r.result(checkLocks, failures)
}

func (r *run) lockSequence(ctx context.Context, a, b *lockHolder) []string {
	acquired := []protocol.LockResult_Outcome{protocol.LockResult_ACQUIRED}
	notAcquired := []protocol.LockResult_Outcome{protocol.LockResult_BUSY, protocol.LockResult_RELEASED, protocol.LockResult_NOT_ACQUIRED, protocol.LockResult_LOST}
	released := []protocol.LockResult_Outcome{protocol.LockResult_RELEASED, protocol.LockResult_NOT_ACQUIRED}
	if failure := r.lock(ctx, a, "a1", lockStep{what: "the first session takes the lock", operation: provider.LockTry, want: acquired}); failure != "" {
		_ = r.closeSession(ctx, b.c, b.session.GetId())
		return []string{failure}
	}
	failures := collect(
		r.lock(ctx, a, "a1", lockStep{what: "the first session checks the lock it holds", operation: provider.LockCheck, want: acquired}),
		r.lock(ctx, b, "b1", lockStep{what: "a session of another process asks for the held lock", operation: provider.LockTry, want: []protocol.LockResult_Outcome{protocol.LockResult_BUSY}}),
		r.lock(ctx, a, "a1", lockStep{what: "the first session releases the lock", operation: provider.LockRelease, want: []protocol.LockResult_Outcome{protocol.LockResult_RELEASED}}),
		r.lock(ctx, a, "a1", lockStep{what: "the first session releases the lock again", operation: provider.LockRelease, want: released}),
		r.lock(ctx, a, "a1", lockStep{what: "the first session reuses a released lock ID", operation: provider.LockTry, want: notAcquired, refusable: true}),
		r.lock(ctx, b, "b1", lockStep{what: "the other session reuses the lock ID that found the lock busy", operation: provider.LockTry, want: notAcquired, refusable: true}),
		r.lock(ctx, b, "b2", lockStep{what: "the other session releases a lock ID it never used", operation: provider.LockRelease,
			want: []protocol.LockResult_Outcome{protocol.LockResult_NOT_ACQUIRED}}),
		r.lock(ctx, b, "b2", lockStep{what: "the other session takes the lock with the ID it fenced", operation: provider.LockTry, want: notAcquired, refusable: true}),
		r.lock(ctx, b, "b3", lockStep{what: "the other session takes the free lock", operation: provider.LockTry, want: acquired}),
	)
	if err := r.closeSession(ctx, b.c, b.session.GetId()); err != nil {
		return append(failures, "Close refused: "+errorText(err))
	}
	retaken := false
	for attempt := 0; !retaken && ctx.Err() == nil; attempt++ {
		lockID := "a-after-close-" + strconv.Itoa(attempt)
		retaken = r.lock(ctx, a, lockID, lockStep{what: "after the other session closed", operation: provider.LockTry, want: acquired}) == ""
		if retaken {
			failures = append(failures, collect(r.lock(ctx, a, lockID, lockStep{what: "the first session releases the lock it took again", operation: provider.LockRelease,
				want: []protocol.LockResult_Outcome{protocol.LockResult_RELEASED}}))...)
			break
		}
		if time.Duration(attempt+1)*releasePoll >= releaseWait {
			failures = append(failures, fmt.Sprintf("closing a session did not release its lock within %s", releaseWait))
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(releasePoll):
		}
	}
	return failures
}

// collect returns the failures that are not empty.
func collect(failures ...string) []string {
	var found []string
	for _, failure := range failures {
		if failure != "" {
			found = append(found, failure)
		}
	}
	return found
}
