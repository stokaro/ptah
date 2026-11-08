package conformance

import (
	"context"
	"fmt"
	"strconv"
	"time"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
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

// liveLocks takes one lock from two sessions: of two processes, or of one
// process under Live.InProcessLocks.
func (r *run) liveLocks(ctx context.Context) {
	if !provider.EngineOffers(r.full, r.engine, provider.LocksFeature) {
		r.add(checkLocks, NotApplicable, "the engine does not offer sessions.locks")
		return
	}
	inProcess := r.config.Live.InProcessLocks
	other, contender := r.all, "another session of the same process"
	if !inProcess {
		var err error
		if other, err = r.secondProcess(ctx); err != nil {
			r.add(checkLocks, Failed, "a second process: "+err.Error())
			return
		}
		contender = "a session of another process"
	}
	first, err := r.openSession(ctx, r.all, r.config.Live.Address)
	if err != nil {
		r.add(checkLocks, Failed, err.Error())
		return
	}
	defer func() { _ = r.closeSession(ctx, r.all, first.GetId()) }()
	if inProcess {
		if failure := r.secondProcessKeptOut(ctx); failure != "" {
			r.add(checkLocks, Failed, failure)
			return
		}
	}
	second, err := r.openSession(ctx, other, r.config.Live.Address)
	if err != nil {
		r.add(checkLocks, Failed, contender+": "+err.Error())
		return
	}
	a, b := &lockHolder{c: r.all, session: first}, &lockHolder{c: other, session: second}
	failures := r.lockSequence(ctx, a, b, contender)
	r.result(checkLocks, failures)
}

// secondProcess returns the second process the run started, or a new one.
func (r *run) secondProcess(ctx context.Context) (*conn, error) {
	if r.second != nil {
		return r.second, nil
	}
	return r.connect(ctx, listedAll())
}

// secondProcessKeptOut requires that a second process cannot open the
// database while the first holds a session of it, where each connection of
// the target is a process of its own. It returns what failed, or the empty
// string. An in-process target serves every connection from one server, so
// it has no second process to keep out.
func (r *run) secondProcessKeptOut(ctx context.Context) string {
	if r.config.Target.kind == "in-process" {
		return ""
	}
	other, err := r.secondProcess(ctx)
	if err != nil {
		return "a second process: " + err.Error()
	}
	session, err := r.openSession(ctx, other, r.config.Live.Address)
	if err != nil {
		return ""
	}
	_ = r.closeSession(ctx, other, session.GetId())
	return "a second process opened the database while a session of the first held it; Live.InProcessLocks is for an engine whose database one process holds at a time"
}

// lockSequence runs the lock steps with a as the first holder and b as the
// contender, which contender describes.
func (r *run) lockSequence(ctx context.Context, a, b *lockHolder, contender string) []string {
	acquired := []protocol.LockResult_Outcome{protocol.LockResult_ACQUIRED}
	notAcquired := []protocol.LockResult_Outcome{protocol.LockResult_BUSY, protocol.LockResult_RELEASED, protocol.LockResult_NOT_ACQUIRED, protocol.LockResult_LOST}
	released := []protocol.LockResult_Outcome{protocol.LockResult_RELEASED, protocol.LockResult_NOT_ACQUIRED}
	if failure := r.lock(ctx, a, "a1", lockStep{what: "the first session takes the lock", operation: provider.LockTry, want: acquired}); failure != "" {
		_ = r.closeSession(ctx, b.c, b.session.GetId())
		return []string{failure}
	}
	failures := collect(
		r.lock(ctx, a, "a1", lockStep{what: "the first session checks the lock it holds", operation: provider.LockCheck, want: acquired}),
		r.lock(ctx, b, "b1", lockStep{what: contender + " asks for the held lock", operation: provider.LockTry, want: []protocol.LockResult_Outcome{protocol.LockResult_BUSY}}),
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
