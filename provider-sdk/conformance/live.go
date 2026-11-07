package conformance

import (
	"context"
	"errors"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// errNoInspect reports a check that needs a read of a provider that offers
// none.
var errNoInspect = errors.New("the provider does not offer schema.inspect")

// live runs the checks that need a database.
func (r *run) live(ctx context.Context) {
	if r.config.Live == nil {
		for _, check := range Checks() {
			if check.Live {
				r.add(check.ID, NotApplicable, "the configuration has no live database")
			}
		}
		return
	}
	if r.engine == "" {
		for _, check := range Checks() {
			if check.Live {
				r.add(check.ID, Failed, "the configuration names no engine the provider describes; set Config.Engine")
			}
		}
		return
	}
	r.liveOpen(ctx)
	r.liveClose(ctx)
	r.liveSessions(ctx)
	r.liveRestart(ctx)
	r.liveInspect(ctx)
	r.liveLocks(ctx)
	r.liveTransactions(ctx)
	r.openCredential(ctx)
	r.liveChanges(ctx)
	r.liveRefused(ctx)
	r.liveCalls(ctx)
}

// openSession opens address on c as the SDK's client does: under
// database.context it resolves the address first and requires the session to
// carry that context.
func (r *run) openSession(ctx context.Context, c *conn, address string) (*protocol.Session, error) {
	var expected *protocol.DatabaseContext
	if provider.EngineOffers(c.description, r.engine, provider.DatabaseContextFeature) {
		request := &protocol.DatabaseContextRequest{Engine: r.engine, Address: address}
		resolved, err := invoke(ctx, r, c, "ResolveDatabaseContext", c.client.ResolveDatabaseContext, request)
		if err != nil {
			return nil, callFailed("ResolveDatabaseContext refused the address", err)
		}
		r.observeText("the database context", resolved.GetDatabase()+"\n"+resolved.GetSourceDatabase())
		if err := provider.ValidateResolvedDatabaseContextReply(c.description, request, resolved); err != nil {
			return nil, fmt.Errorf("the resolved database context fails the SDK's validator: %w", err)
		}
		expected = resolved
	}
	session, err := invoke(ctx, r, c, "Open", c.client.Open, &protocol.OpenRequest{Engine: r.engine, Address: address})
	if err != nil {
		return nil, callFailed("Open refused the address", err)
	}
	r.observeText("a session field", session.GetId()+"\n"+session.GetDatabase()+"\n"+session.GetDefaultSchema()+"\n"+session.GetServerVersion()+"\n"+
		session.GetContext().GetDatabase()+"\n"+session.GetContext().GetSourceDatabase())
	if err := provider.ValidateSessionContext(c.description, r.engine, expected, session); err != nil {
		r.closeSession(ctx, c, session.GetId())
		return nil, fmt.Errorf("the session fails the SDK's validator: %w", err)
	}
	return session, nil
}

// closeSession closes a session and returns the refusal, if any.
func (r *run) closeSession(ctx context.Context, c *conn, id string) error {
	_, err := invoke(ctx, r, c, "Close", c.client.Close, &protocol.CloseRequest{SessionId: id})
	return err
}

// inspect reads the database on a session within the configured scope and
// checks the snapshot as the SDK's client does.
func (r *run) inspect(ctx context.Context, c *conn, session *protocol.Session) (*protocol.Schema, error) {
	if !slices.Contains(c.description.GetFeatures(), provider.InspectFeature) {
		return nil, errNoInspect
	}
	request := &protocol.InspectRequest{SessionId: session.GetId(), Schemas: append([]string(nil), r.config.Live.Schemas...)}
	snapshot, err := invoke(ctx, r, c, "Inspect", c.client.Inspect, request)
	if err != nil {
		return nil, callFailed("Inspect refused", err)
	}
	if _, err := provider.InspectionValues(c.description, request, snapshot); err != nil {
		return nil, fmt.Errorf("the snapshot fails the SDK's validator: %w", err)
	}
	return snapshot, nil
}

func (r *run) liveOpen(ctx context.Context) {
	var failures []string
	for _, c := range []*conn{r.all, r.none} {
		if c == nil {
			continue
		}
		session, err := r.openSession(ctx, c, r.config.Live.Address)
		if err != nil {
			if !narrowedRefusal(c, err) {
				failures = append(failures, "under "+c.profile.name+": "+err.Error())
			}
			continue
		}
		if err := r.closeSession(ctx, c, session.GetId()); err != nil {
			failures = append(failures, "under "+c.profile.name+", Close refused: "+errorText(err))
		}
	}
	r.result(checkOpen, failures)
}

func (r *run) liveClose(ctx context.Context) {
	c := r.all
	session, err := r.openSession(ctx, c, r.config.Live.Address)
	if err != nil {
		r.add(checkClose, Failed, err.Error())
		return
	}
	var failures []string
	if err := r.closeSession(ctx, c, session.GetId()); err != nil {
		failures = append(failures, "Close refused: "+errorText(err))
	}
	if err := r.closeSession(ctx, c, session.GetId()); err != nil {
		failures = append(failures, "closing the session again failed: "+errorText(err))
	}
	if failure := r.usable(ctx, c, session); failure == "" {
		failures = append(failures, "the provider still serves the session it closed")
	}
	r.result(checkClose, failures)
}

// usable reports what a session refused, or the empty string when the
// session answered: a read where the provider offers one, and otherwise a
// unit that changes nothing.
func (r *run) usable(ctx context.Context, c *conn, session *protocol.Session) string {
	if _, err := r.inspect(ctx, c, session); !errors.Is(err, errNoInspect) {
		if err != nil {
			return err.Error()
		}
		return ""
	}
	request := &protocol.LockRequest{Scope: &protocol.LockScope{SessionId: session.GetId(), LockId: "ptah-conformance-usable", Name: r.lockName()}, RequestId: "ptah-conformance-usable"}
	result, err := invoke(ctx, r, c, "CheckLock", c.client.CheckLock, request)
	if err != nil {
		return errorText(err)
	}
	return "CheckLock answered " + result.GetOutcome().String()
}

func (r *run) liveSessions(ctx context.Context) {
	c := r.all
	first, err := r.openSession(ctx, c, r.config.Live.Address)
	if err != nil {
		r.add(checkSessions, Failed, err.Error())
		return
	}
	second, err := r.openSession(ctx, c, r.config.Live.Address)
	if err != nil {
		_ = r.closeSession(ctx, c, first.GetId())
		r.add(checkSessions, Failed, "a second session: "+err.Error())
		return
	}
	var failures []string
	if first.GetId() == second.GetId() {
		failures = append(failures, "two sessions have the same handle")
	}
	if err := r.closeSession(ctx, c, first.GetId()); err != nil {
		failures = append(failures, "Close refused: "+errorText(err))
	}
	if _, err := r.inspect(ctx, c, second); err != nil && !errors.Is(err, errNoInspect) {
		failures = append(failures, "after the first session closed, the second "+err.Error())
	}
	if err := r.closeSession(ctx, c, second.GetId()); err != nil {
		failures = append(failures, "Close refused: "+errorText(err))
	}
	r.result(checkSessions, failures)
}

// liveRestart opens a session in a process of its own, ends the process, and
// sends the handle to a new process.
func (r *run) liveRestart(ctx context.Context) {
	if r.all.exited == nil {
		r.add(checkRestart, NotApplicable, "only an executable target has a process to restart")
		return
	}
	first, err := r.connect(ctx, listedAll())
	if err != nil {
		r.add(checkRestart, Failed, err.Error())
		return
	}
	session, err := r.openSession(ctx, first, r.config.Live.Address)
	if err != nil {
		r.add(checkRestart, Failed, err.Error())
		return
	}
	first.close()
	fresh, err := r.connect(ctx, listedAll())
	if err != nil {
		r.add(checkRestart, Failed, "a new process: "+err.Error())
		return
	}
	if failure := r.usable(ctx, fresh, session); failure == "" {
		r.add(checkRestart, Failed, "a new process serves the session of a process that exited")
		return
	}
	r.pass(checkRestart)
}

func (r *run) liveInspect(ctx context.Context) {
	if !slices.Contains(r.full.GetFeatures(), provider.InspectFeature) {
		r.add(checkInspect, NotApplicable, "the provider does not offer schema.inspect")
		return
	}
	var failures []string
	for _, c := range []*conn{r.all, r.none} {
		if c == nil {
			continue
		}
		failures = append(failures, r.inspectTwice(ctx, c)...)
	}
	r.result(checkInspect, append(failures, r.notedFor(checkInspect)...))
}

func (r *run) inspectTwice(ctx context.Context, c *conn) []string {
	under := "under " + c.profile.name + ": "
	session, err := r.openSession(ctx, c, r.config.Live.Address)
	if err != nil {
		return narrowedFailure(c, under, err)
	}
	defer func() { _ = r.closeSession(ctx, c, session.GetId()) }()
	first, err := r.inspect(ctx, c, session)
	if err != nil {
		return narrowedFailure(c, under, err)
	}
	second, err := r.inspect(ctx, c, session)
	switch {
	case err != nil:
		return []string{under + "a second read: " + err.Error()}
	case !proto.Equal(first, second):
		return []string{under + "two reads of a database nothing changed returned different snapshots"}
	}
	return nil
}

// callError is a call the provider refused, and what the suite was doing.
type callError struct {
	what string
	err  error
}

func callFailed(what string, err error) error { return &callError{what: what, err: err} }

func (e *callError) Error() string { return e.what + ": " + errorText(e.err) }

func (e *callError) Unwrap() error { return e.err }

// narrowedRefusal reports a refusal the suite accepts: one the provider
// meant, under a host that does not list every two-way feature. A provider
// may refuse a service it cannot serve without an addition the host did not
// list, as long as it does not use the addition; and Internal is never a
// refusal it meant, see deliberate.
func narrowedRefusal(c *conn, err error) bool {
	return len(c.profile.features) < len(provider.TwoWayFeatures()) && deliberate(err)
}

// narrowedFailure is the failure of err, or none for a narrowed refusal.
func narrowedFailure(c *conn, under string, err error) []string {
	if narrowedRefusal(c, err) {
		return nil
	}
	return []string{under + err.Error()}
}

// openCredential opens the credential address. Whether the provider opens
// it or refuses, nothing it sends back may quote the canary.
func (r *run) openCredential(ctx context.Context) {
	if r.config.CredentialAddress == "" {
		r.add(checkOpenCredential, NotApplicable, "the configuration has no credential address")
		return
	}
	before := r.redactionCount()
	if session, err := r.openSession(ctx, r.all, r.config.CredentialAddress); err == nil {
		_ = r.closeSession(ctx, r.all, session.GetId())
	}
	r.redactedSince(checkOpenCredential, before)
}

// contextCredential resolves the database context of the credential
// address offline.
func (r *run) contextCredential(ctx context.Context) {
	switch {
	case r.config.CredentialAddress == "":
		r.add(checkContextCredential, NotApplicable, "the configuration has no credential address")
		return
	case r.engine == "":
		r.add(checkContextCredential, Failed, "the configuration names no engine the provider describes; set Config.Engine")
		return
	case !provider.EngineOffers(r.all.description, r.engine, provider.DatabaseContextFeature):
		r.add(checkContextCredential, NotApplicable, "the engine does not offer database.context")
		return
	}
	before := r.redactionCount()
	resolved, err := invoke(ctx, r, r.all, "ResolveDatabaseContext", r.all.client.ResolveDatabaseContext,
		&protocol.DatabaseContextRequest{Engine: r.engine, Address: r.config.CredentialAddress})
	if err == nil {
		r.observeText("the database context of the credential address", resolved.GetDatabase()+"\n"+resolved.GetSourceDatabase())
	}
	r.redactedSince(checkContextCredential, before)
}

func (r *run) redactionCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.redaction)
}

// redactedSince adds id as failed with the redaction violations found since
// before, and as passed when there are none.
func (r *run) redactedSince(id string, before int) {
	r.mu.Lock()
	found := unique(r.redaction[before:])
	r.mu.Unlock()
	r.result(id, found)
}
