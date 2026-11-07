package conformance

import (
	"context"
	"fmt"
	"maps"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// units returns what a step runs: the units Render returns for its request,
// or its units as they are.
func (r *run) units(ctx context.Context, c *conn, session *protocol.Session, step Step) ([]*protocol.Unit, error) {
	if step.Render == nil {
		return cloneUnits(step.Units), nil
	}
	request := proto.CloneOf(step.Render)
	if request.GetEngine() == "" {
		request.Engine = r.engine
	}
	if request.Capabilities == nil {
		request.Capabilities = maps.Clone(session.GetCapabilities())
	}
	if err := provider.ValidateRenderRequest(c.description, request); err != nil {
		return nil, fmt.Errorf("the render request is not one the SDK's client sends: %w", err)
	}
	rendered, err := invoke(ctx, r, c, "Render", c.client.Render, request)
	if err != nil {
		return nil, callFailed("Render refused", err)
	}
	if err := provider.ValidateRenderedBatchReply(c.description, request, rendered); err != nil {
		return nil, fmt.Errorf("the rendered batch fails the SDK's validator: %w", err)
	}
	return rendered.GetUnits(), nil
}

// execute runs units on a session and checks the result as the SDK's client
// does. It returns the result whatever its outcome, and an error that says
// why the outcome is not a confirmed application of every unit.
func (r *run) execute(ctx context.Context, c *conn, session *protocol.Session, requestID string, units []*protocol.Unit) (*protocol.Execution, error) {
	request := r.executeRequest(c, session.GetId(), requestID, units)
	if err := provider.ValidateExecuteRequest(c.description, request); err != nil {
		return nil, fmt.Errorf("the execution request is not one the SDK's client sends: %w", err)
	}
	result, err := invoke(ctx, r, c, "Execute", c.client.Execute, request)
	if err != nil {
		return nil, callFailed("Execute refused", err)
	}
	r.observeResultReason(c, "Execute", result.GetDiagnostic())
	r.observeText("the message of an execution result", result.GetMessage())
	if err := provider.ValidateExecutionAbort(c.description, request, result); err != nil {
		return nil, fmt.Errorf("the execution result fails the SDK's validator: %w", err)
	}
	return result, provider.CheckExecuted(request, result)
}

func (r *run) liveChanges(ctx context.Context) {
	if len(r.config.Live.Changes) == 0 {
		r.add(checkChanges, NotApplicable, "the configuration has no change to apply")
		return
	}
	for _, change := range r.config.Live.Changes {
		r.result(checkChanges+"."+change.Name, r.applyChange(ctx, change))
	}
}

// applyChange applies a change and reverts it on one session, reading the
// database before, between and after.
func (r *run) applyChange(ctx context.Context, change Change) []string {
	c := r.all
	session, err := r.openSession(ctx, c, r.config.Live.Address)
	if err != nil {
		return []string{err.Error()}
	}
	defer func() { _ = r.closeSession(ctx, c, session.GetId()) }()
	before, err := r.inspect(ctx, c, session)
	if err != nil {
		return []string{"before the change: " + err.Error()}
	}
	var failures []string
	apply, err := r.units(ctx, c, session, change.Apply)
	if err != nil {
		return []string{"apply: " + err.Error()}
	}
	if _, err := r.execute(ctx, c, session, "ptah-conformance-apply-"+change.Name, apply); err != nil {
		failures = append(failures, "apply: "+err.Error())
	}
	if applied, err := r.inspect(ctx, c, session); err != nil {
		failures = append(failures, "after the change: "+err.Error())
	} else if proto.Equal(applied, before) {
		failures = append(failures, "the database read the same after the change as before it")
	}
	revert, err := r.units(ctx, c, session, change.Revert)
	if err != nil {
		return append(failures, "revert: "+err.Error())
	}
	if _, err := r.execute(ctx, c, session, "ptah-conformance-revert-"+change.Name, revert); err != nil {
		failures = append(failures, "revert: "+err.Error())
	}
	if after, err := r.inspect(ctx, c, session); err != nil {
		failures = append(failures, "after the revert: "+err.Error())
	} else if !proto.Equal(after, before) {
		failures = append(failures, "the database read differently after the revert than before the change")
	}
	return failures
}

func (r *run) liveRefused(ctx context.Context) {
	if len(r.config.Live.Refused) == 0 {
		r.add(checkRefused, NotApplicable, "the configuration has no refused unit")
		return
	}
	c := r.all
	session, err := r.openSession(ctx, c, r.config.Live.Address)
	if err != nil {
		r.add(checkRefused, Failed, err.Error())
		return
	}
	defer func() { _ = r.closeSession(ctx, c, session.GetId()) }()
	var failures []string
	for i, unit := range r.config.Live.Refused {
		result, _ := r.execute(ctx, c, session, fmt.Sprintf("ptah-conformance-refused-%d", i), []*protocol.Unit{unit})
		switch {
		case result == nil:
			failures = append(failures, fmt.Sprintf("refused unit %d has no confirmed outcome", i))
		case result.GetOutcome() != protocol.Execution_NOT_APPLIED || result.GetAppliedCount() != 0:
			failures = append(failures, fmt.Sprintf("refused unit %d answered %s with %d units applied", i, result.GetOutcome(), result.GetAppliedCount()))
		}
	}
	if len(r.config.Live.Changes) > 0 {
		failures = append(failures, r.refusedPrefix(ctx, c, session)...)
	}
	r.result(checkRefused, failures)
}

// refusedPrefix runs the units of the first change followed by the first
// refused unit, and requires the confirmed prefix to be what the database
// holds afterwards.
func (r *run) refusedPrefix(ctx context.Context, c *conn, session *protocol.Session) []string {
	change := r.config.Live.Changes[0]
	before, err := r.inspect(ctx, c, session)
	if err != nil {
		return []string{"before the prefix: " + err.Error()}
	}
	apply, err := r.units(ctx, c, session, change.Apply)
	if err != nil {
		return []string{"the prefix: " + err.Error()}
	}
	units := append(slices.Clone(apply), r.config.Live.Refused[0])
	result, _ := r.execute(ctx, c, session, "ptah-conformance-refused-prefix", units)
	after, err := r.inspect(ctx, c, session)
	if err != nil {
		return []string{"after the prefix: " + err.Error()}
	}
	switch {
	case result == nil:
		return []string{"the prefix and a refused unit have no confirmed outcome"}
	case result.GetOutcome() == protocol.Execution_NOT_APPLIED && result.GetAppliedCount() == 0:
		if !proto.Equal(after, before) {
			return []string{"NOT_APPLIED left the database changed"}
		}
		return nil
	case result.GetOutcome() != protocol.Execution_PARTIAL || int(result.GetAppliedCount()) != len(apply):
		return []string{fmt.Sprintf("the prefix and a refused unit answered %s with %d of %d units applied", result.GetOutcome(), result.GetAppliedCount(), len(units))}
	}
	var failures []string
	if proto.Equal(after, before) {
		failures = append(failures, "PARTIAL confirmed the prefix, and the database does not hold it")
	}
	revert, err := r.units(ctx, c, session, change.Revert)
	if err == nil {
		_, err = r.execute(ctx, c, session, "ptah-conformance-refused-revert", revert)
	}
	if err != nil {
		failures = append(failures, "reverting the confirmed prefix: "+err.Error())
	}
	return failures
}

// liveRPCFor returns the session-bound read a request addresses.
func liveRPCFor(request proto.Message) *rpc { return rpcFor(liveRPCs(), request) }

func (r *run) liveCalls(ctx context.Context) {
	if len(r.config.Live.Calls) == 0 {
		r.add(checkLiveCalls, NotApplicable, "the configuration has no session-bound call")
		return
	}
	for _, call := range r.config.Live.Calls {
		r.result(checkLiveCalls+"."+call.Name, r.liveCall(ctx, call))
	}
}

func (r *run) liveCall(ctx context.Context, call LiveCall) []string {
	c := r.all
	session, err := r.openSession(ctx, c, r.config.Live.Address)
	if err != nil {
		return []string{err.Error()}
	}
	defer func() { _ = r.closeSession(ctx, c, session.GetId()) }()
	request := call.Request(proto.CloneOf(session))
	entry := liveRPCFor(request)
	switch {
	case entry == nil:
		return []string{"the request is not one of the session-bound reads the suite sends"}
	case !slices.Contains(c.description.GetFeatures(), entry.feature):
		return []string{fmt.Sprintf("the call needs %s, which the description does not offer", entry.feature)}
	}
	if err := entry.validRequest(c.description, request); err != nil {
		return []string{"the request is not one the SDK's client sends: " + err.Error()}
	}
	reply, err := entry.call(ctx, r, c, proto.CloneOf(request))
	switch {
	case call.Refused && err == nil:
		return []string{entry.method + " answered a request it must refuse"}
	case call.Refused && !deliberate(err):
		return []string{entry.method + " failed where it must refuse: " + errorText(err)}
	case call.Refused:
		return nil
	case err != nil:
		return []string{entry.method + " refused the request: " + errorText(err)}
	}
	if err := entry.validReply(c.description, request, reply); err != nil {
		return []string{"the reply fails the SDK's validator: " + err.Error()}
	}
	return nil
}
