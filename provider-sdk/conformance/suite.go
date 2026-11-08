package conformance

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

const (
	suiteName          = "ptah-provider-conformance"
	defaultCallTimeout = 2 * time.Minute
)

// ErrConfig reports a configuration the suite cannot run with.
var ErrConfig = errors.New("invalid conformance configuration")

var namePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// Check runs the suite and returns its report. It returns an error only for
// a configuration it cannot run with, which wraps ErrConfig; a provider that
// fails a check, or does not start at all, is a report with failures. ctx
// bounds the whole run.
func Check(ctx context.Context, config Config) (*Report, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	r := &run{config: config, timeout: config.CallTimeout, sensitive: sensitiveTexts(config)}
	if r.timeout == 0 {
		r.timeout = defaultCallTimeout
	}
	r.report = &Report{Suite: suiteName, Contract: provider.ContractString(provider.Contract()), Target: config.Target.kind}
	defer r.closeAll()
	r.checkAll(ctx)
	return r.report, nil
}

// Run runs the suite in a test and reports each check that applies as a
// subtest of t, which fails when the check failed. A check that does not
// apply is logged, not skipped, so that a test runner that refuses skipped
// tests still runs the suite. A configuration the suite cannot run with
// fails t at once.
func Run(t *testing.T, config Config) {
	t.Helper()
	report, err := Check(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range report.Results {
		if result.Status == NotApplicable {
			t.Logf("%s does not apply: %s", result.ID, result.Detail)
			continue
		}
		t.Run(result.ID, func(t *testing.T) {
			if result.Status == Failed {
				t.Error(result.Detail)
			}
		})
	}
}

func validateConfig(config Config) error {
	if config.Target.connect == nil {
		return fmt.Errorf("%w: a target is required", ErrConfig)
	}
	if config.ProviderID == "" {
		return fmt.Errorf("%w: the expected provider identity is required", ErrConfig)
	}
	if config.CallTimeout < 0 {
		return fmt.Errorf("%w: the call timeout is negative", ErrConfig)
	}
	names := make(map[string]bool)
	for _, call := range config.Calls {
		if err := validName(names, "call", call.Name); err != nil {
			return err
		}
		if call.Request == nil || offlineRPCFor(call.Request) == nil {
			return fmt.Errorf("%w: call %s does not send a request of an offline service", ErrConfig, call.Name)
		}
	}
	live := config.Live
	if live == nil {
		return nil
	}
	if live.Address == "" {
		return fmt.Errorf("%w: the live part needs an address", ErrConfig)
	}
	names = make(map[string]bool)
	for _, change := range live.Changes {
		if err := validName(names, "change", change.Name); err != nil {
			return err
		}
	}
	names = make(map[string]bool)
	for _, call := range live.Calls {
		if err := validName(names, "live call", call.Name); err != nil {
			return err
		}
		if call.Request == nil {
			return fmt.Errorf("%w: live call %s builds no request", ErrConfig, call.Name)
		}
	}
	return nil
}

func validName(seen map[string]bool, what, name string) error {
	if !namePattern.MatchString(name) || seen[name] {
		return fmt.Errorf("%w: the %s name %q is empty, repeated, or not letters, digits, dots, dashes and underscores", ErrConfig, what, name)
	}
	seen[name] = true
	return nil
}

// run is one execution of the suite.
type run struct {
	config    Config
	timeout   time.Duration
	sensitive []string
	report    *Report
	// full is the provider's description under a host that lists every
	// two-way feature, as the provider sent it.
	full *protocol.Description
	// engine is the engine the credential and live checks use, or empty
	// when the configuration does not select one the provider describes.
	engine string
	// all, second and none are connections the offline checks share: two
	// under a host that lists every two-way feature, one under a host that
	// lists none.
	all, second, none *conn

	mu        sync.Mutex
	opened    []*conn
	reasons   []string
	redaction []string
	// noted are failures that hosts listing every two-way feature but one
	// found, by the check that reports them.
	noted map[string][]string
	// reading serializes the reads of those hosts.
	reading sync.Mutex
}

// profile is what a host announces in Describe.
type profile struct {
	name     string
	features []string
}

func listedAll() profile {
	return profile{name: "a host that lists every two-way feature", features: provider.TwoWayFeatures()}
}

func listedNone() profile {
	return profile{name: "a host that lists no two-way feature"}
}

func (p profile) request(required []string) *protocol.DescribeRequest {
	return &protocol.DescribeRequest{Revision: provider.Revision, RequiredFeatures: slices.Clone(required),
		HostContract: provider.Contract(), HostFeatures: slices.Clone(p.features)}
}

// conn is a link to the provider after a Describe under one profile.
type conn struct {
	*link
	profile profile
	// reply is the description as the provider sent it.
	reply *protocol.Description
	// description is the description both sides validate with: the reply
	// without the two-way features the host did not list.
	description *protocol.Description
}

// reasons reports whether the connection negotiated errors.reasons.
func (c *conn) reasons() bool {
	return c != nil && slices.Contains(c.description.GetFeatures(), provider.ErrorReasonsFeature)
}

func (r *run) add(id string, outcome Status, detail string) {
	r.report.Results = append(r.report.Results, Result{ID: id, Status: outcome, Detail: r.redact(detail)})
}

func (r *run) pass(id string) { r.add(id, Passed, "") }

// result adds id as passed when failures is empty, and as failed with the
// failures otherwise.
func (r *run) result(id string, failures []string) {
	if len(failures) == 0 {
		r.pass(id)
		return
	}
	r.add(id, Failed, joinFailures(failures))
}

// connect starts a link and describes the provider under p. The connection
// is closed when the run ends.
func (r *run) connect(ctx context.Context, p profile) (*conn, error) {
	opened, err := r.config.Target.connect(ctx)
	if err != nil {
		return nil, err
	}
	c := &conn{link: opened, profile: p}
	r.track(c)
	reply, err := r.describe(ctx, opened, p.request(r.config.RequiredFeatures))
	if err != nil {
		return nil, fmt.Errorf("%w under %s: %s", errDescribeRefused, p.name, errorText(err))
	}
	c.reply = reply
	c.description = provider.NegotiatedDescription(reply, p.features)
	return c, nil
}

// errDescribeRefused marks a provider that started and refused the
// Describe of a profile, as opposed to one that did not start.
var errDescribeRefused = errors.New("Describe refused")

// narrowsValidly reports whether the full description, without the two-way
// features a host that lists features did not list, is one the SDK accepts.
// When it is not, a feature the host still lists needs one it left out, and
// the provider has to refuse that host rather than describe itself: the SDK's
// server does so. A host that lists no two-way feature is the narrowest such
// host, and is held to the same rule as one that leaves out one feature.
func narrowsValidly(full *protocol.Description, required, features []string) bool {
	return provider.ValidateDescription(provider.NegotiatedDescription(full, features), required) == nil
}

func (r *run) track(c *conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.opened = append(r.opened, c)
}

func (r *run) closeAll() {
	r.mu.Lock()
	opened := r.opened
	r.opened = nil
	r.mu.Unlock()
	for _, c := range opened {
		c.close()
	}
}

// describe sends one Describe on a link that negotiated nothing yet, so a
// refusal it receives carries no reason.
func (r *run) describe(ctx context.Context, opened *link, request *protocol.DescribeRequest) (*protocol.Description, error) {
	bounded, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	reply, err := opened.client.Describe(bounded, request)
	if err != nil {
		r.observeError(nil, "Describe", err)
		return nil, err
	}
	return reply, nil
}

// invoke makes one call on c within the suite's bound, observes a refusal,
// and resolves the model references and shared coverage of a reply as the
// SDK's client does.
func invoke[Q, R proto.Message](ctx context.Context, r *run, c *conn, method string, call func(context.Context, Q, ...grpc.CallOption) (R, error), request Q) (R, error) {
	var zero R
	bounded, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	reply, err := call(bounded, request)
	if err != nil {
		r.observeError(c, method, err)
		return zero, err
	}
	if err := resolveReply(c.description, reply); err != nil {
		return zero, fmt.Errorf("the reply of %s does not resolve: %w", method, err)
	}
	return reply, nil
}

// resolveReply resolves what the transport shares in a reply: model
// references where the description lists models.references, then shared
// coverage lists where it lists transport.shared-coverage.
func resolveReply(description *protocol.Description, reply proto.Message) error {
	if slices.Contains(description.GetFeatures(), provider.ModelReferencesFeature) {
		if err := provider.ResolveModelReferences(description, reply); err != nil {
			return err
		}
	}
	if slices.Contains(description.GetFeatures(), provider.SharedCoverageFeature) {
		return provider.ResolveSharedCoverage(reply)
	}
	return nil
}

// observeError records what a refusal breaks: a reason the connection did
// not negotiate, a malformed reason or more than one, and a reason that
// quotes a sensitive text. c is nil for a call before negotiation.
func (r *run) observeError(c *conn, method string, err error) {
	reported, isStatus := status.FromError(err)
	if !isStatus {
		return
	}
	var found []*protocol.ProviderDiagnostic
	for _, detail := range reported.Proto().GetDetails() {
		if !detail.MessageIs(&protocol.ProviderDiagnostic{}) {
			continue
		}
		diagnostic := &protocol.ProviderDiagnostic{}
		if detail.UnmarshalTo(diagnostic) != nil {
			diagnostic = &protocol.ProviderDiagnostic{}
		}
		found = append(found, diagnostic)
	}
	where := method
	if c != nil {
		where = method + " under " + c.profile.name
	}
	switch {
	case len(found) == 0:
	case !c.reasons():
		r.violate(&r.reasons, fmt.Sprintf("the refusal of %s carries a reason the host did not negotiate", where))
	case len(found) > 1:
		r.violate(&r.reasons, fmt.Sprintf("the refusal of %s carries %d reasons", where, len(found)))
	case provider.ValidateDiagnostic(found[0]) != nil:
		r.violate(&r.reasons, fmt.Sprintf("the refusal of %s carries a reason of an invalid shape", where))
	}
	for _, diagnostic := range found {
		r.observeText("the reason of the refusal of "+where, diagnostic.GetReason())
	}
}

// observeResultReason records a result's diagnostic the connection did not
// negotiate, and one that quotes a sensitive text.
func (r *run) observeResultReason(c *conn, method string, diagnostic *protocol.ProviderDiagnostic) {
	if diagnostic == nil {
		return
	}
	where := method + " under " + c.profile.name
	if !c.reasons() {
		r.violate(&r.reasons, fmt.Sprintf("the result of %s carries a reason the host did not negotiate", where))
	} else if provider.ValidateDiagnostic(diagnostic) != nil {
		r.violate(&r.reasons, fmt.Sprintf("the result of %s carries a reason of an invalid shape", where))
	}
	r.observeText("the reason of the result of "+where, diagnostic.GetReason())
}

// observeText records text the provider sent that quotes a sensitive text.
func (r *run) observeText(what, text string) {
	for _, secret := range r.sensitive {
		if containsFold(text, secret) {
			r.violate(&r.redaction, what+" quotes the live address, its credential or the canary")
			return
		}
	}
}

// violate records a violation. A check that reports violations found while
// it ran counts them by position, so a repeated one is recorded again;
// unique removes the repeats from what the report shows.
func (r *run) violate(list *[]string, violation string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*list = append(*list, violation)
}

// unique returns list without repeats, in the order each first appears.
func unique(list []string) []string {
	var kept []string
	for _, entry := range list {
		if !slices.Contains(kept, entry) {
			kept = append(kept, entry)
		}
	}
	return kept
}

// checkAll runs every check in report order.
func (r *run) checkAll(ctx context.Context) {
	if !r.bootstrap(ctx) {
		for _, check := range Checks()[1:] {
			r.add(check.ID, NotApplicable, "the provider did not start")
		}
		return
	}
	r.handshake(ctx)
	r.descriptionChecks(ctx)
	r.services(ctx)
	r.unknownSessions(ctx)
	r.contextCredential(ctx)
	r.models()
	r.calls(ctx)
	r.live(ctx)
	r.mu.Lock()
	reasons, redaction := unique(r.reasons), unique(r.redaction)
	r.mu.Unlock()
	r.result(checkReasons, reasons)
	r.result(checkRedacted, redaction)
}

// bootstrap starts the provider under a host that lists every two-way
// feature and keeps the connection for the checks that follow.
func (r *run) bootstrap(ctx context.Context) bool {
	c, err := r.connect(ctx, listedAll())
	if err != nil {
		r.add(checkBootstrap, Failed, err.Error())
		return false
	}
	r.all, r.full = c, c.reply
	engines := make([]string, 0, len(c.reply.GetEngines()))
	for _, engine := range c.reply.GetEngines() {
		engines = append(engines, engine.GetId())
	}
	r.report.Provider = &Identity{ID: c.reply.GetProviderId(), Version: c.reply.GetVersion(), SemanticIdentity: c.reply.GetSemanticIdentity(),
		MinContract: provider.ContractString(provider.MinimumContract(c.reply)), Engines: engines, Features: slices.Clone(c.reply.GetFeatures())}
	switch {
	case r.config.Engine != "" && slices.Contains(engines, r.config.Engine):
		r.engine = r.config.Engine
	case r.config.Engine == "" && len(engines) == 1:
		r.engine = engines[0]
	}
	r.pass(checkBootstrap)
	return true
}

// deliberate reports an error the provider meant. The SDK's server answers
// a reply its validator refuses, such as one that uses an addition the host
// did not list, with a status whose message is the text of
// provider.ErrProtocol, and a provider answers a defect of its own with
// Internal; neither is a refusal, and neither is a reply the suite could not
// resolve.
func deliberate(err error) bool {
	var carrier interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &carrier) {
		return false
	}
	reported := carrier.GRPCStatus()
	return reported.Code() != codes.Internal && !strings.HasPrefix(reported.Message(), provider.ErrProtocol.Error())
}

// errorText is what the suite reports of a failed call: the gRPC code and
// the status message, or the error itself for a failure the suite found.
// The run redacts it before it reaches the report.
func errorText(err error) string {
	if reported, isStatus := status.FromError(err); isStatus {
		return fmt.Sprintf("%s: %s", reported.Code(), reported.Message())
	}
	return err.Error()
}
