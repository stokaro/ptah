package conformance

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// unofferedFeature is a feature name no provider offers: the suite requires
// it to see a provider refuse what it cannot do, and lists it as a host
// feature to see a provider ignore what it does not know.
const unofferedFeature = "ptah.conformance.unoffered-feature"

// handshake runs the checks of what the provider does with the host's
// Describe. Each sends one Describe to a provider started for it, because a
// host that is refused stops the provider.
func (r *run) handshake(ctx context.Context) {
	required := r.config.RequiredFeatures
	r.refusedDescribe(ctx, checkOtherMajor, &protocol.DescribeRequest{Revision: provider.Revision + 1, RequiredFeatures: slices.Clone(required),
		HostContract: &protocol.ContractVersion{Major: provider.Revision + 1}})
	r.refusedDescribe(ctx, checkContractMajor, &protocol.DescribeRequest{Revision: provider.Revision, RequiredFeatures: slices.Clone(required),
		HostContract: &protocol.ContractVersion{Major: provider.Revision + 1, Minor: provider.ContractMinor}})
	r.hostFeatures(ctx)
	r.refusedDescribe(ctx, checkRequiredFeatures, &protocol.DescribeRequest{Revision: provider.Revision, RequiredFeatures: append(slices.Clone(required), unofferedFeature),
		HostContract: provider.Contract()})
	r.firstMinorHost(ctx)
	if err := provider.CheckContract(provider.Contract(), provider.MinimumContract(r.full)); err != nil {
		r.add(checkMinimumContract, Failed, err.Error())
	} else {
		r.pass(checkMinimumContract)
	}
}

// errNotStarted reports a provider that did not start for a check, which is
// no refusal of what the check sends.
var errNotStarted = errors.New("the provider did not start")

// describeOnce starts the provider, sends request, and stops the provider.
func (r *run) describeOnce(ctx context.Context, request *protocol.DescribeRequest) (*protocol.Description, error) {
	opened, err := r.config.Target.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errNotStarted, err)
	}
	defer opened.close()
	return r.describe(ctx, opened, request)
}

func (r *run) refusedDescribe(ctx context.Context, id string, request *protocol.DescribeRequest) {
	reply, err := r.describeOnce(ctx, request)
	switch {
	case errors.Is(err, errNotStarted):
		r.add(id, Failed, err.Error())
	case err == nil:
		r.add(id, Failed, fmt.Sprintf("Describe answered with the description of %s", reply.GetProviderId()))
	default:
		r.pass(id)
	}
}

// hostFeatures sends a repeated feature, a malformed one and an empty one,
// each of which the provider refuses, and a feature of a later contract,
// which it ignores: a newer host lists features this provider does not know.
func (r *run) hostFeatures(ctx context.Context) {
	var failures []string
	for _, listed := range [][]string{
		{provider.ErrorReasonsFeature, provider.ErrorReasonsFeature},
		{"errors reasons"},
		{""},
	} {
		request := listedAll().request(r.config.RequiredFeatures)
		request.HostFeatures = listed
		switch _, err := r.describeOnce(ctx, request); {
		case errors.Is(err, errNotStarted):
			failures = append(failures, err.Error())
		case err == nil:
			failures = append(failures, fmt.Sprintf("Describe accepted the host features %q", listed))
		}
	}
	request := listedAll().request(r.config.RequiredFeatures)
	request.HostFeatures = append(request.HostFeatures, unofferedFeature)
	reply, err := r.describeOnce(ctx, request)
	switch {
	case err != nil:
		failures = append(failures, "Describe refused a host that lists a feature the provider does not know: "+errorText(err))
	case !proto.Equal(reply, r.full):
		failures = append(failures, "a host feature the provider does not know changed the description")
	}
	r.result(checkHostFeatures, failures)
}

// firstMinorHost describes the provider to a host that announces no
// contract version, as a host of contract 25.0 does.
func (r *run) firstMinorHost(ctx context.Context) {
	request := &protocol.DescribeRequest{Revision: provider.Revision, RequiredFeatures: slices.Clone(r.config.RequiredFeatures)}
	reply, err := r.describeOnce(ctx, request)
	refuses := provider.CheckContract(provider.HostContract(request), provider.MinimumContract(r.full)) != nil
	consistent := narrowsValidly(r.full, r.config.RequiredFeatures, nil)
	switch {
	case errors.Is(err, errNotStarted):
		r.add(checkFirstMinorHost, Failed, err.Error())
	case refuses && err == nil:
		r.add(checkFirstMinorHost, Failed, "Describe answered a contract 25.0 host although the provider's minimum is "+provider.ContractString(provider.MinimumContract(r.full)))
	case refuses:
		r.pass(checkFirstMinorHost)
	case err != nil && !consistent:
		r.pass(checkFirstMinorHost)
	case err != nil:
		r.add(checkFirstMinorHost, Failed, "Describe refused a contract 25.0 host: "+errorText(err))
	case !consistent:
		r.add(checkFirstMinorHost, Failed, "Describe answered a contract 25.0 host, which lists no two-way feature that a feature of the provider needs")
	case !proto.Equal(reply, provider.NegotiatedDescription(r.full, nil)):
		r.add(checkFirstMinorHost, Failed, "a contract 25.0 host received more than the full description without its two-way features")
	default:
		r.pass(checkFirstMinorHost)
	}
}

// descriptionChecks checks the description and how it narrows with the
// host's features.
func (r *run) descriptionChecks(ctx context.Context) {
	var invalid []string
	if err := provider.ValidateDescription(r.full, r.config.RequiredFeatures); err != nil {
		invalid = append(invalid, fmt.Sprintf("under %s: %v", r.all.profile.name, err))
	}
	none, err := r.connect(ctx, listedNone())
	switch {
	case errors.Is(err, errDescribeRefused) && !narrowsValidly(r.full, r.config.RequiredFeatures, nil):
		// A feature the provider lists needs a two-way feature, so it cannot
		// describe itself to a host that lists none, and the refusal is the
		// answer negotiation asks for, as it is from a host that leaves out
		// that one feature.
	case err != nil:
		invalid = append(invalid, err.Error())
	default:
		r.none = none
		if err := provider.ValidateDescription(none.reply, r.config.RequiredFeatures); err != nil {
			invalid = append(invalid, fmt.Sprintf("under %s: %v", none.profile.name, err))
		}
	}
	r.result(checkDescriptionValid, invalid)
	if got := r.full.GetProviderId(); got != r.config.ProviderID {
		r.add(checkIdentity, Failed, fmt.Sprintf("the description names %q, the manifest %q", got, r.config.ProviderID))
	} else {
		r.pass(checkIdentity)
	}
	second, err := r.connect(ctx, listedAll())
	switch {
	case err != nil:
		r.add(checkStable, Failed, "a second start failed: "+err.Error())
	case !proto.Equal(second.reply, r.full):
		r.second = second
		r.add(checkStable, Failed, "a second start described the provider differently")
	default:
		r.second = second
		r.pass(checkStable)
	}
	r.negotiation(ctx)
}

// negotiation compares the description under a host that lists no two-way
// feature, and under hosts that list every one but one, with the full
// description narrowed by provider.NegotiatedDescription. A description
// that keeps a feature the host did not list, or a field that feature adds,
// is what the check exists to find. Where the narrowed description is one the
// SDK refuses, because a feature the host still lists needs the one it left
// out, the provider must refuse the host rather than describe itself.
//
// Each host that lists every feature but one also sends the offline calls
// and, with a live database, a read, so that a reply that uses the one
// feature the host left out is found; calls.<name> and live.inspect report
// what they find.
func (r *run) negotiation(ctx context.Context) {
	var failures []string
	if r.none != nil && !proto.Equal(r.none.reply, provider.NegotiatedDescription(r.full, nil)) {
		failures = append(failures, "under "+r.none.profile.name+", the description keeps a two-way feature or a field one adds")
	}
	offered := offeredTwoWay(r.full)
	outcomes := make([]string, len(offered))
	var wait sync.WaitGroup
	slots := make(chan struct{}, negotiationParallelism)
	for i, feature := range offered {
		wait.Go(func() {
			slots <- struct{}{}
			defer func() { <-slots }()
			outcomes[i] = r.withoutFeature(ctx, feature)
		})
	}
	wait.Wait()
	r.result(checkNegotiation, append(failures, collect(outcomes...)...))
}

// negotiationParallelism bounds the providers the negotiation check starts at
// once: one for each two-way feature the provider offers.
const negotiationParallelism = 8

// withoutFeature describes the provider to a host that lists every two-way
// feature but feature, sends that host's calls and read, and returns what is
// wrong with the description.
func (r *run) withoutFeature(ctx context.Context, feature string) string {
	p := profile{name: "a host that does not list " + feature, features: slices.DeleteFunc(provider.TwoWayFeatures(), func(name string) bool { return name == feature })}
	expected := provider.NegotiatedDescription(r.full, p.features)
	consistent := narrowsValidly(r.full, r.config.RequiredFeatures, p.features)
	opened, err := r.config.Target.connect(ctx)
	if err != nil {
		return "the provider did not start for " + p.name + ": " + err.Error()
	}
	defer opened.close()
	reply, err := r.describe(ctx, opened, p.request(r.config.RequiredFeatures))
	switch {
	case err != nil && consistent:
		return fmt.Sprintf("Describe refused %s: %s", p.name, errorText(err))
	case err != nil:
		return ""
	case !consistent:
		return fmt.Sprintf("Describe answered %s, which a feature it lists needs", p.name)
	case !proto.Equal(reply, expected):
		return p.name + " receives it, or a field it adds"
	}
	r.narrowedWork(ctx, &conn{link: opened, profile: p, reply: reply, description: provider.NegotiatedDescription(reply, p.features)})
	return ""
}

// narrowedWork sends a narrowed host's offline calls and, with a live
// database, its read, and notes what fails for the checks that report it.
func (r *run) narrowedWork(ctx context.Context, c *conn) {
	for _, call := range r.config.Calls {
		r.note(checkCalls+"."+call.Name, r.narrowed(ctx, offlineRPCFor(call.Request), call.Request, call.Refused, c)...)
	}
	if r.config.Live == nil || r.engine == "" || !slices.Contains(r.full.GetFeatures(), provider.InspectFeature) {
		return
	}
	// One read at a time: the narrowed hosts run at once, and a database
	// refuses a burst of catalog scans it would answer one by one.
	r.reading.Lock()
	defer r.reading.Unlock()
	r.note(checkInspect, r.inspectTwice(ctx, c)...)
}

// note keeps failures a narrowed host found for the check id reports.
func (r *run) note(id string, failures ...string) {
	if len(failures) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.noted == nil {
		r.noted = make(map[string][]string)
	}
	r.noted[id] = append(r.noted[id], failures...)
}

// notedFor returns the failures narrowed hosts found for id, sorted: the hosts
// run at once, so the order they finish in says nothing.
func (r *run) notedFor(id string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	found := slices.Clone(r.noted[id])
	slices.Sort(found)
	return found
}

// offeredTwoWay returns the two-way features a description lists for the
// provider or for one of its engines, sorted.
func offeredTwoWay(description *protocol.Description) []string {
	var offered []string
	listed := append(slices.Clone(description.GetFeatures()), engineFeatures(description)...)
	for _, feature := range provider.TwoWayFeatures() {
		if slices.Contains(listed, feature) {
			offered = append(offered, feature)
		}
	}
	slices.Sort(offered)
	return offered
}

func engineFeatures(description *protocol.Description) []string {
	var features []string
	for _, engine := range description.GetEngines() {
		features = append(features, engine.GetFeatures()...)
	}
	return features
}
