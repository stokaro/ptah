package embedprovider_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedprovider"
)

// fakeProvider answers however a test needs it to.
//
// A struct of values rather than a function field: a row that carried the means
// to answer would be code in a table, and what varies between these cases is
// data -- the width, the number of vectors, whether it refuses.
type fakeProvider struct {
	profile embedprovider.Profile
	// width is the length of each vector it answers with.
	width int
	// answers is how many vectors it returns, whatever it was asked. Zero means
	// one per input, which is correct behavior.
	answers int
	// refuse is what it fails with, nil to answer.
	refuse error
	// ignoreCancellation makes it answer a canceled request.
	ignoreCancellation bool
	// nonFinite puts an infinity in the first vector.
	nonFinite bool
}

func (f *fakeProvider) Profile() embedprovider.Profile { return f.profile }

func (f *fakeProvider) Embed(
	ctx context.Context, inputs []string,
) (embedprovider.Result, error) {
	if !f.ignoreCancellation && ctx.Err() != nil {
		return embedprovider.Result{}, ctx.Err()
	}
	if f.refuse != nil {
		return embedprovider.Result{}, f.refuse
	}
	count := len(inputs)
	if f.answers > 0 {
		count = f.answers
	}
	result := embedprovider.Result{}
	for range count {
		vector := make(embedprovider.Vector, f.width)
		for index := range vector {
			vector[index] = 0.5
		}
		result.Vectors = append(result.Vectors, vector)
	}
	if f.nonFinite && len(result.Vectors) > 0 && f.width > 0 {
		result.Vectors[0][0] = infinity()
	}
	return result, nil
}

// infinity is a value a vector must never carry.
func infinity() float32 {
	huge := float32(1e38)
	return huge * huge
}

// working is a provider that answers correctly, at width four.
func working() *fakeProvider {
	return &fakeProvider{
		profile: embedprovider.Profile{
			Name: "local", Provider: "openai-compatible", EndpointClass: "local",
			EndpointHost: "127.0.0.1:11434", Model: "test-embed", Dimension: 4,
			// #nosec G101 -- a reference to where a credential lives, which is the
			// only form this package ever holds; there is no credential here.
			CredentialSource: "env:PTAH_EMBED_TOKEN",
		},
		width: 4,
	}
}

// refusing is a provider that answers nothing, with the sentinel a real one
// would use for a model identifier it does not have.
func refusing() *fakeProvider {
	provider := working()
	provider.refuse = errors.New("boom: " + embedprovider.ErrProvider.Error())
	provider.refuse = wrap(embedprovider.ErrProvider, "answered 404")
	return provider
}

// wrap builds an error carrying a sentinel, the way an adapter does.
func wrap(sentinel error, detail string) error {
	return errors.Join(sentinel, errors.New(detail))
}

// TestProbe_AWorkingProviderPassesEveryCheck is the control every case below
// needs.
//
// Without it a probe that failed everything satisfies each negative row and
// stops every specification this verb exists to make checkable.
func TestProbe_AWorkingProviderPassesEveryCheck(t *testing.T) {
	c := qt.New(t)

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: working(), Dimension: 4, Absent: refusing(),
	})

	c.Assert(report.Failures(), qt.HasLen, 0, qt.Commentf("%v", report.Failures()))
	c.Assert(report.Passed(), qt.IsTrue)
	c.Assert(report.Dimension, qt.Equals, 4)
	c.Assert(report.Unmeasured, qt.HasLen, 0)
	// And nothing it answered with is in the report. A probe is run before any
	// decision about data has been taken, so a report carrying an embedding is
	// one somebody has to redact.
	c.Assert(report.Checks, qt.Not(qt.HasLen), 0)
}

// TestProbe_ADeclaredWidthTheProviderDoesNotAnswerWithFails is the case the
// verb exists for.
//
// The dimension in a specification is what somebody typed, and until this verb
// the first thing that measured it was a backfill -- which had already sent
// source rows to the endpoint by the time it found out.
func TestProbe_ADeclaredWidthTheProviderDoesNotAnswerWithFails(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.width = 1024

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(failureNamed(c, report, embedprovider.CheckDimension).Detail, qt.Contains,
		"answered with 1024 dimensions and the specification declares 4")
	// And the width it actually answered with is reported, because that is the
	// number the specification has to be corrected to.
	c.Assert(report.Dimension, qt.Equals, 1024)
}

// TestProbe_AnUndeclaredWidthIsNotAPass is the honest answer for a
// specification that states none.
//
// A generation whose width nobody declared is one no later verification can
// check the stored vectors against, so reporting it as a pass would be the
// probe agreeing that nothing is wrong with a corpus nothing can measure.
func TestProbe_AnUndeclaredWidthIsNotAPass(t *testing.T) {
	c := qt.New(t)

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: working(), Dimension: 0, Absent: refusing(),
	})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(failureNamed(c, report, embedprovider.CheckDimension).Detail, qt.Contains,
		"the specification declares none")
}

// TestProbe_APartialBatchFails is the answer that produces a wrong corpus
// rather than a failed run.
//
// A caller cannot tell which input a short answer skipped, so attributing the
// vectors it did return puts some of them on the wrong rows.
func TestProbe_APartialBatchFails(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.answers = 1

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(failureNamed(c, report, embedprovider.CheckBatch).Detail, qt.Contains,
		"a partial batch is not a complete one")
}

// TestProbe_ANonFiniteValueFails is a vector PostgreSQL will take and no
// distance operator can use.
func TestProbe_ANonFiniteValueFails(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.nonFinite = true

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(failureNamed(c, report, embedprovider.CheckShape).Detail, qt.Contains, "vector 0")
}

// TestProbe_AProviderThatAnswersACanceledRequestFails is what turns an
// interrupt into a wait.
//
// A backfill an operator asked to stop keeps spending the provider budget if
// the adapter never looks at the context.
func TestProbe_AProviderThatAnswersACanceledRequestFails(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.ignoreCancellation = true

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(failureNamed(c, report, embedprovider.CheckCancellation).Detail, qt.Contains,
		"would keep spending")
}

// TestProbe_AnUnreachableEndpointStopsAndSaysWhatIsUnmeasured is the shape of a
// refusal.
//
// One refusal, not eight. A report that turned an unreachable endpoint into a
// failure of every check would say the endpoint is wrong in eight ways, and the
// checks it could not run are named rather than reported as passing or failing.
func TestProbe_AnUnreachableEndpointStopsAndSaysWhatIsUnmeasured(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.refuse = wrap(embedprovider.ErrUnreachable, "dial tcp: connection refused")

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(report.Checks, qt.HasLen, 1)
	c.Assert(report.Checks[0].Name, qt.Equals, embedprovider.CheckReachable)
	c.Assert(report.Unmeasured, qt.Contains,
		"everything after reachability, including the error shape, because nothing answered")
}

// TestProbe_ARefusedCredentialIsNotAnUnreachableEndpoint is the distinction an
// operator acts on.
//
// One sends them to their network and the other to their secret store.
func TestProbe_ARefusedCredentialIsNotAnUnreachableEndpoint(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.refuse = wrap(embedprovider.ErrUnauthorized, "answered 401")

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(report.Checks, qt.HasLen, 2)
	c.Assert(report.Checks[0].Name, qt.Equals, embedprovider.CheckReachable)
	c.Assert(report.Checks[0].Passed, qt.IsTrue)
	c.Assert(report.Checks[1].Name, qt.Equals, embedprovider.CheckAuthorized)
	c.Assert(report.Checks[1].Passed, qt.IsFalse)
	// And it names WHERE the credential came from, never what it was.
	c.Assert(report.Checks[1].Detail, qt.Contains, "env:PTAH_EMBED_TOKEN")
}

// TestProbe_AnEndpointThatAnswersForAnyModelIsReportedAsUnmeasured is a fact
// about the endpoint rather than a failure of Ptah's.
//
// A gateway that answers for a model identifier nobody configured cannot tell
// an operator that they misspelled their model, and a probe that called this a
// pass would leave that unsaid.
func TestProbe_AnEndpointThatAnswersForAnyModelIsReportedAsUnmeasured(t *testing.T) {
	c := qt.New(t)

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: working(), Dimension: 4, Absent: working(),
	})

	c.Assert(report.Passed(), qt.IsTrue)
	c.Assert(report.Unmeasured, qt.HasLen, 1)
	c.Assert(report.Unmeasured[0], qt.Contains, "a typo in the model name would not be reported")
}

// TestProbe_NoAbsentEndpointLeavesTheRefusalShapeUnmeasured is the other half.
//
// A check that did not run must not read as one that passed.
func TestProbe_NoAbsentEndpointLeavesTheRefusalShapeUnmeasured(t *testing.T) {
	c := qt.New(t)

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: working(), Dimension: 4,
	})

	c.Assert(report.Passed(), qt.IsTrue)
	c.Assert(report.Unmeasured, qt.Contains,
		"how a refusal arrives, because no absent-model endpoint was supplied")
	for _, check := range report.Checks {
		c.Assert(check.Name, qt.Not(qt.Equals), embedprovider.CheckErrorShape)
	}
}

// failureNamed returns the failed check with a name, and fails the test when
// there is none.
func failureNamed(
	c *qt.C, report embedprovider.ProbeReport, name embedprovider.CheckName,
) embedprovider.Check {
	c.Helper()
	for _, check := range report.Failures() {
		if check.Name == name {
			return check
		}
	}
	c.Fatalf("no failed check named %s in %v", name, report.Checks)
	return embedprovider.Check{}
}

// TestProbe_AnEndpointNeedingNoCredentialReadsAsOneSentence covers
// stokaro/ptah#2648 finding 6.
//
// A specification with no `credential:` is the shape `guides/configure-a-
// provider.md` recommends for a local endpoint, and the probe rendered its pass
// line by appending " was accepted" to a string that was already a whole
// sentence: `no credential was sent, and the endpoint asked for none was
// accepted`. Two predicates, one subject.
//
// The pass line is asserted whole rather than by substring, because the defect
// was a suffix and a Contains check on the first clause would have passed
// throughout. The published sentence is the target.
func TestProbe_AnEndpointNeedingNoCredentialReadsAsOneSentence(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.profile.CredentialSource = ""

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(authorizedDetail(c, report), qt.Equals,
		"no credential was sent, and the endpoint asked for none")
}

// TestProbe_AnEndpointThatRefusesAnAbsentCredentialSaysSo is the half that was
// not a papercut.
//
// The shared subject made a refusal read `no credential was sent, and the
// endpoint asked for none was refused: 401` -- a sentence asserting the
// endpoint asked for nothing while reporting that it refused the request. The
// `Not(Contains)` is what keeps that clause from coming back: an assertion on
// the new wording alone would pass over a string that carried both.
func TestProbe_AnEndpointThatRefusesAnAbsentCredentialSaysSo(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.profile.CredentialSource = ""
	provider.refuse = wrap(embedprovider.ErrUnauthorized, "answered 401")

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	detail := authorizedDetail(c, report)
	c.Assert(detail, qt.Contains, "no credential was sent, and the endpoint refused the request")
	c.Assert(detail, qt.Contains, "answered 401")
	c.Assert(detail, qt.Not(qt.Contains), "asked for none")
}

// TestProbe_AConfiguredCredentialStillNamesItsSource is the control.
//
// Every assertion above is satisfied by a probe that stopped naming where a
// credential came from at all, which is the fact an operator needs when the
// endpoint says no.
func TestProbe_AConfiguredCredentialStillNamesItsSource(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.refuse = wrap(embedprovider.ErrUnauthorized, "answered 401")

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(authorizedDetail(c, report), qt.Equals,
		"the credential from env:PTAH_EMBED_TOKEN was refused: "+
			"embedding endpoint refused the credential\nanswered 401")
}

// authorizedDetail is what the report says about authorization.
func authorizedDetail(c *qt.C, report embedprovider.ProbeReport) string {
	c.Helper()
	for _, check := range report.Checks {
		if check.Name == embedprovider.CheckAuthorized {
			return check.Detail
		}
	}
	c.Fatal("the report carries no authorization check")
	return ""
}

// TestProbe_AMalformedVectorIsShapeAndNotBatch is stokaro/ptah#2641 finding 2,
// at the level where the two checks are told apart.
//
// `batchCheck` ran the same whole-answer validation `shapeCheck` does, so one
// empty vector failed both -- and the second failure said the endpoint could
// not carry a batch of two, which it can. An operator reducing their batch size
// over that would be chasing a defect that is not there.
func TestProbe_AMalformedVectorIsShapeAndNotBatch(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.width = 0

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(failureNamed(c, report, embedprovider.CheckShape).Detail, qt.Contains, "vector 0 is empty")
	c.Assert(passedNamed(c, report, embedprovider.CheckBatch).Detail, qt.Equals,
		"2 inputs answered with 2 vectors")
}

// TestProbe_AShortAnswerIsBatchAndNotShape is the other half of the pair.
//
// Either check alone can be satisfied by one that fires for everything. Only
// the pair shows that each answers its own question: here the vectors are well
// formed and the count is wrong, and the verdicts are the exact opposite of the
// case above.
func TestProbe_AShortAnswerIsBatchAndNotShape(t *testing.T) {
	c := qt.New(t)
	provider := working()
	provider.answers = 1

	report := embedprovider.Probe(context.Background(), embedprovider.ProbeSubject{
		Provider: provider, Dimension: 4, Absent: refusing(),
	})

	c.Assert(passedNamed(c, report, embedprovider.CheckShape).Passed, qt.IsTrue)
	c.Assert(failureNamed(c, report, embedprovider.CheckBatch).Detail, qt.Equals,
		"2 inputs answered with 1 vector: a partial batch is not a complete one")
}

// passedNamed is the check by that name, which must have passed.
func passedNamed(c *qt.C, report embedprovider.ProbeReport, name embedprovider.CheckName) embedprovider.Check {
	c.Helper()
	for _, check := range report.Checks {
		if check.Name == name && check.Passed {
			return check
		}
	}
	c.Fatalf("the report carries no passing %s check", name)
	return embedprovider.Check{}
}
