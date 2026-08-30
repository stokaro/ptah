package embedprovider

import (
	"context"
	"errors"
	"fmt"
)

// ProbeInputs are what a probe sends.
//
// Two fixed strings, and nothing from the operator's database. A probe exists
// to be run BEFORE a generation is built -- before anybody has decided to send
// a corpus to this endpoint -- so it must be answerable without sending one
// row. They differ from each other because the batch check needs two inputs and
// two identical ones cannot show that a provider answered for both.
var ProbeInputs = []string{
	"ptah provider probe",
	"ptah provider probe, second input",
}

// CheckName identifies one thing a probe establishes.
type CheckName string

// The checks, in the order they are attempted. Each is a separate question, so
// a provider that fails one is reported as failing that one rather than as
// unusable.
const (
	// CheckReachable is whether the endpoint answered at all.
	CheckReachable CheckName = "reachable"
	// CheckAuthorized is whether it accepted the credential.
	CheckAuthorized CheckName = "authorized"
	// CheckEmbeds is whether it answered an embedding request for the model.
	CheckEmbeds CheckName = "embeds"
	// CheckShape is one vector per input, of a usable length, with finite
	// values.
	CheckShape CheckName = "shape"
	// CheckDimension is whether the answer's width is the one the generation
	// was declared with.
	CheckDimension CheckName = "dimension"
	// CheckBatch is whether a request carrying several inputs is answered for
	// every one of them.
	CheckBatch CheckName = "batch"
	// CheckCancellation is whether a canceled request stops rather than
	// returning an answer nobody waited for.
	CheckCancellation CheckName = "cancellation"
	// CheckErrorShape is whether a refusal arrives as one of this package's
	// errors rather than as whatever the endpoint wrote.
	CheckErrorShape CheckName = "error shape"
)

// Check is one thing a probe established, or did not.
type Check struct {
	Name CheckName
	// Passed is the answer.
	Passed bool
	// Detail says what was measured, on a pass as well as on a failure. A check
	// that explains itself only when it fails leaves a reader unable to tell a
	// measurement from an assumption.
	Detail string
}

// ProbeReport is what a probe found.
//
// It carries no vector. The whole point of a probe is that it can be run and
// pasted into an issue before any decision about data has been taken, and a
// report carrying an embedding of anything is a report somebody has to redact.
type ProbeReport struct {
	// Profile is the endpoint, without its credential.
	Profile Profile
	// Checks are what was established, in attempt order.
	Checks []Check
	// Dimension is the width the provider actually answered with, zero when
	// nothing answered.
	Dimension int
	// Unmeasured names what did not run, and why.
	//
	// A report listing only what it checked reads as though it checked
	// everything, which is the reading a decision about sending a corpus
	// somewhere must not be made on.
	Unmeasured []string
}

// Passed reports whether every check that ran passed.
func (r ProbeReport) Passed() bool {
	for _, check := range r.Checks {
		if !check.Passed {
			return false
		}
	}
	return len(r.Checks) > 0
}

// Failures are the checks that did not pass.
func (r ProbeReport) Failures() []Check {
	var failures []Check
	for _, check := range r.Checks {
		if !check.Passed {
			failures = append(failures, check)
		}
	}
	return failures
}

// ProbeSubject is what a probe measures.
type ProbeSubject struct {
	// Provider is the configured endpoint.
	Provider Provider
	// Dimension is the width the generation was declared with, zero when the
	// specification declares none.
	Dimension int
	// Absent is the same endpoint configured with a model identifier nothing
	// should recognize, used to see how a refusal arrives. Nil skips that
	// check, which is then reported as unmeasured rather than as a pass.
	Absent Provider
}

// Probe asks a provider what it answers, without sending anything from the
// operator's database.
//
// It exists because every fact a plan states about a provider is CONFIGURED:
// the model identifier and the output dimension are what somebody typed, and
// the first thing that measured them was a backfill -- which had already sent
// source rows to the endpoint by the time it found out the width was wrong.
// This is the same measurement taken before that decision (stokaro/ptah#2068).
//
// Every check is attempted independently where it can be. A provider that
// refuses the credential cannot be asked about its dimension, so the checks
// that depend on an answer are reported as unmeasured rather than as failures:
// a report that turned one refusal into eight would say the endpoint is wrong
// in eight ways.
func Probe(ctx context.Context, subject ProbeSubject) ProbeReport {
	report := ProbeReport{Profile: subject.Provider.Profile()}

	result, err := subject.Provider.Embed(ctx, ProbeInputs[:1])
	if err != nil {
		return probeRefusal(report, subject, err)
	}
	report.Checks = append(report.Checks,
		Check{Name: CheckReachable, Passed: true,
			Detail: "the endpoint at " + report.Profile.EndpointHost + " answered"},
		Check{Name: CheckAuthorized, Passed: true,
			Detail: credentialDetail(report.Profile)},
		Check{Name: CheckEmbeds, Passed: true,
			Detail: "model " + report.Profile.Model + " answered an embedding request"})

	report.Dimension = measuredDimension(result)
	report.Checks = append(report.Checks,
		shapeCheck(result), dimensionCheck(result, subject.Dimension))
	report.Checks = append(report.Checks, batchCheck(ctx, subject.Provider))
	report.Checks = append(report.Checks, cancellationCheck(subject.Provider))
	return errorShapeCheck(ctx, report, subject)
}

// probeRefusal is what a probe can say when the first request did not answer.
//
// The refusal itself is the measurement: the sentinel says which of the first
// three checks failed, and everything after it is unmeasured because nothing
// answered to measure.
func probeRefusal(report ProbeReport, subject ProbeSubject, err error) ProbeReport {
	reachable := !errors.Is(err, ErrUnreachable)
	authorized := reachable && !errors.Is(err, ErrUnauthorized)

	report.Checks = append(report.Checks, Check{
		Name: CheckReachable, Passed: reachable,
		Detail: reachabilityDetail(reachable, report.Profile, err),
	})
	if !reachable {
		report.Unmeasured = append(report.Unmeasured,
			"everything after reachability, because nothing answered")
		return report
	}
	report.Checks = append(report.Checks, Check{
		Name: CheckAuthorized, Passed: authorized,
		Detail: authorizationDetail(authorized, report.Profile, err),
	})
	if !authorized {
		report.Unmeasured = append(report.Unmeasured,
			"everything after authentication, because the credential was refused")
		return report
	}
	report.Checks = append(report.Checks, Check{
		Name: CheckEmbeds, Passed: false,
		Detail: fmt.Sprintf("model %s did not answer an embedding request: %v",
			report.Profile.Model, err),
	})
	report.Unmeasured = append(report.Unmeasured,
		"the shape, dimension, batch and cancellation checks, because there is no answer to measure")
	_ = subject
	return report
}

// reachabilityDetail says what happened, either way.
func reachabilityDetail(reachable bool, profile Profile, err error) string {
	if reachable {
		return "the endpoint at " + profile.EndpointHost + " answered"
	}
	return fmt.Sprintf("the endpoint at %s could not be reached: %v", profile.EndpointHost, err)
}

// authorizationDetail says what happened, either way.
func authorizationDetail(authorized bool, profile Profile, err error) string {
	if authorized {
		return credentialDetail(profile)
	}
	return fmt.Sprintf("%s was refused: %v", credentialSource(profile), err)
}

// credentialDetail names where the credential came from, never what it was.
func credentialDetail(profile Profile) string {
	return credentialSource(profile) + " was accepted"
}

// credentialSource names the reference, or the absence of one.
func credentialSource(profile Profile) string {
	if profile.CredentialSource == "" {
		return "no credential was sent, and the endpoint asked for none"
	}
	return "the credential from " + profile.CredentialSource
}

// shapeCheck is one vector per input, non-empty, and finite throughout.
func shapeCheck(result Result) Check {
	if err := ValidateResult(result, 1, 0); err != nil {
		return Check{Name: CheckShape, Passed: false, Detail: err.Error()}
	}
	return Check{Name: CheckShape, Passed: true,
		Detail: fmt.Sprintf("one vector of %d finite values for one input",
			measuredDimension(result))}
}

// dimensionCheck compares the measured width with the declared one.
//
// A generation that declares none is reported as unmeasured rather than as a
// pass: the width is then whatever the provider answers, and a corpus built on
// a width nobody stated is one nothing can verify later.
func dimensionCheck(result Result, declared int) Check {
	measured := measuredDimension(result)
	if declared == 0 {
		return Check{Name: CheckDimension, Passed: false,
			Detail: fmt.Sprintf("the provider answered with %d dimensions and the "+
				"specification declares none, so nothing can check the stored vectors later",
				measured)}
	}
	if measured != declared {
		return Check{Name: CheckDimension, Passed: false,
			Detail: fmt.Sprintf("the provider answered with %d dimensions and the "+
				"specification declares %d", measured, declared)}
	}
	return Check{Name: CheckDimension, Passed: true,
		Detail: fmt.Sprintf("%d dimensions, as declared", measured)}
}

// batchCheck asks for two inputs and requires two answers.
//
// A provider that answers for one of them is the case that matters: a caller
// cannot tell which input a short answer skipped, so attributing the vectors it
// did return would put some of them on the wrong rows.
func batchCheck(ctx context.Context, provider Provider) Check {
	result, err := provider.Embed(ctx, ProbeInputs)
	if err != nil {
		return Check{Name: CheckBatch, Passed: false,
			Detail: fmt.Sprintf("a request carrying %d inputs failed: %v", len(ProbeInputs), err)}
	}
	if err := ValidateResult(result, len(ProbeInputs), 0); err != nil {
		return Check{Name: CheckBatch, Passed: false, Detail: err.Error()}
	}
	return Check{Name: CheckBatch, Passed: true,
		Detail: fmt.Sprintf("%d inputs answered with %d vectors",
			len(ProbeInputs), len(result.Vectors))}
}

// cancellationCheck requires a canceled request to stop.
//
// A provider that ignores cancellation turns every interrupt into a wait, and a
// backfill an operator asked to stop keeps spending the provider budget. The
// context is canceled before the call, so what is measured is whether the
// adapter looks at it at all.
func cancellationCheck(provider Provider) Check {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := provider.Embed(canceled, ProbeInputs[:1])
	if errors.Is(err, context.Canceled) {
		return Check{Name: CheckCancellation, Passed: true,
			Detail: "a canceled request stopped rather than answering"}
	}
	if err != nil {
		return Check{Name: CheckCancellation, Passed: false,
			Detail: fmt.Sprintf("a canceled request failed with %v rather than reporting "+
				"the cancellation", err)}
	}
	return Check{Name: CheckCancellation, Passed: false,
		Detail: "a canceled request was answered, so an interrupted backfill would keep spending"}
}

// errorShapeCheck asks for a model nothing should have and requires the refusal
// to arrive classified.
//
// Not cosmetic. Every decision a run takes about a failure -- retry, stop, fail
// the batch -- reads these sentinels, so a refusal that arrives as an
// unclassified error is one the engine cannot act on correctly.
func errorShapeCheck(ctx context.Context, report ProbeReport, subject ProbeSubject) ProbeReport {
	if subject.Absent == nil {
		report.Unmeasured = append(report.Unmeasured,
			"how a refusal arrives, because no absent-model endpoint was supplied")
		return report
	}
	_, err := subject.Absent.Embed(ctx, ProbeInputs[:1])
	switch {
	case err == nil:
		// The endpoint answered for a model identifier nobody configured. That
		// is a fact about the endpoint rather than a failure of Ptah's, and it
		// is worth saying: a gateway that answers for any name cannot tell an
		// operator that they misspelled their model.
		report.Unmeasured = append(report.Unmeasured,
			"how a refusal arrives, because the endpoint answered for a model identifier "+
				"nothing configured; a typo in the model name would not be reported by it")
	case errors.Is(err, ErrProvider), errors.Is(err, ErrUnauthorized), errors.Is(err, ErrUnreachable):
		report.Checks = append(report.Checks, Check{Name: CheckErrorShape, Passed: true,
			Detail: "a refused request arrived as a classified error the engine can act on"})
	default:
		report.Checks = append(report.Checks, Check{Name: CheckErrorShape, Passed: false,
			Detail: fmt.Sprintf("a refused request arrived unclassified: %v", err)})
	}
	return report
}

// measuredDimension is the width of the first vector, or zero.
func measuredDimension(result Result) int {
	if len(result.Vectors) == 0 {
		return 0
	}
	return len(result.Vectors[0])
}
