package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/embedprovider"
	"go.5x5.cz/ptah/internal/embedspec"
)

// absentModel is a model identifier no endpoint should recognize.
//
// It is deliberately not a plausible name. What the probe reads from asking for
// it is the SHAPE of the refusal, so a string that happened to name a real
// model somewhere would turn that check into a measurement of somebody's
// gateway.
const absentModel = "ptah-probe-no-such-model"

// newProbeCommand returns "ptah inference probe".
func newProbeCommand() *cobra.Command {
	var options commonOptions
	var format string
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "probe",
		Short: "Ask the embedding provider what it answers, without sending a source row",
		Long: `Measure the endpoint a specification names, before anything is built.

Every fact a plan states about a provider is configured rather than measured:
the model identifier and the output dimension are what somebody typed. The first
thing that checked them used to be the backfill -- which had already sent source
rows to the endpoint by the time it found the width was wrong.

This asks the same questions first, and it sends two fixed strings and nothing
from your database. It needs no database at all, so it is also what a CI job
runs against a specification an author is still writing.

What it establishes: that the endpoint answers, that it accepts the credential
the specification points at, that the model answers an embedding request, that
one input comes back as one usable vector, that the width is the one the
specification declares, that a batch is answered for every input, that a
canceled request stops, and that a refusal arrives as an error the engine can
act on.

What it cannot establish is whether the provider retains what you send it. Ptah
never claims a guarantee it cannot enforce.

It returns 1 when a check fails, so a pipeline can gate on it.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runProbe(cmd.Context(), cmd.OutOrStdout(), options, format, timeout)
		},
	}
	addSpecFlags(cmd, &options)
	cmd.Flags().StringVar(&format, "format", "text", "Output format: text or json")
	cmd.Flags().DurationVar(&timeout, "provider-timeout", 30*time.Second,
		"How long one provider request may take")
	return cmd
}

// runProbe measures the provider and reports.
func runProbe(
	ctx context.Context, out io.Writer, options commonOptions, format string, timeout time.Duration,
) error {
	if format != "text" && format != "json" {
		return fmt.Errorf("invalid --format value %q: text or json", format)
	}
	loaded, err := options.spec.resolve(ctx)
	if err != nil {
		return err
	}
	subject, err := probeSubject(loaded, timeout)
	if err != nil {
		return err
	}

	report := embedprovider.Probe(ctx, subject)
	if err := writeProbe(out, format, report); err != nil {
		return err
	}
	if report.Passed() {
		return nil
	}
	return exitcode.New(1, fmt.Errorf("%d of %d provider checks did not pass",
		len(report.Failures()), len(report.Checks)))
}

// probeSubject builds the endpoint the specification names, and a second one
// asking for a model nothing should have.
//
// Two providers rather than one with a flag, because the second is a different
// question: what a refusal looks like. Building it here rather than inside the
// probe keeps that package over the interface, with no knowledge of how an
// endpoint is configured.
func probeSubject(
	loaded embedspec.Loaded, timeout time.Duration,
) (embedprovider.ProbeSubject, error) {
	configured, err := buildProviderFor(loaded, loaded.Spec.Model.Identifier, timeout)
	if err != nil {
		return embedprovider.ProbeSubject{}, err
	}
	absent, err := buildProviderFor(loaded, absentModel, timeout)
	if err != nil {
		return embedprovider.ProbeSubject{}, err
	}
	return embedprovider.ProbeSubject{
		Provider:  configured,
		Dimension: loaded.Spec.Model.ReportedDimension,
		Absent:    absent,
	}, nil
}

// buildProviderFor resolves the endpoint with a chosen model identifier.
func buildProviderFor(
	loaded embedspec.Loaded, model string, timeout time.Duration,
) (embedprovider.Provider, error) {
	reference, err := embedprovider.ParseCredentialRef(loaded.Credential)
	if err != nil {
		return nil, err
	}
	return embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name:     loaded.Spec.Model.Provider,
		BaseURL:  loaded.Endpoint,
		Model:    model,
		Revision: loaded.Spec.Model.Revision,
		// Zero, so the adapter accepts whatever width comes back and the probe
		// is the thing that compares it with the declared one.
		//
		// A run configures the declared width here, and the adapter then
		// refuses a mismatch as a failed request. That is right for a backfill
		// -- a wrong vector must not be written -- and wrong here: it would
		// report "the model did not answer an embedding request" for an
		// endpoint that answered perfectly well, at a width the operator needs
		// to be told.
		Dimension:          0,
		RequestedDimension: loaded.Spec.Model.RequestedDimension,
		EndpointClass:      string(loaded.Spec.Model.EndpointClass),
		Credential:         reference,
		MaxBatch:           len(embedprovider.ProbeInputs),
		Timeout:            timeout,
	})
}

// writeProbe renders the report in whichever form was asked for.
func writeProbe(out io.Writer, format string, report embedprovider.ProbeReport) error {
	if format == "json" {
		return writeProbeJSON(out, report)
	}
	return writeProbeText(out, report)
}

// probeDocument is the JSON form.
//
// Its own type rather than the report's own tags, because what a pipeline reads
// and what the package models are two decisions: the profile is flattened to
// the four fields a reader acts on, and the vectors that are not in the report
// at all stay absent by construction rather than by a tag somebody could
// change.
type probeDocument struct {
	Endpoint   string              `json:"endpoint"`
	Class      string              `json:"endpoint_class"`
	Model      string              `json:"model"`
	Credential string              `json:"credential_source,omitempty"`
	Dimension  int                 `json:"dimension"`
	Passed     bool                `json:"passed"`
	Checks     []probeCheckElement `json:"checks"`
	Unmeasured []string            `json:"unmeasured,omitempty"`
}

// probeCheckElement is one check as a pipeline reads it.
type probeCheckElement struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

// writeProbeJSON is what a pipeline consumes.
func writeProbeJSON(out io.Writer, report embedprovider.ProbeReport) error {
	document := probeDocument{
		Endpoint: report.Profile.EndpointHost, Class: report.Profile.EndpointClass,
		Model: report.Profile.Model, Credential: report.Profile.CredentialSource,
		Dimension: report.Dimension, Passed: report.Passed(),
		Unmeasured: report.Unmeasured,
	}
	for _, check := range report.Checks {
		document.Checks = append(document.Checks, probeCheckElement{
			Name: string(check.Name), Passed: check.Passed, Detail: check.Detail,
		})
	}
	body, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("render the probe: %w", err)
	}
	_, err = fmt.Fprintf(out, "%s\n", body)
	return err
}

// writeProbeText is what a person reads.
func writeProbeText(out io.Writer, report embedprovider.ProbeReport) error {
	lines := []string{
		fmt.Sprintf("%s at %s, declared %s",
			report.Profile.Model, report.Profile.EndpointHost, report.Profile.EndpointClass),
	}
	for _, check := range report.Checks {
		lines = append(lines, bullet(fmt.Sprintf("%s %s: %s",
			marks[check.Passed], check.Name, check.Detail)))
	}
	// What did not run, always. A report saying only what it checked reads as
	// though it checked everything, and this one is read before a decision to
	// send a corpus somewhere.
	for _, unmeasured := range report.Unmeasured {
		lines = append(lines, bullet("not measured: "+unmeasured))
	}
	return writeLines(out, lines...)
}

// marks render an answer, padded so the names below them line up.
var marks = map[bool]string{true: "ok  ", false: "fail"}
