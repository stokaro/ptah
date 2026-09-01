package inference

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/ociartifact"
)

// evidenceOptions are the flags every verb that can leave a record takes.
type evidenceOptions struct {
	// publishTo is the OCI reference the record is pushed to, empty for none.
	publishTo string
	// writeTo is the local path the record is written to, empty for none.
	writeTo string
	// attachTo is the OCI reference of the release this record is about, empty
	// for none. A record naming one is published into that release's own
	// repository, as a referrer of it.
	attachTo string
}

// addEvidenceFlags registers them.
//
// --plain-http is not among them. It belongs to the verb rather than to one
// direction of traffic: the same run can fetch a release and publish a record,
// and two flags meaning "this registry speaks HTTP" would let an operator
// answer the question once and have it apply to half of what they did.
func addEvidenceFlags(flags *pflag.FlagSet, options *evidenceOptions) {
	flags.StringVar(&options.publishTo, "publish-evidence", "",
		"OCI reference to publish this run's record to; omitted keeps it out of a registry")
	flags.StringVar(&options.writeTo, "evidence-file", "",
		"Path to write this run's record to as JSON; omitted writes no file")
}

// addSubjectFlag registers the flag that says what a record is about.
//
// Only the verbs that produce a record ABOUT something take it. A release is
// what the others attach to, so a release attaching to a release would be a
// question nothing asks.
func addSubjectFlag(cmd *cobra.Command, options *evidenceOptions) {
	cmd.Flags().StringVar(&options.attachTo, "attach-to", "",
		"OCI reference of the release this record is about; the record is "+
			"published into that release's repository as a referrer of it, "+
			"which is how it is found without remembering a tag")
	// A referrer lands in its subject's repository, so a run naming both would
	// have said where the record went twice and the registry would have obeyed
	// one of them.
	requireExclusiveOnCommandLine(cmd, "publish-evidence", "attach-to")
}

// destinationNamed reports whether this run was asked to leave a record
// anywhere. Building one costs nothing next to the run that produced it, and
// guarding on the registry alone made --evidence-file do nothing without one.
func (e evidenceOptions) destinationNamed() bool {
	return e.publishTo != "" || e.writeTo != "" || e.attachTo != ""
}

// publishRecord pushes a record and says where it went.
//
// A failure to publish is reported and does not fail the verb. The measurement
// or the pointer move already happened; failing here would report a run that did
// not do what it did, and the registry being unreachable is not a fact about the
// generation.
func publishRecord(
	ctx context.Context, out io.Writer, options commonOptions, evidence evidenceOptions,
	record embedrelease.Record, err error, swallow publicationFailure,
) error {
	if err != nil {
		return err
	}
	if err := writeRecordFile(out, evidence.writeTo, record); err != nil {
		return err
	}
	if evidence.publishTo == "" && evidence.attachTo == "" {
		return nil
	}
	if err := refuseMixedPlainHTTP(options, evidence); err != nil {
		// Before the push rather than after: the point is that a credential
		// for the second registry never leaves over an unencrypted connection.
		return err
	}
	result, publishErr := sendRecord(ctx, options.spec.plainHTTP, evidence, record)
	if publishErr != nil {
		if reported := writeLines(out, bullet(fmt.Sprintf(
			"the record was not published: %v", publishErr))); reported != nil {
			return reported
		}
		return publicationOutcome(swallow, publishErr)
	}
	return writeLines(out, bullet(fmt.Sprintf(
		"record %s published as %s", record.Digest[:12], result.Descriptor.Digest)))
}

// sendRecord pushes a record, or attaches it to what it is about.
//
// The two are separate calls rather than one with an optional subject, because
// a referrer lands in its subject's repository and a standalone record lands
// where the operator said. Cobra has already refused a run naming both.
func sendRecord(
	ctx context.Context, plainHTTP bool, evidence evidenceOptions, record embedrelease.Record,
) (ociartifact.PushResult, error) {
	options := embedrelease.PublishOptions{
		RecordedAt: time.Now().UTC(), PlainHTTP: plainHTTP,
	}
	if evidence.attachTo != "" {
		return embedrelease.Attach(ctx, evidence.attachTo, record, options)
	}
	return embedrelease.Publish(ctx, evidence.publishTo, record, options)
}

// refuseMixedPlainHTTP stops one unencrypted-transport decision covering two
// registries.
//
// --plain-http says "this registry speaks HTTP", and it is one flag because one
// run usually names one registry. A run that fetches --release from a trusted
// local registry and publishes its record to an authenticated production one
// names two, and forwarding the exception to both would offer the second
// registry's credential over an unencrypted connection -- which is the opposite
// of what the flag's own help promises.
//
// Refused rather than scoped per host: a run that meant to reach two registries
// on two transports can say so by publishing in a second command, and a flag
// that quietly applied to one of them would be the silent half of this problem
// rather than the fix.
func refuseMixedPlainHTTP(options commonOptions, evidence evidenceOptions) error {
	if !options.spec.plainHTTP || strings.TrimSpace(options.spec.reference) == "" {
		return nil
	}
	release := registryHostOf(options.spec.reference)
	destination := evidence.publishTo
	if destination == "" {
		destination = evidence.attachTo
	}
	if host := registryHostOf(destination); host == release {
		return nil
	}
	return fmt.Errorf(
		"--plain-http is one registry's exception and this run names two: %s for the release "+
			"and %s for the record; publish the record in a separate command, or reach both "+
			"over TLS",
		registryHostOf(options.spec.reference), registryHostOf(destination))
}

// registryHostOf is the host an OCI reference addresses, for comparison only.
//
// An oci-layout:// directory has no host and compares equal to nothing, which
// is the right answer: a local directory is not a registry a credential is
// offered to.
func registryHostOf(reference string) string {
	trimmed := strings.TrimSpace(reference)
	if ociartifact.IsLayoutRef(trimmed) {
		return trimmed
	}
	_, rest, found := strings.Cut(trimmed, "://")
	if !found {
		rest = trimmed
	}
	host, _, _ := strings.Cut(rest, "/")
	return host
}

// writeRecordFile keeps the record where a registry is not.
//
// A registry is where evidence belongs when there is one, and there often is
// not: a first migration, a CI job that runs before anything is published, an
// operator who has no registry at all. The record is the same bytes either way,
// so what somebody keeps locally is what they would have fetched.
//
// The two destinations are independent. Naming both writes the file and pushes
// the artifact, and naming neither is the default -- the record is built
// regardless, because the cost of building it is nothing next to the run that
// produced it.
//
// A failure to write is reported and does not fail the verb, for the reason a
// failure to publish does not: the measurement or the pointer move already
// happened, and a directory that is not there is not a fact about the
// generation. Failing here would report a run that did not do what it did.
func writeRecordFile(out io.Writer, path string, record embedrelease.Record) error {
	if path == "" {
		return nil
	}
	// 0o600 because the record is the operator's to share rather than the
	// filesystem's to offer. It carries no credential -- the specification
	// names where one lives, never what it is -- and it does carry what a
	// corpus was built from.
	if err := os.WriteFile(path, record.Body, 0o600); err != nil {
		return writeLines(out, bullet(fmt.Sprintf("the record was not written: %v", err)))
	}
	return writeLines(out, bullet(fmt.Sprintf(
		"record %s written to %s", record.Digest[:12], path)))
}

// publicationFailure says what a failed publication means to the verb that
// asked for it, and there are exactly two answers.
type publicationFailure bool

const (
	// swallowed: the verb's own work is committed. A measurement was taken, a
	// pointer was moved, a generation was retired -- and failing after that
	// would report a run that did not do what it did. The failure is on
	// standard output and the exit code says the run succeeded, because it did.
	swallowed publicationFailure = true
	// fatal: publishing IS the verb's effect. `plan` writes nothing anywhere,
	// so a `plan --publish-evidence` that published nothing did nothing, and
	// exiting 0 told a CI job it had released what the next environment
	// promotes. The promotion downstream then kept running the previous release
	// under the same tag (stokaro/ptah#2649 finding 7).
	fatal publicationFailure = false
)

// publicationOutcome turns a reported failure into the caller's exit code.
func publicationOutcome(swallow publicationFailure, publishErr error) error {
	if swallow == swallowed {
		return nil
	}
	return publishErr
}
