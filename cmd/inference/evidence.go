package inference

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/embedeval"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/embedverify"
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
	// plainHTTP permits an unencrypted connection to that registry.
	plainHTTP bool
}

// addEvidenceFlags registers them.
func addEvidenceFlags(flags *pflag.FlagSet, options *evidenceOptions) {
	flags.StringVar(&options.publishTo, "publish-evidence", "",
		"OCI reference to publish this run's record to; omitted keeps it out of a registry")
	flags.StringVar(&options.writeTo, "evidence-file", "",
		"Path to write this run's record to as JSON; omitted writes no file")
	dbcli.RegisterPlainHTTPFlag(flags, &options.plainHTTP)
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
	cmd.MarkFlagsMutuallyExclusive("publish-evidence", "attach-to")
}

// destinationNamed reports whether this run was asked to leave a record
// anywhere. Building one costs nothing next to the run that produced it, and
// guarding on the registry alone made --evidence-file do nothing without one.
func (e evidenceOptions) destinationNamed() bool {
	return e.publishTo != "" || e.writeTo != "" || e.attachTo != ""
}

// verificationRecord turns a report into the record a registry holds.
//
// The findings are carried whole rather than summarized to a verdict. Six
// months later the question is not whether it passed -- the pointer already
// answers that -- but what it said, and a record holding one boolean cannot be
// re-read into an answer.
func verificationRecord(
	spec embedgen.Spec, report embedverify.Report, retrieval *embedeval.Report, at time.Time,
) embedrelease.Verification {
	record := embedrelease.Verification{
		Generation: spec.Identity().Digest,
		Passed:     report.Passed(),
		SourceRows: report.SourceRows, TargetRows: report.TargetRows,
		Unmeasured: report.Unmeasured,
		MeasuredAt: at,
	}
	for _, finding := range report.Findings {
		record.Findings = append(record.Findings, embedrelease.Finding{
			Layer: string(finding.Layer), Severity: string(finding.Severity),
			Summary: finding.Summary, Count: finding.Count,
		})
	}
	if retrieval != nil {
		record.Retrieval = &embedrelease.Retrieval{
			QueryParameters: retrieval.Scores.QueryParameters,
			RecallAtK:       retrieval.Scores.RecallAtK,
			MRR:             retrieval.Scores.MRR,
			NDCG:            retrieval.Scores.NDCG,
			ExactAgreement:  retrieval.Scores.ExactAgreement,
			Cases:           retrieval.Scores.Cases,
			ExactCases:      retrieval.Scores.ExactCases,
			Blockers:        retrieval.Blockers,
		}
	}
	return record
}

// publishRecord pushes a record and says where it went.
//
// A failure to publish is reported and does not fail the verb. The measurement
// or the pointer move already happened; failing here would report a run that did
// not do what it did, and the registry being unreachable is not a fact about the
// generation.
func publishRecord(
	ctx context.Context, out io.Writer, evidence evidenceOptions,
	record embedrelease.Record, err error,
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
	result, publishErr := sendRecord(ctx, evidence, record)
	if publishErr != nil {
		return writeLines(out, bullet(fmt.Sprintf(
			"the record was not published: %v", publishErr)))
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
	ctx context.Context, evidence evidenceOptions, record embedrelease.Record,
) (ociartifact.PushResult, error) {
	options := embedrelease.PublishOptions{
		RecordedAt: time.Now().UTC(), PlainHTTP: evidence.plainHTTP,
	}
	if evidence.attachTo != "" {
		return embedrelease.Attach(ctx, evidence.attachTo, record, options)
	}
	return embedrelease.Publish(ctx, evidence.publishTo, record, options)
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
