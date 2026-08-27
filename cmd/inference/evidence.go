package inference

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/embedeval"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/embedverify"
)

// evidenceOptions are the flags every verb that can leave a record takes.
type evidenceOptions struct {
	// publishTo is the OCI reference the record is pushed to, empty for none.
	publishTo string
	// plainHTTP permits an unencrypted connection to that registry.
	plainHTTP bool
}

// addEvidenceFlags registers them.
func addEvidenceFlags(flags *pflag.FlagSet, options *evidenceOptions) {
	flags.StringVar(&options.publishTo, "publish-evidence", "",
		"OCI reference to publish this run's record to; omitted keeps it out of a registry")
	dbcli.RegisterPlainHTTPFlag(flags, &options.plainHTTP)
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
	if evidence.publishTo == "" {
		return nil
	}
	result, publishErr := embedrelease.Publish(ctx, evidence.publishTo, record,
		embedrelease.PublishOptions{RecordedAt: time.Now().UTC(), PlainHTTP: evidence.plainHTTP})
	if publishErr != nil {
		return writeLines(out, bullet(fmt.Sprintf(
			"the record was not published: %v", publishErr)))
	}
	return writeLines(out, bullet(fmt.Sprintf(
		"record %s published as %s", record.Digest[:12], result.Descriptor.Digest)))
}
