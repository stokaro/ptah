package embedrelease

import (
	"time"

	"ptah.run/internal/embedeval"
	"ptah.run/internal/embedverify"
)

// VerificationOf turns a report into the record a registry holds.
//
// The findings are carried whole rather than summarized to a verdict. Six
// months later the question is not whether it passed -- the pointer already
// answers that -- but what it said, and a record holding one boolean cannot be
// re-read into an answer.
//
// It lives here rather than in the command layer because two callers need the
// same conversion and one of them is not a command: a cutover plan cites
// [Verification.MeasurementDigest] of the report it was built on, and the
// record written afterwards has to cite the same value. Two conversions could
// answer differently about the same report, which is the shape of
// stokaro/ptah#2643 rather than a risk of repeating it.
//
// generation is the identity the report is about, passed rather than derived
// from a specification so that this package stays free of the schema types.
// retrieval may be nil, which is a verification that measured no retrieval.
// at is the instant the artifact digest covers, and the zero time is meaningful
// here: a caller that only needs the measurement digest has no instant to give.
func VerificationOf(
	generation string, report embedverify.Report, retrieval *embedeval.Report, at time.Time,
) Verification {
	record := Verification{
		Generation: generation,
		Passed:     report.Passed(),
		SourceRows: report.SourceRows, TargetRows: report.TargetRows,
		Unmeasured: report.Unmeasured,
		MeasuredAt: at,
	}
	for _, finding := range report.Findings {
		record.Findings = append(record.Findings, Finding{
			Layer: string(finding.Layer), Severity: string(finding.Severity),
			Summary: finding.Summary, Count: finding.Count,
		})
	}
	if retrieval != nil {
		record.Retrieval = &Retrieval{
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
