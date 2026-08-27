//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/ociartifact"
)

// TestInferenceReleaseEvidenceE2E publishes a generation change's evidence to a
// real registry and reads it back.
//
// A verification report that exists only in a terminal is a verification nobody
// can produce six months later, when somebody asks why a corpus was replaced.
// The registry is where Ptah's other evidence already lives, and what this test
// establishes is that these records go there and come back the same
// (stokaro/ptah#2068).
func TestInferenceReleaseEvidenceE2E(t *testing.T) {
	registry := requiredOCIRegistry(t)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	repository := fmt.Sprintf("oci://%s/ptah-inference-evidence-%d", registry, time.Now().UnixNano())

	release := assertReleaseRoundTrips(c, ctx, repository)
	assertVerificationIsFoundFromTheRelease(c, ctx, repository, release)
	assertACutoverRecordsWhatAuthorizedIt(c, ctx, repository)
}

// assertReleaseRoundTrips publishes what a generation change proposes.
func assertReleaseRoundTrips(
	c *qt.C, ctx context.Context, repository string,
) ociartifact.PushResult {
	c.Helper()
	release := embedrelease.Release{
		Generation: "gen-2", Replaces: "gen-1",
		SpecDigest: "spec-1", CorpusDigest: "corpus-1",
		Target:          "public.articles.embedding",
		Reproducibility: "partial",
		Reason:          "the provider exposes no immutable revision",
		CreatedAt:       time.Now().UTC().Truncate(time.Second),
	}
	record, err := embedrelease.NewReleaseRecord(release)
	c.Assert(err, qt.IsNil)

	result, err := embedrelease.Publish(ctx, repository+":release", record,
		embedrelease.PublishOptions{Tags: []string{"release"}, PlainHTTP: true})
	c.Assert(err, qt.IsNil)

	pulled := pullRecord(c, ctx, repository+"@"+result.Descriptor.Digest.String(),
		embedrelease.ReleaseArtifactType, embedrelease.ReleaseFileName)
	var readBack embedrelease.Release
	c.Assert(json.Unmarshal(pulled, &readBack), qt.IsNil)

	c.Assert(readBack.Generation, qt.Equals, "gen-2")
	c.Assert(readBack.Version, qt.Equals, embedrelease.RecordVersion)
	// Whether it can be rebuilt, and why not, survive the round trip. The
	// answer six months later to "can we rebuild this" is either yes or a
	// sentence, and a record carrying neither reads as yes.
	c.Assert(readBack.Reproducibility, qt.Equals, "partial")
	c.Assert(readBack.Reason, qt.Equals, "the provider exposes no immutable revision")
	c.Assert(readBack.Digest(), qt.Equals, record.Digest)
	return result
}

// assertVerificationIsFoundFromTheRelease attaches a report to the release it is
// about.
//
// Attached rather than tagged, because evidence accumulates: a generation gets
// one release and several verifications, and finding them by remembering a tag
// each is how a record goes missing.
func assertVerificationIsFoundFromTheRelease(
	c *qt.C, ctx context.Context, repository string, release ociartifact.PushResult,
) {
	c.Helper()
	verification := embedrelease.Verification{
		Generation: "gen-2", Passed: false, SourceRows: 3, TargetRows: 3,
		Findings: []embedrelease.Finding{{
			Layer: "freshness", Severity: "blocking",
			Summary: "1 target rows were computed from a source state that has since changed",
			Count:   1,
		}},
		Unmeasured: []string{"the stored vectors were not read back"},
		Retrieval: &embedrelease.Retrieval{
			CorpusDigest: "corpus-1", QueryParameters: "ivfflat.probes=1",
			RecallAtK: 0.8, MRR: 0.9, NDCG: 0.85, ExactAgreement: 0.4,
			Cases: 10, ExactCases: 10,
		},
		MeasuredAt: time.Now().UTC().Truncate(time.Second),
	}
	record, err := embedrelease.NewVerificationRecord(verification)
	c.Assert(err, qt.IsNil)

	subject := release.Descriptor
	_, err = embedrelease.Publish(ctx, repository, record,
		embedrelease.PublishOptions{Subject: &subject, PlainHTTP: true})
	c.Assert(err, qt.IsNil)

	// Found from the release rather than by a name somebody had to keep.
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	referrers, err := client.Referrers(ctx,
		repository+"@"+release.Descriptor.Digest.String(),
		embedrelease.VerificationArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(referrers, qt.HasLen, 1)
	c.Assert(referrers[0].Annotations["cz.5x5.ptah.inference.passed"], qt.Equals, "false")

	pulled := pullRecord(c, ctx, repository+"@"+referrers[0].Digest.String(),
		embedrelease.VerificationArtifactType, embedrelease.VerificationFileName)
	var readBack embedrelease.Verification
	c.Assert(json.Unmarshal(pulled, &readBack), qt.IsNil)

	// The findings come back whole. Six months later the question is not
	// whether it passed -- the pointer already answers that -- but what it
	// said, and a record holding one boolean cannot be re-read into an answer.
	c.Assert(readBack.Findings, qt.HasLen, 1)
	c.Assert(readBack.Findings[0].Summary, qt.Equals,
		"1 target rows were computed from a source state that has since changed")
	c.Assert(readBack.Unmeasured, qt.DeepEquals, []string{"the stored vectors were not read back"})
	// And the retrieval numbers keep the settings they were taken under,
	// without which they are not comparable to any others.
	c.Assert(readBack.Retrieval, qt.IsNotNil)
	c.Assert(readBack.Retrieval.QueryParameters, qt.Equals, "ivfflat.probes=1")
	c.Assert(readBack.Retrieval.ExactAgreement, qt.Equals, 0.4)
	c.Assert(readBack.Digest(), qt.Equals, record.Digest)
}

// assertACutoverRecordsWhatAuthorizedIt is the record somebody reads when they
// ask why a corpus changed.
func assertACutoverRecordsWhatAuthorizedIt(c *qt.C, ctx context.Context, repository string) {
	c.Helper()
	at := time.Now().UTC().Truncate(time.Second)
	cutover := embedrelease.Cutover{
		Generation: "gen-2", Replaced: "gen-1", Target: "public.articles",
		PlanDigest: "0123456789abcdef", Approver: "an operator",
		VerificationDigest: "fedcba9876543210",
		StabilizeUntil:     at.Add(time.Hour), CutOverAt: at,
	}
	record, err := embedrelease.NewCutoverRecord(cutover)
	c.Assert(err, qt.IsNil)

	result, err := embedrelease.Publish(ctx, repository, record, embedrelease.PublishOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)

	pulled := pullRecord(c, ctx, repository+"@"+result.Descriptor.Digest.String(),
		embedrelease.CutoverArtifactType, embedrelease.CutoverFileName)
	var readBack embedrelease.Cutover
	c.Assert(json.Unmarshal(pulled, &readBack), qt.IsNil)

	// The plan DIGEST rather than a rendered plan: an approval binds to it, so
	// a record carrying the prose could not be checked against the approval it
	// claims.
	c.Assert(readBack.PlanDigest, qt.Equals, "0123456789abcdef")
	c.Assert(readBack.Approver, qt.Equals, "an operator")
	c.Assert(readBack.VerificationDigest, qt.Equals, "fedcba9876543210")
	c.Assert(readBack.StabilizeUntil.Equal(at.Add(time.Hour)), qt.IsTrue)
	c.Assert(readBack.Digest(), qt.Equals, record.Digest)
}

// pullRecord fetches one record's body back out of a registry.
func pullRecord(c *qt.C, ctx context.Context, reference, artifactType, fileName string) []byte {
	c.Helper()
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	artifact, err := client.Pull(ctx, reference, ociartifact.PullOptions{
		ExpectedArtifactTypes: []string{artifactType},
	})
	c.Assert(err, qt.IsNil)
	body, err := fs.ReadFile(artifact.FileSystem, fileName)
	c.Assert(err, qt.IsNil, qt.Commentf("no %s in the artifact", fileName))
	return body
}

// TestInferenceEvidencePublishedByTheCLIE2E is the same records reached the way
// an operator reaches them.
//
// The test above drives the package, and a package test cannot tell a flag that
// is parsed from a flag that is used: `--publish-evidence` could be registered,
// accepted, printed in help, and dropped on the floor, and every assertion
// there would still pass. This runs the lifecycle through the cobra tree with
// the flag set and then goes to the registry to see whether anything arrived
// (stokaro/ptah#2068).
func TestInferenceEvidencePublishedByTheCLIE2E(t *testing.T) {
	registry := requiredOCIRegistry(t)
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_evidence_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()
	specPath := writeCLISpec(c, endpoint.URL)

	repository := fmt.Sprintf("oci://%s/ptah-inference-cli-%d", registry, time.Now().UnixNano())
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")

	generation := activeGenerationFrom(c, ctx, specPath, dbName)
	assertVerifyPublishesWhatItMeasured(c, ctx, specPath, dbName, repository, generation)
	assertCutoverPublishesWhatAuthorizedIt(c, ctx, specPath, dbName, repository, generation)
	assertAnUnreachableRegistryDoesNotUndoTheRun(c, ctx, specPath, dbName)
}

// assertVerifyPublishesWhatItMeasured runs the verb and then reads its record.
func assertVerifyPublishesWhatItMeasured(
	c *qt.C, ctx context.Context, specPath, dbURL, repository, generation string,
) {
	c.Helper()
	output := runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--publish-evidence", repository+":verification", "--plain-http")
	c.Assert(output, qt.Contains, "published as sha256:")

	pulled := pullRecord(c, ctx, repository+":verification",
		embedrelease.VerificationArtifactType, embedrelease.VerificationFileName)
	var record embedrelease.Verification
	c.Assert(json.Unmarshal(pulled, &record), qt.IsNil)

	c.Assert(record.Generation, qt.Equals, generation)
	c.Assert(record.Passed, qt.IsTrue)
	c.Assert(record.SourceRows > 0, qt.IsTrue)
	c.Assert(record.TargetRows, qt.Equals, record.SourceRows)
	// What this run did NOT measure is in the record too. A report that says
	// only what it checked reads as though it checked everything, and the
	// question six months later is exactly which of these numbers was measured.
	c.Assert(record.Unmeasured, qt.Not(qt.HasLen), 0)
}

// assertCutoverPublishesWhatAuthorizedIt does the same for the pointer move.
func assertCutoverPublishesWhatAuthorizedIt(
	c *qt.C, ctx context.Context, specPath, dbURL, repository, generation string,
) {
	c.Helper()
	digest := planDigestOf(c, ctx, specPath, dbURL)
	output := runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--approve", digest, "--approver", "an operator",
		"--publish-evidence", repository+":cutover", "--plain-http")
	c.Assert(output, qt.Contains, "published as sha256:")

	pulled := pullRecord(c, ctx, repository+":cutover",
		embedrelease.CutoverArtifactType, embedrelease.CutoverFileName)
	var record embedrelease.Cutover
	c.Assert(json.Unmarshal(pulled, &record), qt.IsNil)

	// The approval binds to a plan digest, so the record carries the digest the
	// operator approved rather than the prose the plan rendered. A record
	// carrying the prose could not be checked against the approval it claims.
	//
	// It carries the WHOLE digest, of which the approval an operator types is
	// the prefix: two plans sharing twelve characters are one plan to a record
	// that kept only what was typed, and the record is the artifact somebody
	// reads six months later without the plan in front of them.
	c.Assert(strings.HasPrefix(record.PlanDigest, digest), qt.IsTrue,
		qt.Commentf("plan %s does not start with the approved %s", record.PlanDigest, digest))
	c.Assert(len(record.PlanDigest), qt.Equals, 64)
	c.Assert(record.Approver, qt.Equals, "an operator")
	c.Assert(record.Generation, qt.Equals, generation)
	c.Assert(record.CutOverAt.IsZero(), qt.IsFalse)
}

// assertAnUnreachableRegistryDoesNotUndoTheRun is the other half of the
// contract, and the one worth having a fixture for.
//
// The measurement already happened when the publication is attempted. A verb
// that failed here would report a run that did not do what it did, and a
// registry nobody can reach is not a fact about the generation. It says so
// instead.
func assertAnUnreachableRegistryDoesNotUndoTheRun(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	output := runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--publish-evidence", "oci://127.0.0.1:1/ptah-nowhere:verification", "--plain-http")

	c.Assert(output, qt.Contains, "the record was not published")
	// And the verification it could not publish is still in the output, which
	// is the whole reason failing here would be wrong.
	c.Assert(output, qt.Contains, "rows")
}
