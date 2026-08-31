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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

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

// publishedSpecification stands in for the document a release carries.
//
// This test drives the package, which publishes and reads bytes and never
// parses them. The CLI test below carries a real specification through the same
// path.
const publishedSpecification = "version: 1\nname: articles v2\n"

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
	record, err := embedrelease.NewReleaseRecord(release, []byte(publishedSpecification))
	c.Assert(err, qt.IsNil)

	result, err := embedrelease.Publish(ctx, repository+":release", record,
		embedrelease.PublishOptions{Tags: []string{"release"}, PlainHTTP: true})
	c.Assert(err, qt.IsNil)

	pulled := pullRecord(c, ctx, repository+"@"+result.Descriptor.Digest.String(),
		embedrelease.ReleaseArtifactType, embedrelease.ReleaseFileName)
	var readBack embedrelease.Release
	c.Assert(json.Unmarshal(pulled, &readBack), qt.IsNil)

	// The document the change was built from travelled with the record, which
	// is what lets another environment run this release rather than be told
	// the digest of a file it has never seen.
	carried := pullRecord(c, ctx, repository+"@"+result.Descriptor.Digest.String(),
		embedrelease.ReleaseArtifactType, embedrelease.SpecificationFileName)
	c.Assert(string(carried), qt.Equals, publishedSpecification)

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
	assertPlanLeavesNoRecordUnasked(c, ctx, specPath, dbName)
	assertPlanWritesTheReleaseToAFile(c, ctx, specPath, dbName, generation)
	assertPlanPublishesTheRelease(c, ctx, specPath, dbName, repository, generation)
	assertAPublishedReleaseRunsWithoutTheFile(c, ctx, dbName, repository, generation)
	assertVerifyIsFoundFromTheReleaseItIsAbout(c, ctx, specPath, dbName, repository)
	assertVerifyPublishesWhatItMeasured(c, ctx, specPath, dbName, repository, generation)
	assertCutoverPublishesWhatAuthorizedIt(c, ctx, specPath, dbName, repository, generation)
	assertAnUnreachableRegistryDoesNotUndoTheRun(c, ctx, specPath, dbName)
	assertAVerificationIsKeptWithoutARegistry(c, ctx, specPath, dbName)
	assertAnUnwritableEvidenceFileDoesNotUndoTheRun(c, ctx, specPath, dbName)
}

// assertPlanLeavesNoRecordUnasked is the control for the three below.
//
// plan is the verb an operator runs to look, and it stays one. Publishing on
// every run would put a release in a registry for every question anybody asked
// of a specification, and the assertions that follow would pass either way.
func assertPlanLeavesNoRecordUnasked(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	output := runInference(c, ctx, "plan", "--spec", specPath, "--db-url", dbURL)

	c.Assert(output, qt.Contains, "generation ")
	// Every line either destination can produce says "the record" or "record
	// <digest>", including the two that report a FAILURE to keep it. Asserting
	// only the successes left a plan that published unasked and could not reach
	// the registry reading exactly like one that published nothing. The plan's
	// own prose says "record" too -- a step recording a starting position -- so
	// the assertion is on the two shapes rather than on the word.
	c.Assert(output, qt.Not(qt.Contains), "the record was")
	c.Assert(output, qt.Not(qt.Contains), "published as")
	c.Assert(output, qt.Not(qt.Contains), "written to")
}

// assertPlanWritesTheReleaseToAFile is the half an operator with no registry
// gets, and it is the same bytes the registry would have held.
func assertPlanWritesTheReleaseToAFile(
	c *qt.C, ctx context.Context, specPath, dbURL, generation string,
) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "release.json")

	output := runInference(c, ctx, "plan",
		"--spec", specPath, "--db-url", dbURL, "--evidence-file", path)
	c.Assert(output, qt.Contains, "written to "+path)

	body, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	var record embedrelease.Release
	c.Assert(json.Unmarshal(body, &record), qt.IsNil)

	c.Assert(record.Generation, qt.Equals, generation)
	c.Assert(record.Version, qt.Equals, embedrelease.RecordVersion)
	// The document that proposed the change, which is not the generation: two
	// files that differ only in a name address one generation, and a reader
	// asking which file this came from has nothing else to go on.
	c.Assert(record.SpecDigest, qt.HasLen, 64)
	c.Assert(record.SpecDigest, qt.Not(qt.Equals), record.Generation)
	c.Assert(record.Target, qt.Equals, "public.articles.embedding")
	// Whether it can be rebuilt, and why not. A record carrying neither reads
	// as yes.
	c.Assert(record.Reproducibility, qt.Not(qt.Equals), "")
}

// assertPlanPublishesTheRelease is stokaro/ptah#2475: the record existed, and
// no verb produced one, so every verification published stood alone.
func assertPlanPublishesTheRelease(
	c *qt.C, ctx context.Context, specPath, dbURL, repository, generation string,
) {
	c.Helper()
	output := runInference(c, ctx, "plan",
		"--spec", specPath, "--db-url", dbURL,
		"--publish-evidence", repository+":release", "--plain-http")
	c.Assert(output, qt.Contains, "published as sha256:")

	pulled := pullRecord(c, ctx, repository+":release",
		embedrelease.ReleaseArtifactType, embedrelease.ReleaseFileName)
	var record embedrelease.Release
	c.Assert(json.Unmarshal(pulled, &record), qt.IsNil)
	c.Assert(record.Generation, qt.Equals, generation)
	c.Assert(record.Digest(), qt.HasLen, 64)
}

// assertAPublishedReleaseRunsWithoutTheFile is the promotion the epic asks for:
// one immutable release specification, promoted by digest through development,
// staging and production, each environment producing its own run against the
// same release identity.
//
// The environment receiving it has never seen the operator's file, which is why
// the release carries the document rather than its digest. This runs the verb
// with no --spec at all, and the file is on this machine only because the same
// process published it a moment ago.
func assertAPublishedReleaseRunsWithoutTheFile(
	c *qt.C, ctx context.Context, dbURL, repository, generation string,
) {
	c.Helper()
	output := runInference(c, ctx, "plan",
		"--release", repository+":release", "--db-url", dbURL, "--plain-http")

	// The same generation the file produced. A promotion answering with a
	// different identity would recompute a corpus that is already correct.
	c.Assert(output, qt.Contains, "generation "+generation)
	// And it reached the database: the plan measured what only a server can
	// answer, so this is the whole verb rather than a specification being read.
	c.Assert(output, qt.Contains, "for 3 rows")
}

// assertVerifyIsFoundFromTheReleaseItIsAbout closes the chain the issue named.
//
// Attached rather than tagged: a generation gets one release and several
// verifications, and finding them by remembering a tag each is how a record
// goes missing. The package-level test above proves the mechanism; this proves
// the verbs reach it, which is the half that was absent.
func assertVerifyIsFoundFromTheReleaseItIsAbout(
	c *qt.C, ctx context.Context, specPath, dbURL, repository string,
) {
	c.Helper()
	output := runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--attach-to", repository+":release", "--plain-http")
	c.Assert(output, qt.Contains, "published as sha256:")

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	referrers, err := client.Referrers(ctx, repository+":release",
		embedrelease.VerificationArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(referrers, qt.HasLen, 1)
	c.Assert(referrers[0].Annotations["cz.5x5.ptah.inference.passed"], qt.Equals, "true")
}

// assertVerifyPublishesWhatItMeasured runs the verb and then reads its record.
//
// It is also the control for the attachment above: a verification with no
// release to attach to still lands, addressed by its own digest. That is the
// record somebody wants most, and a subject that had become required would have
// taken it away from every operator who never ran plan against a registry.
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
	c.Assert(record.PlanDigest, qt.HasLen, 64)
	c.Assert(record.Approver, qt.Equals, "an operator")
	c.Assert(record.Generation, qt.Equals, generation)
	c.Assert(record.CutOverAt.IsZero(), qt.IsFalse)
	// This run typed a name beside a digest, so the record says the approval
	// was not signed. It is the negative half of stokaro/ptah#2643 finding 4,
	// and it is the half that matters: a record that always said "signed"
	// would satisfy the positive assertion in the signed-approval suite while
	// making a typed name indistinguishable from a key -- which is the defect,
	// not the fix. The positive is
	// TestInferenceSignedApprovalE2E's assertASignedPlanCutsOverAndRecordsThePrincipal.
	c.Assert(record.ApprovalSigned, qt.IsFalse)
	// The measurement the plan cited, not the generation identity and not a
	// report restamped at this instant (stokaro/ptah#2643 findings 1 to 3).
	c.Assert(record.VerificationDigest, qt.Not(qt.Equals), record.Generation)
	c.Assert(record.VerificationDigest, qt.HasLen, 64)
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

// assertAVerificationIsKeptWithoutARegistry is the destination an operator has
// when they have no registry.
//
// A first migration, a CI job that runs before anything is published, a team
// with no registry at all: the record is the same bytes either way, so what is
// kept locally is what would have been fetched.
func assertAVerificationIsKeptWithoutARegistry(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "verification.json")

	output := runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--evidence-file", path)

	c.Assert(output, qt.Contains, "written to "+path)
	// No registry was named, and the record still exists. Guarding the record
	// on the registry alone made this flag do nothing without one.
	body, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	var record embedrelease.Verification
	c.Assert(json.Unmarshal(body, &record), qt.IsNil)
	c.Assert(record.Passed, qt.IsTrue)
	c.Assert(record.SourceRows > 0, qt.IsTrue)
	// What the run did NOT measure is in the file too, which is the half of the
	// epic's goal a pass/fail line cannot carry.
	c.Assert(record.Unmeasured, qt.Not(qt.HasLen), 0)

	// Written for the operator rather than offered to the filesystem.
	info, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o600))
}

// assertAnUnwritableEvidenceFileDoesNotUndoTheRun is the same contract the
// registry half carries.
//
// The measurement already happened when the write is attempted. A directory
// that is not there is not a fact about the generation, and failing here would
// report a run that did not do what it did.
func assertAnUnwritableEvidenceFileDoesNotUndoTheRun(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "no-such-directory", "verification.json")

	output := runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--evidence-file", path)

	c.Assert(output, qt.Contains, "the record was not written")
	// And the verification it could not write is still in the output, which is
	// the whole reason failing here would be wrong.
	c.Assert(output, qt.Contains, "rows")
}
