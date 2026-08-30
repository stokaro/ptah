package inference_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/inference"
	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/embedspec"
	"go.5x5.cz/ptah/internal/ociartifact"
)

// TestDescribe_RunsAPublishedReleaseInsteadOfAFile is the receiving half of a
// promotion.
//
// The environment that runs a release has never seen the operator's file. It
// addresses the release, and what it runs is the document the release carried --
// which is the difference between promoting one specification through three
// environments and copying it into three repositories.
//
// Driven through `describe` because that verb needs no database, so the
// assertion is about the promotion and not about a server being up.
func TestDescribe_RunsAPublishedReleaseInsteadOfAFile(t *testing.T) {
	c := qt.New(t)
	specPath := writeDescribeSpec(c, "test-embed", "embedding")
	fromFile := describedIdentity(c, specPath)
	reference := publishReleaseOf(c, specPath)

	body, stderr, err := runInferenceCommand(c, "describe", "--release", reference, "--format", "json")

	c.Assert(err, qt.IsNil)
	var described struct {
		Generation string `json:"generation"`
	}
	c.Assert(json.Unmarshal([]byte(body), &described), qt.IsNil)
	// The same generation as the file it was published from. A promotion that
	// produced a different identity would recompute a corpus already correct.
	c.Assert(described.Generation, qt.Equals, fromFile)
	// What the reference resolved to is reported, because a tag moves and a
	// record keeping only the tag says two environments agreed without
	// establishing that they did.
	c.Assert(stderr, qt.Contains, "resolved to sha256:")
	c.Assert(stderr, qt.Contains, described.Generation)
}

// TestDescribe_TheAnswerCarriesNoTraceOfWhereItCameFrom is the control for the
// line above, and the reason that line is on the error stream.
//
// `describe --format json` is what a CI job diffs between two commits to decide
// whether a corpus has to be recomputed. A document that also carried its own
// provenance would differ on every promotion for a reason that changes no
// vector, and the job would rebuild a corpus that is already right.
func TestDescribe_TheAnswerCarriesNoTraceOfWhereItCameFrom(t *testing.T) {
	c := qt.New(t)
	specPath := writeDescribeSpec(c, "test-embed", "embedding")
	reference := publishReleaseOf(c, specPath)

	fromFile, _, err := runInferenceCommand(c, "describe", "--spec", specPath, "--format", "json")
	c.Assert(err, qt.IsNil)
	fromRelease, _, err := runInferenceCommand(c, "describe", "--release", reference, "--format", "json")
	c.Assert(err, qt.IsNil)

	c.Assert(fromRelease, qt.Equals, fromFile)
}

// TestDescribe_RefusesASpecificationTheReleaseDoesNotDescribe is what makes a
// release one artifact rather than two that agree by convention.
//
// Pulling by digest establishes that the artifact is the one the reference
// named. It cannot establish that the artifact agrees with itself, and a
// release whose record claims one specification while its layer carries another
// is what would let an approval, a cutover record and a verification all name a
// document nobody ran.
func TestDescribe_RefusesASpecificationTheReleaseDoesNotDescribe(t *testing.T) {
	c := qt.New(t)
	declared := writeDescribeSpec(c, "test-embed", "embedding")
	substituted := writeDescribeSpec(c, "other-model", "embedding")
	reference := publishReleaseWithBody(c, declared, readFile(c, substituted))

	_, _, err := runInferenceCommand(c, "describe", "--release", reference)

	c.Assert(err, qt.ErrorIs, embedspec.ErrDocumentMismatch)
}

// TestDescribe_RefusesBothASpecificationAndARelease is the input guard.
//
// A run given both has been told twice which corpus to rebuild. Silently
// preferring either is how an environment promotes a digest and embeds the file
// that happened to be in its working directory.
func TestDescribe_RefusesBothASpecificationAndARelease(t *testing.T) {
	c := qt.New(t)
	specPath := writeDescribeSpec(c, "test-embed", "embedding")

	_, _, err := runInferenceCommand(c, "describe",
		"--spec", specPath, "--release", publishReleaseOf(c, specPath))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "[release spec] were all set")
}

// TestDescribe_RefusesNeitherASpecificationNorARelease is the other half.
func TestDescribe_RefusesNeitherASpecificationNorARelease(t *testing.T) {
	c := qt.New(t)

	_, _, err := runInferenceCommand(c, "describe")

	c.Assert(err, qt.ErrorMatches, `--spec or --release is required`)
}

// publishReleaseOf puts a specification into a release the way `plan` does.
func publishReleaseOf(c *qt.C, specPath string) string {
	c.Helper()
	return publishReleaseWithBody(c, specPath, readFile(c, specPath))
}

// publishReleaseWithBody publishes a release describing one document and
// carrying another, which is how the substitution case is built.
//
// The two are one function with two arguments rather than two functions, so the
// agreeing case and the disagreeing one are built by the same code and the
// refusal test cannot pass because its fixture was assembled differently.
func publishReleaseWithBody(c *qt.C, describes string, carries []byte) string {
	c.Helper()
	declared, err := embedspec.Load(describes)
	c.Assert(err, qt.IsNil)

	record, err := embedrelease.NewReleaseRecord(embedrelease.Release{
		Generation: declared.Spec.Identity().Digest,
		SpecDigest: declared.Digest,
		Target:     declared.Spec.Target.Table,
		CreatedAt:  time.Now().UTC(),
	}, carries)
	c.Assert(err, qt.IsNil)

	reference := ociartifact.LayoutScheme + c.TempDir() + "/release:v1"
	target, tag, err := ociartifact.OpenLayout(reference)
	c.Assert(err, qt.IsNil)
	archive := fstest.MapFS{record.FileName: &fstest.MapFile{Data: record.Body}}
	for name, body := range record.Files {
		archive[name] = &fstest.MapFile{Data: body}
	}
	_, err = ociartifact.PushTo(c.Context(), target, archive, ociartifact.PushOptions{
		ArtifactType: record.ArtifactType, Tags: []string{tag},
	})
	c.Assert(err, qt.IsNil)
	return reference
}

// readFile is the document as published.
func readFile(c *qt.C, path string) []byte {
	c.Helper()
	body, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	return body
}

// runInferenceCommand drives a verb with the two streams kept apart.
//
// Apart, because the whole point of the resolution notice is that it is not in
// the answer: a helper merging them would make the control above pass whatever
// stream the line went to.
func runInferenceCommand(c *qt.C, args ...string) (stdout, stderr string, err error) {
	c.Helper()
	cmd := inference.NewCommand()
	var out, errs bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errs)
	cmd.SetArgs(args)
	err = cmd.ExecuteContext(context.Background())
	return out.String(), errs.String(), err
}

// TestOpen_AnArgumentErrorCostsNoRegistryRoundTrip pins the order of the
// checks.
//
// Resolving a --release reaches a registry. A run that was going to be refused
// for naming no database should not spend a network round trip finding that
// out, and the reference here names an address nothing answers on -- so a
// resolution that happened first would report that instead, and slowly.
func TestOpen_AnArgumentErrorCostsNoRegistryRoundTrip(t *testing.T) {
	c := qt.New(t)

	_, _, err := runInferenceCommand(c, "status",
		"--release", "oci://127.0.0.1:1/nothing-listens-here:v1", "--run-id", "r")

	c.Assert(err, qt.ErrorMatches, `--db-url is required`)
}
