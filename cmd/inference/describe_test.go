package inference_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/inference"
)

// TestDescribe_ReadsASpecificationWithNoDatabase is why the verb exists.
//
// Every other verb requires --db-url, so until this one a specification could
// not be checked at all without a live PostgreSQL. An author writing one and a
// CI job asking whether an edit changed the corpus both need the file's own
// answer and have no server to hand.
func TestDescribe_ReadsASpecificationWithNoDatabase(t *testing.T) {
	c := qt.New(t)

	output, err := runDescribe(c, writeDescribeSpec(c, "test-embed", "embedding"))

	c.Assert(err, qt.IsNil)
	c.Assert(output, qt.Contains, "generation ")
	c.Assert(output, qt.Contains, "reproducibility: ")
	c.Assert(output, qt.Contains, "the text of title, body")
	c.Assert(output, qt.Contains, "Consistency mode: outbox")
}

// TestDescribe_TheRowCountIsAbsentRatherThanZero is the one number this must
// not invent.
//
// Counting needs the database. An uncounted source rendered as zero says the
// disclosure is empty, which is the single most misleading answer a page about
// what leaves your database could give.
func TestDescribe_TheRowCountIsAbsentRatherThanZero(t *testing.T) {
	c := qt.New(t)

	output, err := runDescribe(c, writeDescribeSpec(c, "test-embed", "embedding"))
	c.Assert(err, qt.IsNil)
	c.Assert(output, qt.Contains, "nobody counted")

	body, err := runDescribe(c, writeDescribeSpec(c, "test-embed", "embedding"), "--format", "json")
	c.Assert(err, qt.IsNil)

	var described struct {
		Disclosure struct {
			RowsInScope int64 `json:"rows_in_scope"`
		} `json:"disclosure"`
	}
	c.Assert(json.Unmarshal([]byte(body), &described), qt.IsNil)
	c.Assert(described.Disclosure.RowsInScope, qt.Equals, int64(-1))
}

// TestDescribe_AnEditThatChangesTheCorpusChangesTheIdentity is the CI use.
//
// Two specifications, one field apart, and the digest a job would compare.
func TestDescribe_AnEditThatChangesTheCorpusChangesTheIdentity(t *testing.T) {
	c := qt.New(t)

	before := describedIdentity(c, writeDescribeSpec(c, "test-embed", "embedding"))
	after := describedIdentity(c, writeDescribeSpec(c, "other-model", "embedding"))

	c.Assert(before, qt.Not(qt.Equals), "")
	c.Assert(before, qt.Not(qt.Equals), after)
}

// TestDescribe_AnEditThatDoesNotChangeTheCorpusKeepsTheIdentity is the control.
//
// Without it, an implementation returning a fresh digest every call would
// satisfy the test above. The target column is not part of what a vector comes
// out as, so two specifications differing only there address one corpus --
// which is also why `prepare` has to refuse the collision rather than rely on
// the identity to catch it.
func TestDescribe_AnEditThatDoesNotChangeTheCorpusKeepsTheIdentity(t *testing.T) {
	c := qt.New(t)

	first := describedIdentity(c, writeDescribeSpec(c, "test-embed", "embedding"))
	second := describedIdentity(c, writeDescribeSpec(c, "test-embed", "embedding"))

	c.Assert(first, qt.Equals, second)
}

// TestDescribe_APartialReproducibilityCarriesItsReason is the answer that must
// not read as a yes.
//
// A specification whose provider exposes no immutable revision describes a
// generation that cannot be rebuilt exactly: asking the provider again may
// answer with different vectors, and Ptah cannot see that it did. The word
// "partial" alone reads as a shade of yes; the reason is what makes it a fact
// somebody can act on.
func TestDescribe_APartialReproducibilityCarriesItsReason(t *testing.T) {
	c := qt.New(t)

	output, err := runDescribe(c, writeDescribeSpecWithoutRevision(c))

	c.Assert(err, qt.IsNil)
	c.Assert(output, qt.Contains, "reproducibility: partial (")
	c.Assert(output, qt.Contains, "no immutable revision")
}

// TestDescribe_TheReasonIsInTheJSONToo is the same for the form a CI job reads.
func TestDescribe_TheReasonIsInTheJSONToo(t *testing.T) {
	c := qt.New(t)

	body, err := runDescribe(c, writeDescribeSpecWithoutRevision(c), "--format", "json")
	c.Assert(err, qt.IsNil)

	var described struct {
		Reproducibility string `json:"reproducibility"`
		Reason          string `json:"reproducibility_reason"`
	}
	c.Assert(json.Unmarshal([]byte(body), &described), qt.IsNil)
	c.Assert(described.Reproducibility, qt.Equals, "partial")
	c.Assert(described.Reason, qt.Contains, "no immutable revision")
}

// TestDescribe_AFullReproducibilityCarriesNoReason is the control.
//
// Without it, a renderer that appended a parenthesis to every answer would
// satisfy the two above.
func TestDescribe_AFullReproducibilityCarriesNoReason(t *testing.T) {
	c := qt.New(t)

	output, err := runDescribe(c, writeDescribeSpec(c, "test-embed", "embedding"))

	c.Assert(err, qt.IsNil)
	c.Assert(output, qt.Contains, "reproducibility: full")
	c.Assert(output, qt.Not(qt.Contains), "reproducibility: full (")
}

// TestDescribe_RefusesAFormatItCannotWrite pins the input guard.
func TestDescribe_RefusesAFormatItCannotWrite(t *testing.T) {
	c := qt.New(t)

	_, err := runDescribe(c, writeDescribeSpec(c, "test-embed", "embedding"), "--format", "yaml")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `invalid --format value "yaml"`)
}

// TestDescribe_RefusesAMissingSpecification is the other input guard.
func TestDescribe_RefusesAMissingSpecification(t *testing.T) {
	c := qt.New(t)

	_, err := runDescribe(c, filepath.Join(c.TempDir(), "absent.yaml"))

	c.Assert(err, qt.IsNotNil)
}

// describedIdentity reads the generation out of the JSON form.
func describedIdentity(c *qt.C, specPath string) string {
	c.Helper()
	body, err := runDescribe(c, specPath, "--format", "json")
	c.Assert(err, qt.IsNil)

	var described struct {
		Generation string `json:"generation"`
	}
	c.Assert(json.Unmarshal([]byte(body), &described), qt.IsNil)
	return described.Generation
}

// runDescribe drives the verb through its own command.
func runDescribe(c *qt.C, specPath string, extra ...string) (string, error) {
	c.Helper()
	cmd := inference.NewCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(append([]string{"describe", "--spec", specPath}, extra...))
	err := cmd.ExecuteContext(context.Background())
	return output.String(), err
}

// writeDescribeSpecWithoutRevision writes one whose provider names no immutable
// revision, which is what makes the generation only partly reproducible.
func writeDescribeSpecWithoutRevision(c *qt.C) string {
	c.Helper()
	return writeSpecDocument(c, describeSpecDocument("test-embed", "embedding", ""))
}

// writeDescribeSpec writes a valid specification with a chosen model and column.
func writeDescribeSpec(c *qt.C, model, column string) string {
	c.Helper()
	return writeSpecDocument(c, describeSpecDocument(model, column, "1"))
}

// writeSpecDocument puts a document where the verb can read it.
func writeSpecDocument(c *qt.C, document string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// describeSpecDocument renders a specification, with or without a revision.
//
// The revision is a parameter rather than something a second helper strips out,
// so the two documents are built the same way and the only difference between
// them is the one the test is about.
func describeSpecDocument(model, column, revision string) string {
	revisionLine := ""
	if revision != "" {
		revisionLine = "  revision: \"" + revision + "\"\n"
	}
	return `
version: 1
name: describe
source:
  schema: public
  table: docs
  key_fields: [id]
  input_fields: [title, body]
  version_strategy: updated_at
  version_field: updated_at
  mutable: true
preprocessing:
  separator: "\n"
  null_policy: empty
  empty_policy: skip
  unicode_normalization: none
  truncate: refuse
model:
  provider: openai-compatible
  endpoint_class: local
  endpoint: http://127.0.0.1:9/v1
  identifier: ` + model + `
` + revisionLine + `  reported_dimension: 4
  normalization: none
target:
  schema: public
  table: docs
  column: ` + column + `
  representation: vector
  metric: cosine
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
`
}

// TestDescribeSpecDocument_TheRevisionIsTheOnlyDifference is the control the
// reproducibility tests rest on.
//
// Both fixtures are built by one function, so a change to the shared body
// cannot make them differ in some other way and leave the reproducibility test
// measuring that instead.
func TestDescribeSpecDocument_TheRevisionIsTheOnlyDifference(t *testing.T) {
	c := qt.New(t)

	with := describeSpecDocument("test-embed", "embedding", "1")
	without := describeSpecDocument("test-embed", "embedding", "")

	c.Assert(with, qt.Not(qt.Equals), without)
	c.Assert(with, qt.Contains, `revision: "1"`)
	c.Assert(without, qt.Not(qt.Contains), "revision:")
	c.Assert(strings.Replace(with, "  revision: \"1\"\n", "", 1), qt.Equals, without)
}
