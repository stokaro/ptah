package inference

// White-box testing required: the property under test is that one invocation
// resolves one specification however many times a verb asks for it, and every
// verb that asks more than once needs a live PostgreSQL to get that far. The
// exported surface offers no second ask without a server, so a black-box test
// would either need one or would exercise the single-ask path and assert
// nothing about the memory.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSpecSource_OneInvocationRunsOneSpecification is why the resolution is
// remembered rather than repeated.
//
// A verb resolves more than once: a cutover verifies, reads the pointer, then
// advances the phase, and each of those opens the database. Against a file that
// is three reads of the same bytes. Against a MUTABLE OCI reference it is three
// chances to be handed a different specification, and a cutover carried out
// against two of them is one that no record describes.
//
// Driven through the file, because the file and the reference share the one
// resolution path and a file can be changed between two asks without a
// registry.
func TestSpecSource_OneInvocationRunsOneSpecification(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "spec.yaml")
	writeSpec(c, path, describeSpecDocument("test-embed", "embedding", "1"))
	source := &specSource{path: path}

	first, err := source.resolve(context.Background())
	c.Assert(err, qt.IsNil)
	writeSpec(c, path, describeSpecDocument("other-model", "embedding", "1"))
	second, err := source.resolve(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(second.Digest, qt.Equals, first.Digest)
	c.Assert(second.Spec.Identity().Digest, qt.Equals, first.Spec.Identity().Digest)
}

// TestSpecSource_ASecondInvocationSeesTheChange is the control.
//
// Without it, a resolution that cached across invocations -- in a package-level
// map, say -- would satisfy the test above while making a long-lived process
// blind to an edited specification forever. The memory belongs to one
// invocation.
func TestSpecSource_ASecondInvocationSeesTheChange(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "spec.yaml")
	writeSpec(c, path, describeSpecDocument("test-embed", "embedding", "1"))

	first, err := (&specSource{path: path}).resolve(context.Background())
	c.Assert(err, qt.IsNil)
	writeSpec(c, path, describeSpecDocument("other-model", "embedding", "1"))
	second, err := (&specSource{path: path}).resolve(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(second.Digest, qt.Not(qt.Equals), first.Digest)
}

// writeSpec puts a document where a source can read it.
func writeSpec(c *qt.C, path, document string) {
	c.Helper()
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
}

// describeSpecDocument renders a specification whose model is the one thing
// that varies.
//
// The model is part of what a vector comes out as, so two documents differing
// there are two generations -- which is what makes "the second resolution
// answered with the first document" a statement about the memory rather than
// about two files that happened to agree.
func describeSpecDocument(model, column, revision string) string {
	return `
version: 1
name: promote
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
  revision: "` + revision + `"
  reported_dimension: 4
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
