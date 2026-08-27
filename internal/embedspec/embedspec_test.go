package embedspec_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedspec"
)

// complete is a specification file with every field an operator would write.
const complete = `
version: 1
name: articles v2
description: the second generation over the article corpus
source:
  schema: public
  table: articles
  filter: '"published"'
  key_fields: [id]
  input_fields: [title, body]
  version_strategy: updated_at
  version_field: updated_at
  mutable: true
preprocessing:
  separator: "\n"
  prefix: "search_document: "
  null_policy: empty
  empty_policy: skip
  unicode_normalization: nfc
  collapse_whitespace: true
  max_input_bytes: 8000
  truncate: bytes
model:
  provider: openai-compatible
  endpoint_class: local
  endpoint: http://localhost:11434/v1
  identifier: nomic-embed-text
  revision: "1.5"
  requested_dimension: 768
  reported_dimension: 768
  normalization: none
  pooling: mean
  credential: env:PTAH_EMBED_TOKEN
target:
  schema: public
  table: articles
  column: embedding_v2
  representation: vector
  metric: cosine
  index_method: hnsw
  index_options:
    m: "16"
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
  max_plan_age: 30m
`

// TestParse_ReadsACompleteSpecification is the control.
func TestParse_ReadsACompleteSpecification(t *testing.T) {
	c := qt.New(t)

	loaded, err := embedspec.Parse([]byte(complete), "spec.yaml")

	c.Assert(err, qt.IsNil)
	c.Assert(loaded.Spec.Source.Table, qt.Equals, "articles")
	c.Assert(loaded.Spec.Source.InputFields, qt.DeepEquals, []string{"title", "body"})
	c.Assert(loaded.Spec.Preprocessing.NullPolicy, qt.Equals, embedgen.NullAsEmpty)
	c.Assert(loaded.Spec.Preprocessing.UnicodeNormalization, qt.Equals, embedgen.UnicodeNFC)
	c.Assert(loaded.Spec.Model.ReportedDimension, qt.Equals, 768)
	c.Assert(loaded.Spec.Target.Metric, qt.Equals, embedgen.MetricCosine)
	c.Assert(loaded.Spec.Target.IndexOptions, qt.DeepEquals, map[string]string{"m": "16"})
	c.Assert(loaded.Mode, qt.Equals, embedcatchup.ModeOutbox)
	c.Assert(loaded.Source.Mutable, qt.IsTrue)
	c.Assert(loaded.Policy.MaxPlanAge, qt.Equals, 30*time.Minute)
	c.Assert(loaded.Credential, qt.Equals, "env:PTAH_EMBED_TOKEN")
	c.Assert(loaded.Spec.Identity().Digest, qt.Not(qt.Equals), "")
}

// TestParse_RefusesAFieldItDoesNotKnow is why unknown fields are an error.
//
// `input_field` for `input_fields` produces a valid specification for a
// different generation, and the operator's evidence that it worked is that it
// ran.
func TestParse_RefusesAFieldItDoesNotKnow(t *testing.T) {
	c := qt.New(t)
	document := `
version: 1
source:
  table: articles
  key_fields: [id]
  input_field: [title]
  mutable: false
`

	_, err := embedspec.Parse([]byte(document), "spec.yaml")

	c.Assert(err, qt.ErrorMatches, `(?s)parse spec.yaml: .*field input_field not found.*`)
}

// TestParse_RefusesAnUnstatedMutability is the field with no safe default.
//
// A live table planned as a frozen one skips every change made while the
// backfill runs, and the plan says nothing about it because nothing asked.
func TestParse_RefusesAnUnstatedMutability(t *testing.T) {
	c := qt.New(t)
	document := `
version: 1
source:
  table: articles
  key_fields: [id]
  input_fields: [title]
`

	_, err := embedspec.Parse([]byte(document), "spec.yaml")

	c.Assert(err, qt.ErrorMatches,
		`spec.yaml does not say whether the source is mutable; a live table planned as a frozen `+
			`one skips every change made while the backfill runs`)
}

// TestParse_RefusesAnEnumeratedValueThisBuildDoesNotAct walks the policies.
//
// A value passed through would become part of a content address over a word
// nothing reads, and the first sign would be vectors that do not match what the
// file describes.
func TestParse_RefusesAnEnumeratedValueThisBuildDoesNotAct(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name: "a null policy", document: withField("null_policy: interpolate"),
			want: `spec.yaml: preprocessing.null_policy "interpolate" is not one this build acts ` +
				`on; it has empty, skip, refuse`,
		},
		{
			name: "a distance metric", document: withField("metric: manhattan"),
			want: `spec.yaml: target.metric "manhattan" is not one this build acts on; it has ` +
				`cosine, l2, inner_product`,
		},
		{
			name: "a consistency mode", document: withField("mode: debezium"),
			want: `spec.yaml: unknown consistency mode "debezium"; this build has ` +
				`\[immutable dual_write outbox\]`,
		},
		{
			name:     "a credential that is a value rather than a reference",
			document: withField("credential: not-a-reference"),
			want: `spec.yaml: unsupported credential reference scheme: ` +
				`"not-a-reference" is not scheme:locator, such as env:NAME or file:/path`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := embedspec.Parse([]byte(test.document), "spec.yaml")

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestParse_RefusesAFormatVersionItDoesNotRead keeps a file written for a later
// build from being read as one written for this one.
func TestParse_RefusesAFormatVersionItDoesNotRead(t *testing.T) {
	c := qt.New(t)

	_, err := embedspec.Parse([]byte("version: 99\n"), "spec.yaml")

	c.Assert(err, qt.ErrorMatches, `spec.yaml declares format version 99 and this build reads 1`)
}

// TestParse_TheIdentityFollowsTheFile is what makes the whole format load-bearing.
//
// Two files that differ in something the identity is taken over are two
// generations, and two that differ only in a display name are one.
func TestParse_TheIdentityFollowsTheFile(t *testing.T) {
	tests := []struct {
		name string
		edit func(string) string
		same bool
	}{
		{
			name: "a different separator",
			edit: func(s string) string { return replaceOnce(s, `separator: "\n"`, `separator: " "`) },
			same: false,
		},
		{
			name: "a different input field order",
			edit: func(s string) string {
				return replaceOnce(s, "input_fields: [title, body]", "input_fields: [body, title]")
			},
			same: false,
		},
		{
			name: "a different name",
			edit: func(s string) string { return replaceOnce(s, "name: articles v2", "name: whatever") },
			same: true,
		},
		{
			name: "different index options",
			edit: func(s string) string { return replaceOnce(s, `m: "16"`, `m: "64"`) },
			same: true,
		},
	}
	base := mustParse(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			edited, err := embedspec.Parse([]byte(test.edit(complete)), "spec.yaml")

			c.Assert(err, qt.IsNil)
			c.Assert(edited.Spec.Identity().Digest == base.Spec.Identity().Digest, qt.Equals, test.same)
		})
	}
}

// mustParse reads the control specification, which every row above is an edit
// of.
//
// It lives here rather than in the test body because a test asserts and does not
// branch, which is the rule scripts/check-test-style.sh enforces.
func mustParse(t *testing.T) embedspec.Loaded {
	t.Helper()
	loaded, err := embedspec.Parse([]byte(complete), "spec.yaml")
	if err != nil {
		t.Fatalf("the control specification does not parse: %v", err)
	}
	return loaded
}

// withField returns the complete document with one line replaced.
//
// #nosec G101 -- the map's values are field names paired with the valid line
// each row replaces, and "credential: env:PTAH_EMBED_TOKEN" is a REFERENCE to
// where a token lives. That is the whole point of the row it belongs to.
func withField(line string) string {
	replacements := map[string]string{
		"null_policy: interpolate":    "null_policy: empty",
		"metric: manhattan":           "metric: cosine",
		"mode: debezium":              "mode: outbox",
		"credential: not-a-reference": "credential: env:PTAH_EMBED_TOKEN",
	}
	return replaceOnce(complete, "  "+replacements[line], "  "+line)
}

// replaceOnce swaps the first occurrence of a fragment.
func replaceOnce(document, from, to string) string {
	index := indexOf(document, from)
	if index < 0 {
		return document
	}
	return document[:index] + to + document[index+len(from):]
}

// indexOf finds a fragment.
func indexOf(document, fragment string) int {
	for position := 0; position+len(fragment) <= len(document); position++ {
		if document[position:position+len(fragment)] == fragment {
			return position
		}
	}
	return -1
}
