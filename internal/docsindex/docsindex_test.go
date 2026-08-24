package docsindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/docsindex"
)

// The corpus every test in this file searches. It is written rather than read
// from the repository so that a documentation edit cannot turn a behavior test
// red for a reason that has nothing to do with the behavior.
var corpus = []docsindex.Document{
	{
		Path: "docs/flags.md",
		Content: `---
title: Flags
sidebar: reference
---

# Flags

Intro text above the first subheading.

## Inspection

` + "`--allow-database-inspect`" + ` takes ask or allow, and decides whether a
configured database may be read.

## Rendering

The renderer never connects.

### Presets

A bare dialect renders against that dialect's default preset.

## Transcript

` + "```console" + `
# this comment is not a heading
$ ptah schema render
` + "```" + `

Text after the fence, still under Transcript.
`,
	},
	{
		Path: "docs/checkpoints.md",
		Content: `# Checkpoints

## Checkpoint versus baseline

Both let a database skip historical migrations, but they solve opposite
problems.

## Related issues

#2123 opened this, and the hash begins a reference rather than a heading.
`,
	},
}

func TestSearchFindsThePassageThatAnswers(t *testing.T) {
	c := qt.New(t)

	results := docsindex.Build(corpus).Search("difference between checkpoint and baseline", 3)

	c.Assert(len(results) > 0, qt.IsTrue)
	c.Assert(results[0].Path, qt.Equals, "docs/checkpoints.md")
	c.Assert(results[0].Heading, qt.Equals, "Checkpoints > Checkpoint versus baseline")
	c.Assert(results[0].Text, qt.Contains, "opposite")
}

// A question the documentation does not answer returns nothing rather than the
// nearest paragraph. BM25 alone always ranks something, and a confidently
// returned nearest paragraph is indistinguishable from an answer.
func TestSearchReturnsNothingForAQuestionTheDocumentsDoNotAnswer(t *testing.T) {
	c := qt.New(t)

	ix := docsindex.Build(corpus)

	c.Assert(ix.Search("what is the price of a subscription refund", 3), qt.HasLen, 0)
	c.Assert(ix.Search("", 3), qt.HasLen, 0)
}

// Coverage is measured against every informative term in the question, not only
// the ones the corpus knows. Counting the known terms only made a question
// whose single recognized word was "database" ask for half of one word, which
// every passage carrying it met.
func TestSearchDoesNotAnswerOnOneRecognizedWord(t *testing.T) {
	c := qt.New(t)

	results := docsindex.Build(corpus).Search("database quota billing overage", 3)

	c.Assert(results, qt.HasLen, 0)
}

func TestSearchMatchesAFlagWrittenWithOrWithoutItsDashes(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "as written on the command line", query: "--allow-database-inspect"},
		{name: "as written in prose", query: "allow-database-inspect"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			results := docsindex.Build(corpus).Search(tt.query, 3)

			c.Assert(len(results) > 0, qt.IsTrue)
			c.Assert(results[0].Heading, qt.Equals, "Flags > Inspection")
		})
	}
}

func TestBuildCountsDocumentsAndPassages(t *testing.T) {
	c := qt.New(t)

	ix := docsindex.Build(corpus)

	c.Assert(ix.DocumentCount(), qt.Equals, 2)
	c.Assert(ix.PassageCount() > ix.DocumentCount(), qt.IsTrue)
}

func TestPassageHeadingTrail(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "text above the first subheading belongs to the page heading",
			query: "intro text above subheading",
			want:  "Flags",
		},
		{
			name:  "a deeper heading nests under the one before it",
			query: "bare dialect renders against default preset",
			want:  "Flags > Rendering > Presets",
		},
		{
			// The level-2 heading after a level-3 one drops the deeper entry
			// rather than nesting under it.
			name:  "a shallower heading pops the trail",
			query: "text after the fence",
			want:  "Flags > Transcript",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			results := docsindex.Build(corpus).Search(tt.query, 1)

			c.Assert(results, qt.HasLen, 1)
			c.Assert(results[0].Heading, qt.Equals, tt.want)
		})
	}
}

// Three things look like a heading and are not. Each one cut a passage in the
// wrong place before the splitter knew about it.
func TestSplitterIgnoresWhatOnlyLooksLikeAHeading(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			// Every shell transcript in this repository opens a comment with a
			// `#`, and splitting there put the command in a passage of its own.
			name:  "a hash inside a fenced block",
			query: "this comment is not a heading",
			want:  "Flags > Transcript",
		},
		{
			// `#2123` is an issue reference; the space after the hashes is what
			// makes a heading.
			name:  "an issue reference",
			query: "hash begins a reference rather than a heading",
			want:  "Checkpoints > Related issues",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			results := docsindex.Build(corpus).Search(tt.query, 1)

			c.Assert(results, qt.HasLen, 1)
			c.Assert(results[0].Heading, qt.Equals, tt.want)
		})
	}
}

// A Starlight page opens with a YAML block whose keys are metadata, not prose.
// Indexing it made every site page match the word "title".
func TestFrontmatterIsNotIndexed(t *testing.T) {
	c := qt.New(t)

	results := docsindex.Build(corpus).Search("sidebar reference title", 3)

	c.Assert(results, qt.HasLen, 0)
}

// Two passages scoring alike come back in the same order on every run. A result
// order that depends on map iteration cannot be pinned, and an answer that
// changes between two identical calls is one a person cannot check.
func TestSearchOrderIsStable(t *testing.T) {
	c := qt.New(t)

	ix := docsindex.Build(corpus)
	first := ix.Search("database", 5)

	c.Assert(len(first) > 1, qt.IsTrue)
	for range 20 {
		c.Assert(ix.Search("database", 5), qt.DeepEquals, first)
	}
}

func TestSearchHonorsTheLimit(t *testing.T) {
	c := qt.New(t)

	ix := docsindex.Build(corpus)

	c.Assert(ix.Search("database", 1), qt.HasLen, 1)
}

// The best match comes first. Every other test here asks a question with one
// answer, and a single result cannot tell an ordering apart from its reverse.
func TestSearchRanksTheBetterMatchFirst(t *testing.T) {
	c := qt.New(t)

	results := docsindex.Build(corpus).Search("database", 5)

	c.Assert(len(results) > 1, qt.IsTrue)
	c.Assert(results[0].Score > results[len(results)-1].Score, qt.IsTrue)
	c.Assert(descending(results), qt.IsTrue)
}

func descending(results []docsindex.Result) bool {
	for i := 1; i < len(results); i++ {
		if results[i-1].Score < results[i].Score {
			return false
		}
	}
	return true
}
