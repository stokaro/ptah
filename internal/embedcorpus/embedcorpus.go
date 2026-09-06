// Package embedcorpus is the evaluation corpus an operator writes: the queries
// a generation has to answer, and which documents answering them means.
//
// It holds identifiers and never documents. That is ADR 0013's boundary and it
// is the reason the format is worth having its own package: a corpus file that
// could carry text would be a second copy of the source, in a repository, with
// a different set of people who can read it (stokaro/ptah#2068).
package embedcorpus

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"

	"ptah.run/internal/embeddigest"
	"ptah.run/internal/embedeval"
)

// FormatVersion is the file format this build reads.
const FormatVersion = 1

// Document is one corpus file.
type Document struct {
	// Version is the file format's version.
	Version int `yaml:"version"`
	// Name and Description are for a person.
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	// DefaultK is how deep a case looks when it does not say.
	DefaultK int `yaml:"default_k"`
	// Cases are the queries.
	Cases []CaseDocument `yaml:"cases"`
}

// CaseDocument is one query and what answering it means.
type CaseDocument struct {
	// ID names the case, for a report a person reads.
	ID string `yaml:"id"`
	// Query is the text that gets embedded and searched with.
	Query string `yaml:"query"`
	// K is how deep this case looks, zero to use the corpus default.
	K int `yaml:"k"`
	// Required are the keys that MUST appear in the top K.
	//
	// A hard expectation rather than a score: a corpus that only scored would
	// pass a generation that ranks the one document the question is about
	// eleventh.
	Required []string `yaml:"required"`
	// Relevant maps a key to its graded relevance, for the ranked measures.
	//
	// A key absent from the map is irrelevant, which is not the same as
	// forbidden -- the corpus says what a good answer contains, not what a bad
	// one may not.
	Relevant map[string]float64 `yaml:"relevant"`
}

// Corpus is a loaded evaluation corpus.
type Corpus struct {
	// Name and Description are for a person.
	Name        string
	Description string
	// DefaultK is how deep a case looks when it does not say.
	DefaultK int
	// Cases are the queries, in the file's order.
	Cases []embedeval.Case
	// Digest is the corpus's content address.
	//
	// A retrieval number means nothing without the corpus it was measured on,
	// and "the corpus" is not a filename: a case edited between two runs makes
	// the two numbers incomparable exactly as a changed model would.
	Digest string
}

// Load reads a corpus file.
func Load(path string) (Corpus, error) {
	body, err := os.ReadFile(path) //gosec:disable G304 -- the operator named this file on the command line
	if err != nil {
		return Corpus{}, fmt.Errorf("read %s: %w", path, err)
	}
	return Parse(body, path)
}

// Parse reads a corpus from bytes.
//
// Unknown fields are refused for the same reason the specification refuses
// them: `expected` for `required` produces a corpus that measures something
// else and reports it as a pass.
func Parse(body []byte, path string) (Corpus, error) {
	decoder := yaml.NewDecoder(strings.NewReader(string(body)))
	decoder.KnownFields(true)
	var document Document
	if err := decoder.Decode(&document); err != nil {
		return Corpus{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return document.Resolve(path)
}

// Resolve turns a parsed document into a corpus.
func (d Document) Resolve(path string) (Corpus, error) {
	if d.Version != FormatVersion {
		return Corpus{}, fmt.Errorf(
			"%s declares format version %d and this build reads %d", path, d.Version, FormatVersion)
	}
	if len(d.Cases) == 0 {
		// An empty corpus passes every threshold by having nothing to measure,
		// and a gate that passes because it asked nothing is the failure this
		// whole feature exists to avoid.
		return Corpus{}, fmt.Errorf("%s declares no cases, so it would measure nothing", path)
	}

	cases := make([]embedeval.Case, 0, len(d.Cases))
	seen := make(map[string]bool, len(d.Cases))
	for index, document := range d.Cases {
		resolved, err := document.resolve(path, index)
		if err != nil {
			return Corpus{}, err
		}
		if seen[resolved.ID] {
			// Two cases with one name make a report nobody can act on: a
			// finding names the case, and the reader has two to look at.
			return Corpus{}, fmt.Errorf("%s declares case %q twice", path, resolved.ID)
		}
		seen[resolved.ID] = true
		cases = append(cases, resolved)
	}

	return Corpus{
		Name: d.Name, Description: d.Description, DefaultK: d.DefaultK,
		Cases: cases, Digest: d.digest(),
	}, nil
}

// resolve turns one case document into a case.
func (c CaseDocument) resolve(path string, index int) (embedeval.Case, error) {
	if strings.TrimSpace(c.ID) == "" {
		return embedeval.Case{}, fmt.Errorf("%s: case %d has no id", path, index+1)
	}
	if strings.TrimSpace(c.Query) == "" {
		return embedeval.Case{}, fmt.Errorf("%s: case %q has no query", path, c.ID)
	}
	if len(c.Required) == 0 && len(c.Relevant) == 0 {
		// A case that expects nothing is satisfied by any answer, including
		// none, and it lifts the mean of every measure it is averaged into.
		return embedeval.Case{}, fmt.Errorf(
			"%s: case %q expects nothing, so any answer satisfies it", path, c.ID)
	}
	for key, grade := range c.Relevant {
		if grade <= 0 {
			// Zero is what an unlisted key already means. A listed zero reads
			// as "this one matters" and scores as "this one does not", which is
			// the corpus disagreeing with itself.
			return embedeval.Case{}, fmt.Errorf(
				"%s: case %q grades %q as %v; a key that does not matter is left out rather than "+
					"graded zero", path, c.ID, key, grade)
		}
	}
	return embedeval.Case{
		ID: c.ID, Query: c.Query, K: c.K,
		Required: c.Required, Relevant: gradesFor(c),
	}, nil
}

// gradesFor is the case's graded relevance, derived from its hard expectation
// when it states none.
//
// A case saying `required: ["42"]` and nothing else is saying that 42 is a
// right answer, which is what a grade means. Read as "no key is relevant" it
// contributed to no ranked measure at all -- `scoreCase` skips a case with no
// grades, deliberately, so that empty cases cannot carry a failing evaluation
// over the line. A corpus where EVERY case is required-only therefore scored
// nothing, and the mean of nothing is zero: `recall 0.000, MRR 0.000, NDCG
// 0.000`, at exit 0, for a generation that answers every query perfectly
// (stokaro/ptah#2634). It is indistinguishable from a generation that found
// nothing, and it is what `--max-recall-drop` and `--max-ndcg-drop` compare
// against.
//
// An explicit `relevant` map wins, because grading is how an author says one
// right answer is better than another, and a derived grade would flatten that.
func gradesFor(c CaseDocument) map[string]float64 {
	if len(c.Relevant) > 0 {
		return c.Relevant
	}
	grades := make(map[string]float64, len(c.Required))
	for _, key := range c.Required {
		grades[key] = 1
	}
	return grades
}

// digest is the corpus's content address.
//
// Everything a number depends on, and nothing else: the queries, their depths,
// and the expectations. The corpus's name and description are outside it for
// the same reason a generation's are -- renaming a corpus does not make its
// measurements incomparable.
func (d Document) digest() string {
	components := []string{"corpus", fmt.Sprintf("%d", FormatVersion), "default_k", fmt.Sprintf("%d", d.DefaultK)}
	for _, testCase := range d.Cases {
		components = append(components,
			"case", testCase.ID, "query", testCase.Query, "k", fmt.Sprintf("%d", testCase.K))
		components = append(components, "required", fmt.Sprintf("%d", len(testCase.Required)))
		components = append(components, testCase.Required...)
		components = append(components, "relevant", fmt.Sprintf("%d", len(testCase.Relevant)))
		components = append(components, gradedKeys(testCase.Relevant)...)
	}
	return embeddigest.Of(components...)
}

// gradedKeys renders a relevance map in a stable order.
//
// Sorted, because a YAML map has no order and two files that differ only in how
// somebody typed them are one corpus.
func gradedKeys(relevant map[string]float64) []string {
	keys := make([]string, 0, len(relevant))
	for key := range relevant {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rendered := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		rendered = append(rendered, key, fmt.Sprintf("%v", relevant[key]))
	}
	return rendered
}
