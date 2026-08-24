// Package docsindex builds a searchable index over Ptah's own documentation and
// answers a question with the passages that address it.
//
// It exists so that a model driving the agent surface answers a question about
// Ptah from Ptah. Every other operation on that surface stops the model guessing
// about the operator's project; this one stops it guessing about the tool, and
// that guess is the one nobody notices, because a wrong answer about a flag
// looks exactly like a right one until somebody runs it (stokaro/ptah#2123).
//
// The index is a plain inverted index scored with BM25. Starlight's pagefind
// index is already built by the site, but it is built for a browser -- WASM,
// chunked fragments, ranked for a search box -- and serving it from Go means
// either linking a WASM runtime or reimplementing its query side. It also ranks
// pages, and a page is the wrong unit: an answer a model can act on is a
// passage.
package docsindex

import (
	"math"
	"sort"
	"strings"
	"unicode"
)

// BM25 parameters. The defaults from the literature; nothing in this corpus
// argued for moving them.
const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

// Document is one source file to index.
type Document struct {
	// Path is the document's repository-relative path, reported with every
	// result so a person can check the answer against the file.
	Path string
	// Content is the document's markdown.
	Content string
}

// Result is one passage that answers a query, with the score it was ranked by.
type Result struct {
	Passage
	Score float64
}

// Index is an immutable inverted index over a set of documents.
type Index struct {
	passages  []Passage
	lengths   []int
	postings  map[string][]posting
	avgLength float64
	documents int
}

type posting struct {
	passage int
	count   int
}

// Build indexes the given documents. Documents with no indexable prose
// contribute nothing and are still counted, because a caller reporting how much
// documentation is loaded is reporting what it was given.
func Build(docs []Document) *Index {
	ix := &Index{
		postings:  make(map[string][]posting),
		documents: len(docs),
	}
	for _, doc := range docs {
		ix.passages = append(ix.passages, splitPassages(doc.Path, doc.Content)...)
	}
	total := 0
	for i, passage := range ix.passages {
		counts := termCounts(passage.Heading + " " + passage.Text)
		length := 0
		for term, count := range counts {
			ix.postings[term] = append(ix.postings[term], posting{passage: i, count: count})
			length += count
		}
		ix.lengths = append(ix.lengths, length)
		total += length
	}
	if len(ix.passages) > 0 {
		ix.avgLength = float64(total) / float64(len(ix.passages))
	}
	return ix
}

// DocumentCount reports how many documents were indexed.
func (ix *Index) DocumentCount() int { return ix.documents }

// PassageCount reports how many passages the documents were cut into.
func (ix *Index) PassageCount() int { return len(ix.passages) }

// Search returns the passages that answer the query, best first, at most limit
// of them.
//
// A question the documentation does not answer returns nothing rather than the
// nearest paragraph. That is the whole point of the coverage rule below: BM25
// alone always ranks something, and a confidently-returned nearest paragraph is
// indistinguishable from an answer.
func (ix *Index) Search(query string, limit int) []Result {
	terms := informativeTerms(query)
	known := make([]string, 0, len(terms))
	for _, term := range terms {
		known = appendIfKnown(known, ix.postings, term)
	}
	if len(known) == 0 {
		return nil
	}
	// Coverage is measured against every informative term in the question, not
	// only the ones the corpus happens to know. A question built mostly from
	// words the documentation never uses is not a question the documentation
	// answers, and counting only the known terms made that question ask for
	// half of its one surviving word -- which every passage containing it met.
	// Measured: "what is the price of a subscription refund" returned three
	// confident passages about OpenAPI export and unsupported engines.
	required := (len(terms) + 1) / 2
	scores := make(map[int]float64)
	matched := make(map[int]int)
	for _, term := range known {
		idf := ix.idf(term)
		for _, post := range ix.postings[term] {
			scores[post.passage] += idf * ix.termScore(post)
			matched[post.passage]++
		}
	}
	return ix.rank(scores, matched, required, limit)
}

func appendIfKnown(known []string, postings map[string][]posting, term string) []string {
	if _, ok := postings[term]; !ok {
		return known
	}
	return append(known, term)
}

func (ix *Index) termScore(post posting) float64 {
	freq := float64(post.count)
	norm := 1 - bm25B + bm25B*float64(ix.lengths[post.passage])/ix.avgLength
	return freq * (bm25K1 + 1) / (freq + bm25K1*norm)
}

func (ix *Index) idf(term string) float64 {
	n := float64(len(ix.postings[term]))
	total := float64(len(ix.passages))
	return math.Log(1 + (total-n+0.5)/(n+0.5))
}

func (ix *Index) rank(scores map[int]float64, matched map[int]int, required, limit int) []Result {
	results := make([]Result, 0, len(scores))
	for passage, score := range scores {
		results = appendIfCovered(results, ix.passages[passage], score, matched[passage], required)
	}
	sort.Slice(results, func(i, j int) bool {
		return moreRelevant(results[i], results[j])
	})
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}
	return results
}

func appendIfCovered(results []Result, passage Passage, score float64, matched, required int) []Result {
	if matched < required {
		return results
	}
	return append(results, Result{Passage: passage, Score: score})
}

// moreRelevant orders by score, then by path and heading so that two passages
// scoring alike come back in the same order on every run. A tool whose result
// order depends on map iteration cannot be pinned by a test, and an answer that
// changes between two identical calls is one a person cannot check.
func moreRelevant(a, b Result) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	if a.Path != b.Path {
		return a.Path < b.Path
	}
	return a.Heading < b.Heading
}

// informativeTerms reduces a question to the terms worth matching on: folded,
// de-duplicated, and with the words that carry no topic removed.
func informativeTerms(query string) []string {
	seen := make(map[string]bool)
	terms := make([]string, 0, 8)
	for term := range strings.FieldsFuncSeq(strings.ToLower(query), isNotTermRune) {
		terms = appendTerm(terms, seen, term)
	}
	return terms
}

func appendTerm(terms []string, seen map[string]bool, term string) []string {
	// A flag is written `--dry-run` in a question and `--dry-run` in the docs,
	// but also `dry-run` in prose about it. Trimming the dashes that surround a
	// term makes the three one token; the dashes inside it are what carry the
	// meaning and stay.
	term = strings.Trim(term, "-_")
	if len(term) < 2 || stopwords[term] || seen[term] {
		return terms
	}
	seen[term] = true
	return append(terms, term)
}

func termCounts(text string) map[string]int {
	counts := make(map[string]int)
	for term := range strings.FieldsFuncSeq(strings.ToLower(text), isNotTermRune) {
		counts = countTerm(counts, term)
	}
	return counts
}

func countTerm(counts map[string]int, term string) map[string]int {
	term = strings.Trim(term, "-_")
	if len(term) < 2 || stopwords[term] {
		return counts
	}
	counts[term]++
	return counts
}

// isNotTermRune splits on everything that cannot be inside a term. `-` and `_`
// are kept because the questions this answers are about flags and identifiers:
// splitting `--allow-database-inspect` into four words loses the one thing that
// made it findable.
func isNotTermRune(r rune) bool {
	return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' && r != '_'
}

// stopwords are the words a question is built from rather than about. They are
// dropped from both sides, so "how do I check a migration is reversible"
// matches on `check`, `migration` and `reversible`.
var stopwords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
	"be": true, "but": true, "by": true, "can": true, "do": true, "does": true,
	"for": true, "from": true, "how": true, "if": true, "in": true, "into": true,
	"is": true, "it": true, "its": true, "me": true, "my": true, "of": true,
	"on": true, "or": true, "should": true, "that": true, "the": true,
	"their": true, "them": true, "then": true, "there": true, "these": true,
	"they": true, "this": true, "to": true, "was": true, "what": true,
	"when": true, "where": true, "which": true, "will": true, "with": true,
	"would": true, "you": true, "your": true,
}
