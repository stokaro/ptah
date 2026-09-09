// Package chronologyguard hosts the test that keeps Ptah's own past out of Go
// comments.
//
// A comment says what holds now. A clause dating a statement to a Ptah issue --
// `X was Y until stokaro/ptah#N`, `it has done Z since #N` -- carries nothing a
// reader can act on: the code it describes is in front of them, and `git log -L`
// answers the rest. Pre-GA there is no second reason to keep it either, because
// Ptah owes no compatibility with its own previous behavior, so the narration
// documents an upgrade path nobody walks. The rule is section 6.7 of
// docs/STYLE_GUIDE.md and the entry AGENTS.md carries under Language And
// Spelling (stokaro/ptah#3134).
//
// TWO CORPORA, ONE RULE. docs/site/scripts/check-implementation-chronology.mjs
// holds the same shape over Markdown and cannot see a Go comment. Matching Go
// source with a regular expression from JavaScript would read source text
// rather than comment text, so it would flag a reference inside a string
// literal and miss nothing in exchange. This package parses the files and looks
// only at comments, the way countsubjectguard does for its own rule.
//
// WHAT IT CATCHES THAT IT SHOULD NOT, once: a pronoun can refer to the user's
// data rather than to Ptah. `a document ... by what it used to say` is a
// sentence about a source row, and the rewrite is to name the noun -- `by text
// the source no longer holds` -- which reads better anyway. One occurrence in
// the whole tree, which is the rate that makes the subject list worth having.
//
// WHAT IT DOES NOT REACH, and why each is right. A plain citation --
// `(stokaro/ptah#2209)`, or a sentence saying what an issue OWNS -- is a
// pointer rather than a date, and no preposition precedes it. `after` is
// deliberately absent: `after stokaro/ptah#2725 removes the converter` is a
// forward reference to work that has an owner, and no regular expression
// separates it from the backward reading. The prose rule still governs `after`;
// this holds the four spellings that have only the backward reading. Backticked
// and quoted text inside a comment is skipped, which is what lets a comment
// quote the banned form in order to name it -- this doc comment does so twice
// above.
//
// The package carries no runtime code of its own.
package chronologyguard
