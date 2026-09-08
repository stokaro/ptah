// Package countsubjectguard hosts the test that keeps a count out of the
// subject position in Go comments.
//
// "Two things follow:" in front of a two-item list tells the reader what they
// can already count, and `thing` stands where the noun belongs. It is filler of
// the kind docs/site/scripts/check-style.mjs already refuses in Markdown, and
// the rule is section 5.3 of docs/STYLE_GUIDE.md.
//
// TWO CORPORA, ONE RULE. The Markdown reader cannot see a Go comment, and most
// of the occurrences the census behind stokaro/ptah#2987 found were in Go: a
// doc comment is documentation, read through godoc by embedders who never open
// this repository. Matching Go source with a regular expression from JavaScript
// would read source text rather than comment text, so it would flag the phrase
// inside a string literal and miss nothing in exchange. This package parses the
// files instead and looks only at comments.
//
// WHAT IT DOES NOT REACH, and why each is right. `one thing` is emphatic or
// contrastive English rather than a list, and does not match because the rule
// asks for a cardinal of two or more. `<number> different things` and
// `<number> other things` say that a term denotes distinct referents, which is
// the subject itself; neither matches, because the rule requires `things`
// immediately after the count. Backticked and quoted text inside a comment is
// skipped, which is what lets a comment quote the banned phrase in order to
// name it -- this package's own doc comment does so twice above.
//
// The package carries no runtime code of its own.
package countsubjectguard
