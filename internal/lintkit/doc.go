// Package lintkit is the code migration lint shares with the bundled engine
// owners that check their own lint rules: the recognizers that read a
// statement's words, the index of the schema state a version starts from, and
// the scan for the unique indexes a file builds.
//
// It works on the forms of ptah.run/core/lintext, the statements a splitter
// returns and the baseline rows a host reads, so ptah.run/migration/lint and
// an owner such as ptah.run/dialect/postgres/pglint read the same answers
// from one implementation. Neither the host nor an owner imports the other.
package lintkit
