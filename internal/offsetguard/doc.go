// Package offsetguard hosts the test that keeps a byte offset out of a refusal's
// sentence.
//
// A SQL parser states where it stopped with [ptah.run/core/ptaherr.At], and the
// reader of the document reports the line and column an author can find. A new
// sentence spelled `at position %d` compiles, passes every test written for it,
// and hands the author a number only `head -c` can use, so nothing but this test
// sees it.
//
// The package carries no runtime code of its own.
package offsetguard
