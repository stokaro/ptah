// Package schematests holds the external tests of cmd/atlas's `schema` verbs.
//
// It exists for the reason stokaro/ptah#1812 records: cmd/atlas was one test
// package of 2517 tests, and on the slow Windows runner that one package took
// more than half of Go's per-package timeout by itself. The slowest test in it
// is 2.9% of the package, so there was nothing to speed up -- the only fix is
// fewer tests per package, and `go test` parallelises packages rather than
// files.
//
// The tests here are the ones that were already `package atlas_test`. Moving
// an external test file changes nothing about what it can reach: it used only
// exported API before the move and only exported API after it. The helpers
// they share live in cmd/atlas/internal/atlastest, which is what made the move
// mechanical.
//
// This file carries the package clause and nothing else, because the directory
// holds only tests.
package schematests
