// Package apiguard hosts the self-test for the public API snapshot guard
// (scripts/check-public-api-snapshot.sh).
//
// The tests exec the guard itself against small fixture packages written into
// throwaway temporary modules at run time, at two grains.
//
// One drives the per-package generation logic (`--emit-package`) and asserts
// that the generated fragment reacts to changes in exported struct fields and
// in methods on concrete named types. That surface is what the pre-#784,
// interface-only guard silently ignored, so those tests are what stop the guard
// from regressing back to it.
//
// The other drives the whole gate over a fixture module carrying its own ledger
// and snapshot, and asserts that an added exported field makes it exit non-zero
// while the tree its snapshot was taken from does not. A fragment that changes
// and a gate that compares it against the recorded one are two links, and the
// gate is the one a pull request runs.
//
// The fixtures are generated into temp modules rather than committed as
// packages so they are resolvable by `go doc` on every Go toolchain (a package
// under a testdata/ directory is excluded from the module graph and cannot be
// documented by import path) without being built, vetted, or linted as part of
// this module. The package carries no runtime code of its own.
package apiguard
