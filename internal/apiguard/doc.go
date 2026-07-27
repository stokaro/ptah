// Package apiguard hosts the self-test for the public API snapshot guard
// (scripts/check-public-api-snapshot.sh).
//
// The test execs the guard's own per-package generation logic
// (`--emit-package`) against small fixture packages it writes into throwaway
// temporary modules at run time, and asserts that the generated snapshot reacts
// to changes in exported struct fields and in methods on concrete named types.
// That surface is what the pre-#784, interface-only guard silently ignored, so
// this test is what stops the guard from regressing back to it.
//
// The fixtures are generated into temp modules rather than committed as
// packages so they are resolvable by `go doc` on every Go toolchain (a package
// under a testdata/ directory is excluded from the module graph and cannot be
// documented by import path) without being built, vetted, or linted as part of
// this module. The package carries no runtime code of its own.
package apiguard
