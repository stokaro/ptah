// Package atlascompat exposes narrow helpers for Atlas compatibility tooling.
//
// The package is intentionally small. It gives external conformance and
// migration-compatibility tools stable entry points for behavior Ptah supports
// publicly, while keeping parser, HCL, conversion, and sum-file implementation
// packages behind Go internal boundaries.
package atlascompat
