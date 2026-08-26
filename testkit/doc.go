// Package testkit provides testing helpers for Ptah users who need real
// database instances in migration and schema tests.
//
// The package lives in its own Go module so testcontainers-go remains an
// opt-in test dependency and does not bloat the main Ptah module graph.
//
// # The published-module job is red until the next release
//
// This module names the live-schema types by their package,
// go.5x5.cz/ptah/catalog. That package exists in the working tree, which is
// what every job here builds against through `replace go.5x5.cz/ptah => ..`.
// The "Build against the published module" job drops that replace on purpose
// and builds against the module proxy, so it resolves the version in the
// require line -- and no released version carries the package yet. Its
// refusal ends:
//
//	does not contain package go.5x5.cz/ptah/catalog
//
// That is the job doing its work, not a defect in it. It goes green when a
// release carrying the package is published and the require line names it.
// Weakening or skipping the job would give back exactly the blind spot it was
// added to close.
package testkit
