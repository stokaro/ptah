// Package integrationharness implements Ptah's dynamic integration-test
// framework: it loads versioned entity fixtures from an injected filesystem
// (integration/fixtures/entities in this repository), runs migration scenarios
// (up, down, idempotency, round-trip) against live databases, and produces the
// reports consumed by the integration-test binary.
//
// The harness is ordinary library code, so it lives outside the integration
// tree and its unit tests sit beside it with no build constraint. Only the
// tests that drive it against a live database belong under integration/, where
// every test file requires //go:build integration.
package integrationharness
