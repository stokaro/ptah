// Package integration implements Ptah's dynamic integration-test framework: it
// loads versioned entity fixtures from fixtures/entities, runs migration
// scenarios (up, down, idempotency, round-trip) against live databases, and
// produces the reports consumed by the integration-test binary.
package integration
