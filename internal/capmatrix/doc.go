// Package capmatrix drives the tiered capability-matrix pipeline of
// stokaro/ptah#1341: it turns the release lines declared in
// internal/capabilityprobe into a CI fan-out, runs one cell of it against a
// live server, and aggregates the per-cell results back into one attributable
// verdict.
//
// The aggregation is the part that cannot be done in a workflow file. A tier
// passes only when every cell the matrix declared runnable came back with a
// result, so a job that never started is a failure rather than an absence, and
// a tier 3 cell whose capability probe disagreed is named as a capability
// disagreement rather than buried in a suite log.
package capmatrix
