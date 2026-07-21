// Package testkit provides testing helpers for Ptah users who need real
// database instances in migration and schema tests.
//
// The package lives in its own Go module so testcontainers-go remains an
// opt-in test dependency and does not bloat the main Ptah module graph.
package testkit
