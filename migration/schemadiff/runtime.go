package schemadiff

import (
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// TargetRuntime supplies the comparison and validation services used when a
// comparison includes target facts. Pure comparisons consume
// only schemaext.ComparisonRuntime and do not require a schema validator.
type TargetRuntime interface {
	schemaext.ComparisonRuntime
	schemavalidation.Service
}

// DatabaseRuntime adds rendering for the live normalization probes used by
// CompareWithDatabase. Offline comparison does not require this service.
type DatabaseRuntime interface {
	TargetRuntime
	renderer.Service
}
