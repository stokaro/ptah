package engine

import (
	"ptah.run/core/featureplan"
	"ptah.run/core/renderer"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemavalidation"
)

// SchemaRuntime composes the services used by schema comparison, validation,
// planning, and AST or whole-schema rendering workflows. Narrower stages may
// accept only the individual service they call. Both in-process runtimes and transport adapters can satisfy it.
type SchemaRuntime interface {
	schemapreparation.Runtime
	featureplan.Runtime
	renderer.Service
	renderer.SchemaService
	schemavalidation.Service
}
