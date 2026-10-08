package generator

import (
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
	"ptah.run/migration/planner"
)

// Runtime selects comparison, conversion, validation, planning, rendering, and
// reversal services used by migration generation. The caller owns composition and supplies the
// same selection to both directions of a plan.
type Runtime interface {
	planner.Runtime
	schemapreparation.Runtime
	schemaext.ReversalService
	schemaprojection.ConstraintService
	schemavalidation.Service
}
