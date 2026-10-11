package generator

import (
	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
)

// Runtime selects comparison, conversion, validation, planning, rendering,
// safety assessment, and reversal services used by migration generation. The
// caller owns composition and supplies the same selection to both directions
// of a plan.
type Runtime interface {
	annotation.Runtime
	planner.Runtime
	safety.Runtime
	schemapreparation.Runtime
	schemaext.ReversalService
	schemaprojection.ConstraintService
	schemaprojection.TableCreationService
	schemavalidation.Service
	schemaext.NormalizationService
}
