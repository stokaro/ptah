// Package routineidentity binds routine argument normalization to structured identities.
package routineidentity

import (
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/internal/routineargs"
)

// Subject returns the identity a routine's attached settings are keyed by:
// its kind, its qualified name, and the types of its input arguments, which
// tell overloads apart, as [objectidentity.Builder.Function] and
// [objectidentity.Builder.Procedure] build it. kind is the routine kind as
// schemamodel.Function.Kind and catalog.Function.Kind spell it: "procedure",
// in any letter case, is a procedure, and anything else a function. A
// declaration and a catalog spell the arguments their own way, and
// [routineargs.InputTypes] reads both to the same types. The comparison that captures the
// settings and the lowering that renders them key routines through this one
// function, so the two cannot disagree about which routine a setting belongs
// to.
func Subject(builder objectidentity.Builder, kind, qualified, arguments string) objectidentity.ID {
	signature := routineargs.InputTypes(arguments)
	if strings.EqualFold(strings.TrimSpace(kind), string(objectidentity.KindProcedure)) {
		return builder.Procedure(qualified, signature)
	}
	return builder.Function(qualified, signature)
}
