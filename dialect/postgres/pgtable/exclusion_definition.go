package pgtable

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// DefinedConstraint exposes the local namespace and expression projection.
func (p *DesiredExclusion) DefinedConstraint(ref objectidentity.ID) schemaext.DefinedConstraint {
	return schemaext.DefinedConstraint{Ref: ref, Indexed: true, Expressions: []string{p.Elements, p.WhereCondition}}
}

// DefinedConstraint exposes the local namespace and expression projection.
func (p *ObservedExclusion) DefinedConstraint(ref objectidentity.ID) schemaext.DefinedConstraint {
	return schemaext.DefinedConstraint{Ref: ref, Indexed: true, Expressions: []string{p.Elements, p.WhereCondition}}
}
