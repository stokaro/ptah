package pgtype

import (
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/deporder"
)

// A type the plan removes is dropped in the type phase's removal window, after
// the tables. A table whose row type the type names cannot be dropped before
// it without CASCADE: the server answers 2BP01. A column drop is not held
// back by a type that names the table's row type.

// ReadsRelation is [ObservedDomain.ReadsRelationIn] with no known session.
func (v *ObservedDomain) ReadsRelation(relation string) bool {
	return v.ReadsRelationIn(relation, identifier.Semantics{})
}

// ReadsRelationIn reports whether the domain's base type names the relation's
// row type, with unqualified names resolved in the session's search path.
func (v *ObservedDomain) ReadsRelationIn(relation string, semantics identifier.Semantics) bool {
	return v != nil && deporder.NamesRowType(v.BaseType, relation, semantics)
}

// ReadsRelation is [ObservedComposite.ReadsRelationIn] with no known session.
func (v *ObservedComposite) ReadsRelation(relation string) bool {
	return v.ReadsRelationIn(relation, identifier.Semantics{})
}

// ReadsRelationIn reports whether an attribute type names the relation's row
// type, with unqualified names resolved in the session's search path.
func (v *ObservedComposite) ReadsRelationIn(relation string, semantics identifier.Semantics) bool {
	if v == nil {
		return false
	}
	for _, attribute := range v.Attributes {
		if deporder.NamesRowType(attribute.Type, relation, semantics) {
			return true
		}
	}
	return false
}

// ReadsRelation is [ObservedRange.ReadsRelationIn] with no known session.
func (v *ObservedRange) ReadsRelation(relation string) bool {
	return v.ReadsRelationIn(relation, identifier.Semantics{})
}

// ReadsRelationIn reports whether the subtype names the relation's row type,
// with unqualified names resolved in the session's search path.
func (v *ObservedRange) ReadsRelationIn(relation string, semantics identifier.Semantics) bool {
	return v != nil && deporder.NamesRowType(v.Subtype, relation, semantics)
}
