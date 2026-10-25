package pgtype

import (
	"strings"

	"ptah.run/internal/tableref"
)

// A type the plan removes is dropped in the type phase's removal window, after
// the tables. A table whose row type the type names cannot be dropped before
// it without CASCADE: the server answers 2BP01, `column r of composite type
// wrap depends on type t`. The observations below answer
// [ptah.run/core/schemaext.RelationReader] so a host can tell that drop from
// one nothing holds back, without knowing the model. A column drop is not held
// back by a type that names the table's row type, and a definition reads no
// relation any other way: a domain's CHECK takes no subquery.

// ReadsRelation is [ObservedDomain.ReadsRelationIn] with the default schema
// not known.
func (v *ObservedDomain) ReadsRelation(relation string) bool {
	return v.ReadsRelationIn(relation, "")
}

// ReadsRelationIn reports whether the domain's base type is the row type of
// relation, or an array of it, an unqualified name resolving in
// defaultSchema. It implements [schemaext.RelationReaderInSchema].
func (v *ObservedDomain) ReadsRelationIn(relation, defaultSchema string) bool {
	return v != nil && namesRowType(v.BaseType, relation, defaultSchema)
}

// ReadsRelation is [ObservedComposite.ReadsRelationIn] with the default schema
// not known.
func (v *ObservedComposite) ReadsRelation(relation string) bool {
	return v.ReadsRelationIn(relation, "")
}

// ReadsRelationIn reports whether an attribute's type is the row type of
// relation, or an array of it, an unqualified name resolving in
// defaultSchema. It implements [schemaext.RelationReaderInSchema].
func (v *ObservedComposite) ReadsRelationIn(relation, defaultSchema string) bool {
	if v == nil {
		return false
	}
	for _, attribute := range v.Attributes {
		if namesRowType(attribute.Type, relation, defaultSchema) {
			return true
		}
	}
	return false
}

// ReadsRelation is [ObservedRange.ReadsRelationIn] with the default schema not
// known.
func (v *ObservedRange) ReadsRelation(relation string) bool {
	return v.ReadsRelationIn(relation, "")
}

// ReadsRelationIn reports whether the subtype is the row type of relation, or
// an array of it, an unqualified name resolving in defaultSchema. It
// implements [schemaext.RelationReaderInSchema].
func (v *ObservedRange) ReadsRelationIn(relation, defaultSchema string) bool {
	return v != nil && namesRowType(v.Subtype, relation, defaultSchema)
}

// namesRowType reports whether spelled, a type as the catalog prints it,
// names the row type of relation, an unqualified name of either resolving in
// defaultSchema. Names compare without case. An empty defaultSchema is not
// known, and an unqualified name then matches the name in any schema, so a
// type that might be the relation's row type counts as one.
func namesRowType(spelled, relation, defaultSchema string) bool {
	spelled = strings.TrimSpace(spelled)
	for strings.HasSuffix(spelled, "[]") {
		spelled = strings.TrimSpace(strings.TrimSuffix(spelled, "[]"))
	}
	named, ok := tableref.Parse(spelled)
	if !ok {
		return false
	}
	want, ok := tableref.Parse(relation)
	if !ok || !strings.EqualFold(named.Name, want.Name) {
		return false
	}
	in := func(ref tableref.Ref) string {
		if ref.Qualified {
			return ref.Schema
		}
		return defaultSchema
	}
	return in(named) == "" || in(want) == "" || strings.EqualFold(in(named), in(want))
}
