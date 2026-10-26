package schemaext

import "ptah.run/core/platform/identifier"

// RelationReader is implemented by a captured value whose stored definition
// reads relations by name, as a row-security policy's expressions do. A server
// records the dependency, so dropping such a relation with CASCADE drops the
// value too. A host that plans that drop asks the values it captured whether
// the drop takes them along, without knowing their models.
type RelationReader interface {
	Value
	// ReadsRelation reports whether the definition names relation, schema
	// qualified or not. The test is syntactic, so it may answer true for a
	// name the definition does not resolve to the relation; that errs on the
	// side of keeping the value.
	ReadsRelation(relation string) bool
}

// RelationReaderInSchema is a [RelationReader] that can also be told where
// unqualified names resolve, and so tell apart two relations of one name in
// different schemas. A host asks it through [ReadsRelationIn].
type RelationReaderInSchema interface {
	RelationReader
	// ReadsRelationIn is ReadsRelation with the identifier semantics of the
	// session that reads the definition: a relation named without a schema
	// is in semantics.DefaultSchema, and a name written without one in the
	// definition may be in any schema of semantics.SearchPath. What semantics
	// leaves out is not known, and the answer then errs as ReadsRelation's
	// does, toward reading the relation; zero semantics give ReadsRelation's
	// answer. A known schema turns an answer to false only where the
	// definition names the relation's name somewhere the relation is not.
	ReadsRelationIn(relation string, semantics identifier.Semantics) bool
}

// ReadsRelationIn asks reader whether its definition reads relation, names
// resolving as semantics says. A reader that implements
// [RelationReaderInSchema] answers with semantics; any other reader is asked
// [RelationReader.ReadsRelation], whose answer does not depend on them.
func ReadsRelationIn(reader RelationReader, relation string, semantics identifier.Semantics) bool {
	if scoped, ok := reader.(RelationReaderInSchema); ok {
		return scoped.ReadsRelationIn(relation, semantics)
	}
	return reader.ReadsRelation(relation)
}
