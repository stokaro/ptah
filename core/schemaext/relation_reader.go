package schemaext

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
