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

// RelationReaderInSchema is a [RelationReader] that can also be told the
// schema an unqualified name resolves to, and so tell apart two relations of
// one name in different schemas. A host asks it through [ReadsRelationIn].
type RelationReaderInSchema interface {
	RelationReader
	// ReadsRelationIn is ReadsRelation with defaultSchema, the schema an
	// unqualified name resolves to, both in relation and in the definition. An
	// empty defaultSchema means it is not known, and the answer is then
	// ReadsRelation's. The test stays syntactic and errs on the same side: a
	// known schema turns an answer to false only where the definition names
	// the relation's name in another schema.
	ReadsRelationIn(relation, defaultSchema string) bool
}

// ReadsRelationIn asks reader whether its definition reads relation, an
// unqualified name resolving in defaultSchema. A reader that implements
// [RelationReaderInSchema] answers with defaultSchema; any other reader is
// asked [RelationReader.ReadsRelation], whose answer does not depend on it.
func ReadsRelationIn(reader RelationReader, relation, defaultSchema string) bool {
	if scoped, ok := reader.(RelationReaderInSchema); ok {
		return scoped.ReadsRelationIn(relation, defaultSchema)
	}
	return reader.ReadsRelation(relation)
}
