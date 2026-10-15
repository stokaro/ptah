package schemaprojection

import "ptah.run/catalog"

// ReadBack answers, for one target, what a read of its server reports for a
// declared column or index where the server keeps something other than what
// the declaration wrote. A converter that describes a declared schema as a
// read of the target, and one that describes a read as a declaration, ask it
// so a schema compared with its own conversion plans nothing.
//
// A target whose reads report what the declaration wrote has none. An
// implementation holds no state and is safe for concurrent use.
type ReadBack interface {
	// ColumnType returns the type a read reports for a column declared as
	// declaredType, and false where the read reports the declared type.
	ColumnType(declaredType string) (string, bool)
	// IndexMethod returns what a read reports as the method of an index
	// declared with indexType, and false where the target reports none.
	IndexMethod(indexType string) (string, bool)
	// UniqueIndexDetail reports whether a unique index a read reported holds
	// something only an index declaration can carry, so it is described as
	// an index rather than as a UNIQUE constraint.
	UniqueIndexDetail(index catalog.Index) bool
}

// ReadBackRuntime gives the read-back service a target registered.
// *engine.Runtime implements it.
type ReadBackRuntime interface {
	// ReadBack returns target's service, nil when it registered none, and an
	// error for a target the runtime does not know.
	ReadBack(target string) (ReadBack, error)
}
