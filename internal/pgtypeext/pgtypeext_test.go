package pgtypeext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgtypeext"
)

// TestBaseTypeName_ReducesADeclarationToTheNameACatalogUses pins the reduction
// the lookup depends on.
func TestBaseTypeName_ReducesADeclarationToTheNameACatalogUses(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{name: "a modifier", declared: "vector(384)", want: "vector"},
		{name: "spacing inside the modifier", declared: "vector( 384 )", want: "vector"},
		{name: "surrounding spacing", declared: "  vector  ", want: "vector"},
		{name: "upper case", declared: "VECTOR(384)", want: "vector"},
		{name: "an array", declared: "vector(384)[]", want: "vector"},
		{name: "a bare array", declared: "hstore[]", want: "hstore"},
		{name: "no modifier at all", declared: "citext", want: "citext"},
		// Not reduced: a longer name is a different type, and trimming toward a
		// known one would make an unrelated extension undroppable.
		{name: "a longer name", declared: "vector_id", want: "vector_id"},
		{name: "a prefixed name", declared: "myvector", want: "myvector"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgtypeext.BaseTypeName(test.declared), qt.Equals, test.want)
		})
	}
}

// TestExtensionFor_AnswersForADeclaredColumnType is the lookup itself.
func TestExtensionFor_AnswersForADeclaredColumnType(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{name: "a vector column", declared: "vector(384)", want: "vector"},
		{name: "half precision", declared: "halfvec(384)", want: "vector"},
		{name: "sparse", declared: "sparsevec(384)", want: "vector"},
		{name: "a key-value column", declared: "HSTORE", want: "hstore"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			extension, found := pgtypeext.ExtensionFor(test.declared)
			c.Assert(found, qt.IsTrue)
			c.Assert(extension, qt.Equals, test.want)
		})
	}
}

// TestExtensionFor_ACoreTypeNamesNoExtension is the other half.
//
// Without it, a lookup answering "vector" for everything would satisfy every
// row above.
func TestExtensionFor_ACoreTypeNamesNoExtension(t *testing.T) {
	tests := []struct {
		name     string
		declared string
	}{
		{name: "text", declared: "TEXT"},
		{name: "a sized string", declared: "VARCHAR(255)"},
		{name: "a number", declared: "BIGINT"},
		{name: "a longer name", declared: "vector_id"},
		{name: "a prefixed name", declared: "myvector"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			extension, found := pgtypeext.ExtensionFor(test.declared)
			c.Assert(found, qt.IsFalse)
			c.Assert(extension, qt.Equals, "")
		})
	}
}

// TestTypes_EveryEntryIsInTheFormTheLookupProduces is the shape control.
//
// An entry spelled in upper case, or carrying a modifier, could never be
// matched by ExtensionFor -- it would sit here looking like coverage while
// answering nothing.
func TestTypes_EveryEntryIsInTheFormTheLookupProduces(t *testing.T) {
	c := qt.New(t)
	claimed := pgtypeext.Types()

	c.Assert(claimed, qt.Not(qt.HasLen), 0)
	for typeName, extension := range claimed {
		c.Assert(typeName, qt.Equals, pgtypeext.BaseTypeName(typeName))
		c.Assert(extension, qt.Not(qt.Equals), "")
	}
}

// TestTypes_IsACopy keeps a caller from editing the claim.
func TestTypes_IsACopy(t *testing.T) {
	c := qt.New(t)

	first := pgtypeext.Types()
	first["vector"] = "something-else"
	delete(first, "hstore")

	second := pgtypeext.Types()
	c.Assert(second["vector"], qt.Equals, "vector")
	c.Assert(second["hstore"], qt.Equals, "hstore")
}
