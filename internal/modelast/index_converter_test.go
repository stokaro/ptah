package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

// fullyDeclaredIndex sets every field a schemamodel.Index carries that either
// converter can express, so a field the copying forgets shows up as a
// difference rather than as a zero both sides agree on.
func fullyDeclaredIndex() schemamodel.Index {
	nullsDistinct := true

	return schemamodel.Index{
		StructName:     "Doc",
		TableName:      "docs",
		Name:           "idx_docs_title",
		Fields:         []string{"title"},
		Unique:         true,
		Comment:        "lookup",
		Type:           "btree",
		Parser:         "ngram",
		Condition:      "archived = false",
		Operator:       "text_pattern_ops",
		IncludeColumns: []string{"body"},
		StorageParams:  map[string]string{"fillfactor": "70"},
		NullsDistinct:  &nullsDistinct,
		Granularity:    4,
		Concurrently:   true,
	}
}

// TestIndexConverters_CarryTheSameDeclaration is the guard the fix for
// stokaro/ptah#3042 needs to keep working.
//
// FromIndex and FromIndexWithTableMapping differ in how they resolve the table
// name and in nothing else. They were two hand-written copies of the same field
// list, and a list written twice stops agreeing the moment one of them grows:
// Concurrently was in neither, so a schema asking for a concurrent build
// rendered a locking one.
//
// The declaration below names the table itself, so both converters take the
// same branch and any difference in the result is a field one of them dropped.
func TestIndexConverters_CarryTheSameDeclaration(t *testing.T) {
	c := qt.New(t)

	index := fullyDeclaredIndex()

	direct := modelast.FromIndex(index)
	mapped := modelast.FromIndexWithTableMapping(index, map[string]string{"Doc": "ignored"})

	c.Assert(mapped, qt.DeepEquals, direct)
}

// TestFromIndex_CarriesTheConcurrentBuild is the reproduction from
// stokaro/ptah#3042 at the hop that lost it.
//
// The keyword survives the parser and the schema model and was dropped here, so
// nothing reached the renderer with it set. internal/txrequire reads the same
// field to route the statement out of a transaction block, which is what a
// concurrent build cannot run inside, so the loss took that decision with it.
func TestFromIndex_CarriesTheConcurrentBuild(t *testing.T) {
	tests := []struct {
		name         string
		concurrently bool
	}{
		{name: "declared", concurrently: true},
		{name: "not declared", concurrently: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index := schemamodel.Index{
				StructName:   "Doc",
				TableName:    "docs",
				Name:         "idx_docs_title",
				Fields:       []string{"title"},
				Concurrently: test.concurrently,
			}

			c.Assert(modelast.FromIndex(index).Concurrently, qt.Equals, test.concurrently)
			c.Assert(modelast.FromIndexWithTableMapping(index, nil).Concurrently, qt.Equals, test.concurrently)
		})
	}
}
