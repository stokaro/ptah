package pgtable_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

// ExampleWithPersistence shows a declaration converted into an observation
// through the owner's data service. Conversion does not inspect a database.
func ExampleWithPersistence() {
	facets, err := pgtable.WithPersistence(schemaext.Facets{}, pgtable.Persistence{Unlogged: true})
	if err != nil {
		fmt.Println(err)
		return
	}
	value, _, err := facets.Get(pgtable.PersistenceKind)
	if err != nil {
		fmt.Println(err)
		return
	}
	observations, err := (pgtable.PersistenceService{}).ConvertFeatures(context.Background(), schemaext.ConversionRequest{
		Target: "postgres", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{value},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(observations[0].Kind())
	fmt.Println(observations[0].(*pgtable.ObservedPersistence).Unlogged)
	// Output:
	// ptah.run/postgres/table-persistence
	// true
}
