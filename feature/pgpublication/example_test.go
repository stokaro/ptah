package pgpublication_test

import (
	"fmt"

	"ptah.run/feature/pgpublication"
)

// ExampleDesiredPublication shows what a declaration means to the server: a
// publication that names no publish set publishes all four operations, one
// that leaves generated columns out publishes none of them, and the
// observation it predicts keeps the owner unmanaged when it names none.
func ExampleDesiredPublication() {
	declared := &pgpublication.DesiredPublication{Tables: []pgpublication.Member{{Schema: "public", Table: "orders"}}}
	if err := pgpublication.ValidateDesired(declared); err != nil {
		fmt.Println(err)
		return
	}
	predicted := declared.Observed()
	fmt.Println(declared.EffectivePublish())
	fmt.Println(declared.EffectiveGeneratedColumns())
	fmt.Printf("%q\n", predicted.Owner)
	// Output:
	// [insert update delete truncate]
	// none
	// ""
}
