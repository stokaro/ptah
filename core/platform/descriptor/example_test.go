package descriptor_test

import (
	"fmt"

	"ptah.run/core/platform/descriptor"
)

// ExampleNewRegistry describes two engines, one in the other's protocol
// family, and folds spellings of them. A registry of one's own is how a test
// or a tool describes an engine without touching the process registry that
// core/platform serves.
func ExampleNewRegistry() {
	identifiers := descriptor.Identifiers{IndexNamespace: "table", IndexNames: "exact", TableNames: "exact", ColumnNames: "exact"}
	registry, err := descriptor.NewRegistry(
		descriptor.Dialect{Name: "acme", Aliases: []string{"acmedb"}, Family: "acme", Identifiers: identifiers, Placeholder: descriptor.PlaceholderDollar},
		descriptor.Dialect{Name: "acme-lite", Family: "acme", Identifiers: identifiers, Placeholder: descriptor.PlaceholderQuestion},
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, spelling := range []string{"AcmeDB", "acme-lite", "duckdb"} {
		fmt.Printf("%q -> %q\n", spelling, registry.Normalize(spelling))
	}
	lite, _ := registry.Lookup("acme-lite")
	fmt.Println("family:", lite.Family, "placeholder:", lite.Placeholder)

	// Output:
	// "AcmeDB" -> "acme"
	// "acme-lite" -> "acme-lite"
	// "duckdb" -> ""
	// family: acme placeholder: question
}
