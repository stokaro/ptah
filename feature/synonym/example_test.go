package synonym_test

import (
	"fmt"

	"ptah.run/feature/synonym"
)

// ExampleDeclaredTarget shows the two spellings of a synonym's target. SQL
// Server records base_object_name with its own bracket quoting; TargetParts
// reads it from the right, so an empty middle part stays where it is, and
// DeclaredTarget writes the parts back the way a declaration spells them.
func ExampleDeclaredTarget() {
	for _, stored := range []string{"[app].[orders]", "[other].[dbo].[gauge]", "[remote]..[dbo].[orders]"} {
		parts := synonym.TargetParts(stored)
		fmt.Printf("%s -> %s database=%q\n", stored, synonym.DeclaredTarget(parts), parts[1])
	}

	// Output:
	// [app].[orders] -> app.orders database=""
	// [other].[dbo].[gauge] -> other.dbo.gauge database="other"
	// [remote]..[dbo].[orders] -> remote..dbo.orders database=""
}
