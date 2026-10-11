package pgextension_test

import (
	"fmt"

	"ptah.run/feature/pgextension"
)

// ExampleChange shows what a change of a surviving extension records and
// what it does: the declaration names a newer version, so the change updates
// the extension, and an update changes the objects the extension owns.
func ExampleChange() {
	change := &pgextension.Change{
		Before: &pgextension.ObservedExtension{Schema: "public", Version: "1.5", Relocatable: true},
		After:  &pgextension.DesiredExtension{Version: "1.6"},
	}
	if err := change.Validate(); err != nil {
		fmt.Println(err)
		return
	}
	effect := change.Effect()
	fmt.Println(change.UpdatesVersion())
	fmt.Println(effect.Impact)
	fmt.Println(effect.Reason)
	// Output:
	// true
	// behavioral
	// changes the version or the schema of an extension and of the objects it owns
}
