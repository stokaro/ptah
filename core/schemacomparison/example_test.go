package schemacomparison_test

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

// exactViews compares view queries as written, for a target whose names are
// case-sensitive and which stores a query as it was given.
type exactViews struct{}

func (exactViews) QueryEqual(desired, current string) bool { return desired == current }

// ExampleRules shows a target registering its own reading of view queries.
// The shared reading folds the case of names, which on this target would call
// two different queries one; the target's rule replaces it, so the view is
// planned for a change.
func ExampleRules() {
	runtime, err := engine.New(engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{
		Name:        "custom",
		Preparation: schemapreparation.Identity{},
		Comparison:  schemacomparison.Rules{Views: exactViews{}},
	}}})
	if err != nil {
		fmt.Println(err)
		return
	}
	desired := &schemamodel.Database{Views: []schemamodel.View{{Name: "recent", Body: "SELECT id FROM Orders"}}}
	current := &catalog.Database{Views: []catalog.View{{Name: "recent", Body: "SELECT id FROM orders"}}}

	diff, err := schemadiff.CompareWithDialect(context.Background(), desired, current, "custom", runtime)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(len(diff.ViewsModified))
	// Output:
	// 1
}
