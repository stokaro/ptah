package plangraph_test

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
)

// ExampleSchedule shows a feature operation depending on a common operation
// contributed by another owner. String payloads stand in for typed operations.
func ExampleSchedule() {
	parent := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("public", "items")
	create := plangraph.StepID{Owner: "example.org/tables", Name: "items"}
	attach := plangraph.StepID{Owner: "example.org/features", Name: "attach"}
	feature := plangraph.Contribution[string]{
		Owner:        attach.Owner,
		Steps:        []plangraph.Step[string]{{ID: attach, Payload: "attach feature", Effects: []plangraph.Effect{{Subject: parent, Action: plangraph.Read}}}},
		Dependencies: []plangraph.Dependency{{Before: create, After: attach}},
	}
	common := plangraph.Contribution[string]{
		Owner: create.Owner,
		Steps: []plangraph.Step[string]{{ID: create, Payload: "create table", Effects: []plangraph.Effect{{Subject: parent, Action: plangraph.Create}}}},
	}
	plan, err := plangraph.Schedule(context.Background(), feature, common)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, step := range plan.Steps {
		fmt.Println(step.Payload)
	}
	// Output:
	// create table
	// attach feature
}
