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

// ExampleScheduleRewritten replaces a common column addition with an owner
// operation and preserves the outgoing dependency. Payload strings stand in
// for typed operations; grouping makes no transaction guarantee.
func ExampleScheduleRewritten() {
	column := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Column("items", "extra")
	add := plangraph.StepID{Owner: "example.org/common", Name: "add-extra"}
	read := plangraph.StepID{Owner: add.Owner, Name: "read-extra"}
	replacement := plangraph.StepID{Owner: "example.org/feature", Name: "column-and-feature"}
	common := plangraph.Contribution[string]{
		Owner: add.Owner,
		Steps: []plangraph.Step[string]{
			{ID: add, Payload: "add column", Effects: []plangraph.Effect{{Subject: column, Action: plangraph.Create}}},
			{ID: read, Payload: "read column", Effects: []plangraph.Effect{{Subject: column, Action: plangraph.Read}}},
		},
		Dependencies: []plangraph.Dependency{{Before: add, After: read}},
	}
	feature := plangraph.Contribution[string]{Owner: replacement.Owner, Steps: []plangraph.Step[string]{{
		ID: replacement, Payload: "add column with feature", Effects: []plangraph.Effect{{Subject: column, Action: plangraph.Create}},
	}}}
	claims := []plangraph.Rewrite{{Sources: []plangraph.StepID{add}, Replacement: replacement}}
	plan, err := plangraph.ScheduleRewritten(context.Background(), common, claims, feature)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, step := range plan.Steps {
		fmt.Println(step.Payload)
	}
	// Output:
	// add column with feature
	// read column
}

// ExampleLifecycleDependencies hands one name from an object a contribution
// drops to an object another contribution creates. Neither contribution sees
// the other's step, so the host derives the order before it schedules both.
func ExampleLifecycleDependencies() {
	name := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("public", "events")
	drop := plangraph.Step[string]{ID: plangraph.StepID{Owner: "example.org/scheme", Name: "z-drop-old"}, Payload: "drop old object",
		Effects: []plangraph.Effect{{Subject: name, Action: plangraph.Drop}}}
	create := plangraph.Step[string]{ID: plangraph.StepID{Owner: "example.org/scheme", Name: "a-create-new"}, Payload: "create new object",
		Effects: []plangraph.Effect{{Subject: name, Action: plangraph.Create}}}
	dropping := plangraph.Contribution[string]{Owner: "example.org/scheme", Steps: []plangraph.Step[string]{drop}}
	creating := plangraph.Contribution[string]{Owner: "example.org/scheme", Steps: []plangraph.Step[string]{create}}
	edges, err := plangraph.LifecycleDependencies(context.Background(), dropping, creating)
	if err != nil {
		fmt.Println(err)
		return
	}
	dropping.Dependencies = append(dropping.Dependencies, edges...)
	plan, err := plangraph.Schedule(context.Background(), dropping, creating)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, step := range plan.Steps {
		fmt.Println(step.Payload)
	}
	// Output:
	// drop old object
	// create new object
}
