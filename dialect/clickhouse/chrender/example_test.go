package chrender_test

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chrender"
	"ptah.run/dialect/clickhouse/chschema"
)

// ExampleRegistry renders an owned TTL operation through an explicitly
// selected local registry, without a built-in provider or database connection.
func ExampleRegistry() {
	registry, err := chrender.Registry()
	if err != nil {
		panic(err)
	}
	before := &chschema.ObservedTable{Engine: "MergeTree", TTL: "created_at + toIntervalDay(7)"}
	after := before.Desired()
	after.TTL.Value = ""
	statements, err := registry.Render(renderer.ExtensionContext{Target: "clickhouse", Parent: &ast.AlterTableNode{Name: "events"}}, ast.AlterExtension, &chast.AlterTTL{Change: chdiff.Table{Before: before, After: after}})
	if err != nil {
		panic(err)
	}
	fmt.Println(statements[0])
	// Output: ALTER TABLE events REMOVE TTL;
}
