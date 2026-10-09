package crdbrender_test

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/dialect/cockroachdb/crdbast"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbrender"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// ExampleRegistry renders a row-level TTL change inside its ALTER TABLE
// parent: the parameter the new policy stops naming is reset before the rest
// is set.
func ExampleRegistry() {
	registry, err := crdbrender.Registry()
	if err != nil {
		fmt.Println(err)
		return
	}
	change := crdbdiff.RowTTL{
		Before: &crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpireAfter: "3 days", JobCron: "@daily"}},
		After:  &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "7 days"}},
	}
	statements, err := registry.Render(renderer.ExtensionContext{
		Target: "cockroachdb", Capabilities: capability.CockroachDB26(), Parent: &ast.AlterTableNode{Name: "sessions"},
	}, ast.AlterExtension, &crdbast.AlterRowTTL{Change: change})
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, statement := range statements {
		fmt.Println(statement)
	}
	// Output:
	// ALTER TABLE "sessions" RESET (ttl_job_cron);
	// ALTER TABLE "sessions" SET (ttl_expire_after = '7 days');
}
