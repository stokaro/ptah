package ydbsecret_test

import (
	"context"
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// ExampleRotationRequests declares the secret ext/pg, whose value comes from
// the variable PTAH_SECRET_PG, and plans it against a database that already
// holds it. A secret compares by its path alone, so the plan is empty until
// the comparison is asked to rotate the secret; the request belongs to that
// one comparison, not to the declaration. ALTER SECRET then names the
// variable, which is read when the statement runs: no value appears in the
// declaration or the plan.
func ExampleRotationRequests() {
	ctx := context.Background()
	runtime := must.Must(builtin.New())
	caps := capability.YDB262()
	declared, err := ydbsecret.Declare(schemaext.Objects{}, "ext", "pg", "", "PTAH_SECRET_PG")
	if err != nil {
		fmt.Println(err)
		return
	}
	desired := &schemamodel.Database{FeatureObjects: declared,
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
	current := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(ydbsecret.ObservedObject("ext", "pg"))),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))}

	for _, paths := range [][]string{nil, {"ext/pg"}} {
		requests, err := ydbsecret.RotationRequests(paths)
		if err != nil {
			fmt.Println(err)
			return
		}
		diff, err := schemadiff.CompareWithDatabaseInfo(ctx, desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps},
			&config.CompareOptions{FeatureRequests: requests}, runtime)
		if err != nil {
			fmt.Println(err)
			return
		}
		statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, runtime, diff, "ydb", planner.Options{Capabilities: caps})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("rotate %q: %d statement(s)\n", paths, len(statements))
		for _, statement := range statements {
			fmt.Println(statement)
		}
	}
	// Output:
	// rotate []: 0 statement(s)
	// rotate ["ext/pg"]: 1 statement(s)
	// ALTER SECRET `ext/pg` WITH (value = $PTAH_SECRET_PG)
}
