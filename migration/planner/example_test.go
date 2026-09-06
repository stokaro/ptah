package planner_test

import (
	"errors"
	"fmt"
	"slices"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// ExampleGenerateSchemaDiffSQL plans the migration from an empty database to a
// small desired schema and renders it as one PostgreSQL script. This is the
// core embedder flow: build or parse a desired *schemamodel.Database, diff it
// against the current state (schemadiff.CompareSchemas diffs two in-memory
// documents; use schemadiff.CompareWithDatabase for a live one), and hand the
// diff to the planner. The diff is the planner's whole input: the comparison
// puts everything planning needs on it, the schema-wide Declared* carries
// included. The output is deterministic: two runs over the same inputs
// produce byte-identical SQL.
func ExampleGenerateSchemaDiffSQL() {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "User", Name: "idx_users_email", Fields: []string{"email"}, Unique: true},
		},
	}

	diff := schemadiff.CompareSchemas(desired, &schemamodel.Database{}, "postgres")
	sql, err := planner.GenerateSchemaDiffSQL(diff, "postgres")
	if err != nil {
		fmt.Println("plan failed:", err)
		return
	}
	fmt.Print(sql)

	// Output:
	// -- POSTGRES TABLE: users --
	// CREATE TABLE "users" (
	//   "id" SERIAL PRIMARY KEY NOT NULL,
	//   "email" VARCHAR(255) NOT NULL
	// );
	//
	// CREATE UNIQUE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email");
}

// ExampleGenerateSchemaDiffSQLStatements is the statement-slice form for
// callers that execute sequentially and want per-statement error context.
// Statements come back without trailing semicolons, and a statement may open
// with the comment lines the planner attached to it, so each element is handed
// to the driver as-is.
func ExampleGenerateSchemaDiffSQLStatements() {
	current := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
		},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "User", Name: "idx_users_email", Fields: []string{"email"}, Unique: true},
		},
	}

	diff := schemadiff.CompareSchemas(desired, current, "postgres")
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, "postgres")
	if err != nil {
		fmt.Println("plan failed:", err)
		return
	}
	for i, statement := range statements {
		fmt.Printf("-- statement %d --\n%s\n", i+1, statement)
	}

	// Output:
	// -- statement 1 --
	// -- Add/modify columns for table: users --
	// -- ALTER statements: --
	// ALTER TABLE "users" ADD COLUMN "email" VARCHAR(255) NOT NULL
	// -- statement 2 --
	// CREATE UNIQUE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email")
}

// ExampleGetPlanner_unsupportedDialect shows the failure contract a caller
// branches on: a dialect no planner is registered for fails with a
// *ptaherr.PlanError satisfying errors.Is against ptaherr.ErrUnsupportedDialect,
// and errors.AsType retrieves the structured form carrying the dialect the
// lookup was made for. Branch on the sentinel rather than on the message: the
// wording printed below is not part of the contract.
func ExampleGetPlanner_unsupportedDialect() {
	_, err := planner.GetPlanner("dbase")
	fmt.Println(err)
	fmt.Println("unsupported dialect:", errors.Is(err, ptaherr.ErrUnsupportedDialect))

	if planErr, ok := errors.AsType[*ptaherr.PlanError](err); ok {
		fmt.Println("dialect:", planErr.Dialect)
	}

	// Output:
	// unsupported database dialect: dbase
	// unsupported dialect: true
	// dialect: dbase
}

// ExampleRequiresNoTransaction shows how an embedder decides whether a plan may
// run inside the migrator's per-migration transaction. The same index addition
// plans as an ordinary CREATE INDEX by default and as CREATE INDEX CONCURRENTLY
// under Options.ConcurrentIndexes; PostgreSQL refuses the concurrent form
// inside a transaction, and RequiresNoTransaction is what reports that.
func ExampleRequiresNoTransaction() {
	current := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
		},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "User", Name: "idx_users_email", Fields: []string{"email"}},
		},
	}
	diff := schemadiff.CompareSchemas(desired, current, "postgres")

	locking := must.Must(planner.GenerateSchemaDiffASTWithOptions(
		diff, "postgres", planner.Options{}))
	concurrent := must.Must(planner.GenerateSchemaDiffASTWithOptions(
		diff, "postgres", planner.Options{ConcurrentIndexes: true}))

	fmt.Println("locking build needs autocommit:", planner.RequiresNoTransaction("postgres", locking))
	fmt.Println("concurrent build needs autocommit:", planner.RequiresNoTransaction("postgres", concurrent))

	// Output:
	// locking build needs autocommit: false
	// concurrent build needs autocommit: true
}

// ExampleRegisteredDialects asks the registry what it can plan for. The example
// asserts membership rather than printing the whole list, because the
// registered set can grow: a built-in dialect added later, or a third-party
// Register call in the same process, would change the full listing.
func ExampleRegisteredDialects() {
	dialects := must.Must(planner.RegisteredDialects())

	fmt.Println("postgres:", slices.Contains(dialects, "postgres"))
	fmt.Println("dbase:", slices.Contains(dialects, "dbase"))

	// Output:
	// postgres: true
	// dbase: false
}
