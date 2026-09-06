package schemadiff_test

import (
	"context"
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/migration/schemadiff"
)

// ExampleCompareSchemas diffs two in-memory desired-schema documents: the
// second argument names the side treated as the existing state, and the diff
// plans what would turn it into the first. It is the simplest fully-offline
// entry point -- no database, no error to handle -- and the natural one for
// comparing two revisions of a declaration.
func ExampleCompareSchemas() {
	previous := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
		},
	}
	schemamodel.Finalize(previous)

	revised := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
			{StructName: "User", Name: "created_at", Type: "TIMESTAMP", Nullable: true},
		},
		Indexes: []schemamodel.Index{{
			StructName: "User",
			Name:       "idx_users_created_at",
			Fields:     []string{"created_at"},
		}},
	}
	schemamodel.Finalize(revised)

	diff := schemadiff.CompareSchemas(revised, previous, "postgres")

	fmt.Println("has changes:", diff.HasChanges())
	for _, table := range diff.TablesModified {
		fmt.Printf("modify %s: add columns %v\n", table.TableName, table.ColumnsAdded.Names())
	}
	for _, index := range diff.IndexAdditions() {
		fmt.Printf("add index %s on %s\n", index.Name, index.TableName)
	}

	// Output:
	// has changes: true
	// modify users: add columns [created_at]
	// add index idx_users_created_at on users
}

// ExampleCompareWithDatabase compares a desired schema against a live
// connection, which is the entry point that also resolves the catalog's
// identifier semantics before comparing. The target here is SQLite's embedded
// in-memory engine, so the whole flow -- connect, read the current schema,
// compare -- runs without any server.
func ExampleCompareWithDatabase() {
	ctx := context.Background()
	conn := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:"))
	defer dbschema.CloseAndWarn(conn)

	current := must.Must(conn.Reader().ReadSchemaContext(ctx))

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Task", Name: "tasks"}},
		Fields: []schemamodel.Field{
			{StructName: "Task", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Task", Name: "title", Type: "TEXT"},
		},
	}
	schemamodel.Finalize(desired)

	diff, err := schemadiff.CompareWithDatabase(ctx, conn, desired, current, nil)
	if err != nil {
		fmt.Println("compare:", err)
		return
	}
	fmt.Println("tables to create:", diff.TablesAdded.Names())

	// Output:
	// tables to create: [tasks]
}

// ExampleCompareWithDialect shows the dialect changing the comparison's answer
// on the same two states. The declaration types a column by the name of an
// enum declared beside it; MySQL stores such a column as an inline
// enum('...') type, so under the mysql dialect the pair converges, while the
// dialect-neutral comparison reads the enum name and the stored spelling as
// two different types and plans changes that would never converge.
func ExampleCompareWithDialect() {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts"}},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "status", Type: "status_kind", Nullable: true},
		},
		Enums: []schemamodel.Enum{{Name: "status_kind", Values: []string{"active", "archived"}}},
	}
	schemamodel.Finalize(desired)

	live := &catalog.Database{
		Tables: []catalog.Table{{Name: "accounts", Columns: []catalog.Column{
			{Name: "status", DataType: "enum('active','archived')", IsNullable: "YES"},
		}}},
	}

	withDialect := schemadiff.CompareWithDialect(desired, live, "mysql")
	fmt.Println("mysql:", withDialect.HasChanges())

	neutral := schemadiff.Compare(desired, live)
	fmt.Println("neutral:", neutral.HasChanges(), "enums to add:", neutral.EnumsAdded.Names())

	// Output:
	// mysql: false
	// neutral: true enums to add: [status_kind]
}

// ExampleCompareReportingUndecidedAdditions shows the honesty contract around
// coverage. The current state's record says the read never looked at
// sequences, so the comparison cannot know whether the declared sequence
// already exists; an unguarded CREATE SEQUENCE would fail the migration if it
// does. The addition is withheld from the diff -- HasChanges stays false --
// and the second return names what was withheld, so a caller can warn instead
// of reporting a synced schema.
func ExampleCompareReportingUndecidedAdditions() {
	desired := &schemamodel.Database{
		Sequences: []schemamodel.Sequence{{Name: "invoice_numbers", Schema: "public"}},
	}
	current := &catalog.Database{}
	current.NotDescribed = coverage.Set{}.WithKind(coverage.Sequence)

	diff, undecided := schemadiff.CompareReportingUndecidedAdditions(desired, current, nil)

	fmt.Println("planned additions:", diff.SequencesAdded.Names())
	fmt.Println("has changes:", diff.HasChanges())
	for _, object := range undecided {
		fmt.Printf("undecided: %s %s\n", object.Kind, object.Name)
	}

	// Output:
	// planned additions: []
	// has changes: false
	// undecided: sequence public.invoice_numbers
}
