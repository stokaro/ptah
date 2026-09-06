package schemamodel_test

import (
	"fmt"
	"strings"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
)

// ExampleMerge composes one desired state from two sources — here two
// hand-built Databases, but goschema.ParseSource or yamlschema.Parse results
// merge the same way. A table in one source may reference a table in the
// other; Merge resolves the reference, rebuilds the dependency graph over the
// combined declarations, and orders the tables so every referenced table
// comes before its referrer.
func ExampleMerge() {
	users := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
		},
	}
	posts := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Post", Name: "posts"}},
		Fields: []schemamodel.Field{
			{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Post", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
		},
	}

	db := must.Must(schemamodel.Merge(posts, users))

	for _, table := range db.Tables {
		fmt.Println(table.Name, "depends on:", db.Dependencies[table.QualifiedName()])
	}

	// Output:
	// users depends on: []
	// posts depends on: [users]
}

// ExampleMerge_conflict shows the collision policy. Identity is the database
// name, not the Go struct name: two sources declaring table "users" through
// different structs are declaring the same table, and when their desired
// properties differ, Merge refuses instead of letting one source silently
// win, and the refusal identifies the object that collided. The errors are
// plain — there is no sentinel to match with errors.Is, so branch on the
// refusal itself rather than on its wording; identical definitions would
// collapse to one without an error.
func ExampleMerge_conflict() {
	first := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users", Comment: "User accounts"},
		},
	}
	second := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Account", Name: "users", Comment: "Login accounts"},
		},
	}

	_, err := schemamodel.Merge(first, second)
	fmt.Println("refused:", err != nil)
	fmt.Println("names the table:", strings.Contains(err.Error(), "users"))

	// Output:
	// refused: true
	// names the table: true
}

// ExampleFinalize assembles a Database by hand — the way an embedder with its
// own schema frontend does — and finalizes it. The generated columns carry
// Field.GeneratedFromEmbedded, which is what makes Finalize re-runnable:
// they are discarded and rebuilt from the source declarations on every call,
// so mutating a declaration and finalizing again replaces them instead of
// stacking duplicates.
func ExampleFinalize() {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Timestamps", Name: "created_at", Type: "TIMESTAMP"},
			{StructName: "Timestamps", Name: "updated_at", Type: "TIMESTAMP"},
		},
		EmbeddedFields: []schemamodel.EmbeddedField{{
			StructName:       "User",
			EmbeddedTypeName: "Timestamps",
			Mode:             "inline",
		}},
	}

	schemamodel.Finalize(db)
	for _, field := range db.Fields {
		if field.StructName == "User" {
			fmt.Printf("%s generated=%t\n", field.Name, field.GeneratedFromEmbedded)
		}
	}

	db.EmbeddedFields[0].Prefix = "audit_"
	schemamodel.Finalize(db)
	for _, field := range db.Fields {
		if field.StructName == "User" {
			fmt.Printf("%s generated=%t\n", field.Name, field.GeneratedFromEmbedded)
		}
	}

	// Output:
	// id generated=false
	// created_at generated=true
	// updated_at generated=true
	// id generated=false
	// audit_created_at generated=true
	// audit_updated_at generated=true
}

// ExampleScopeToDialect projects one multi-dialect schema onto each target.
// An object whose dialects= scope excludes the target is absent from the
// projection — not skipped with a warning, not refused — and
// OmissionsForDialect is the accounting that keeps the absence honest: it
// names what the projection removed, so a caller can report it instead of
// saying less than the truth.
func ExampleScopeToDialect() {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
		},
		Extensions: []schemamodel.Extension{
			{Name: "pg_trgm", IfNotExists: true, Dialects: []string{"postgres"}},
		},
	}

	for _, dialect := range []string{"postgres", "mysql"} {
		scoped := schemamodel.ScopeToDialect(db, dialect)
		fmt.Printf("%s: %d table(s), %d extension(s)\n",
			dialect, len(scoped.Tables), len(scoped.Extensions))
		for _, omitted := range schemamodel.OmissionsForDialect(db, dialect) {
			fmt.Printf("%s omits %s %s (scoped to %v)\n",
				dialect, omitted.Kind, omitted.Name, omitted.Dialects)
		}
	}

	// Output:
	// postgres: 1 table(s), 1 extension(s)
	// mysql: 1 table(s), 0 extension(s)
	// mysql omits extension pg_trgm (scoped to [postgres])
}

// ExampleQualifyTableName pins the canonical qualified-reference rule that
// table-scoped identity comparisons key on: a component containing an
// identifier delimiter is wrapped in SQL-standard double quotes, so a table
// whose name contains a literal dot cannot be misread as schema-qualified.
// Enum deliberately keeps an unqualified Name verbatim instead; see
// [Enum.QualifiedName].
func ExampleQualifyTableName() {
	fmt.Println(schemamodel.QualifyTableName("public", "users"))
	fmt.Println(schemamodel.QualifyTableName("", "log.entries"))

	// Output:
	// public.users
	// "log.entries"
}
