package difftypes_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// ExampleTableCreationsFor builds the table additions of a hand-written diff.
// The caller has names; the bundle each creation needs — the table's own
// columns, the enums they name, the table-level constraints — is read out of
// the desired schema, so stating which tables are added does not also mean
// restating what they are. A name no declared table answers to is skipped,
// because a diff naming an undeclared table has nothing to create.
func ExampleTableCreationsFor() {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
			{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
		},
	}

	creations := difftypes.TableCreationsFor(desired, "users", "not_declared")
	fmt.Println("creations:", creations.Names())
	for _, creation := range creations {
		for _, field := range creation.Fields {
			fmt.Printf("  %s %s\n", field.Name, field.Type)
		}
	}

	// Output:
	// creations: [users]
	//   id SERIAL
	//   email VARCHAR(255)
}

// ExampleTableChanges_InDependencyOrder orders creations so that a table comes
// after everything it references. The edge is the child's foreign= reference;
// the creations were deliberately stated child-first, and the ordering still
// puts the parent first, which is the property migration ordering rests on.
func ExampleTableChanges_InDependencyOrder() {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Post", Name: "posts"},
			{StructName: "User", Name: "users"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Post", Name: "author_id", Type: "INTEGER", Foreign: "users(id)"},
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
		},
	}

	creations := difftypes.TableCreationsFor(desired, "posts", "users")
	fmt.Println("as stated:", creations.Names())
	fmt.Println("ordered:  ", creations.InDependencyOrder().Names())

	// Output:
	// as stated: [posts users]
	// ordered:   [users posts]
}

// ExampleSchemaDiff_HasChanges is the check a CI gate or an automated
// deployment makes before deciding to generate a migration: an empty diff
// answers false, and a diff carrying any change — here one table addition —
// answers true, without the caller reading the object-kind fields itself.
func ExampleSchemaDiff_HasChanges() {
	empty := &difftypes.SchemaDiff{}
	fmt.Println(empty.HasChanges())

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "SERIAL", Primary: true}},
	}
	diff := &difftypes.SchemaDiff{TablesAdded: difftypes.TableCreationsFor(desired, "users")}
	fmt.Println(diff.HasChanges())

	// Output:
	// false
	// true
}

// ExampleSupplementLists enumerates the SchemaDiff lists that qualify another
// list rather than carrying changes of their own. A reader of a serialized
// diff needs the distinction: a supplement without a matching entry in its
// base list changes nothing, and a report that printed one as a change would
// print the same removed object twice.
func ExampleSupplementLists() {
	supplements := difftypes.SupplementLists()
	for _, name := range slices.Sorted(maps.Keys(supplements)) {
		fmt.Printf("%s qualifies %s\n", name, supplements[name])
	}

	// Output:
	// constraint_backed_index_removals qualifies indexes_removed
	// foreign_keys_removed_with_tables qualifies constraints_removed
}

// ExampleRangeChanges_MarshalJSON shows the wire-shape guarantee every Changes
// family in this package keeps: in memory the list carries whole definitions
// for a planner, and the JSON is the list of names. Nil and empty stay
// distinct on the wire — null is a comparison that did not run, and [] is one
// that found nothing.
func ExampleRangeChanges_MarshalJSON() {
	added := difftypes.RangeChanges{
		{Name: "floatrange", Schema: "app", Subtype: "float8"},
	}
	fmt.Println(string(must.Must(json.Marshal(added))))
	fmt.Println(string(must.Must(json.Marshal(difftypes.RangeChanges(nil)))))
	fmt.Println(string(must.Must(json.Marshal(difftypes.RangeChanges{}))))

	// Output:
	// ["app.floatrange"]
	// null
	// []
}
