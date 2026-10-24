package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
)

// inventoryDocument declares objects of every kind the shared comparison
// pairs: two tables and their columns, an index, a foreign key, a CHECK, an
// enum type, a function and a role.
func inventoryDocument() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}, {StructName: "Post", Name: "posts"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
			{StructName: "Post", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Post", Name: "author_id", Type: "INTEGER"},
			{StructName: "Post", Name: "title", Type: "VARCHAR(100)", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "User", Name: "idx_users_email", Fields: []string{"email"}}},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "Post", Table: "posts", Name: "fk_posts_author", Type: "FOREIGN KEY",
				Columns: []string{"author_id"}, ForeignTable: "users", ForeignColumns: []string{"id"},
			},
			{StructName: "Post", Table: "posts", Name: "chk_posts_title", Type: "CHECK", CheckExpression: "title <> ''"},
		},
		Enums:     []schemamodel.Enum{{Name: "mood", Values: []string{"happy", "sad"}}},
		Functions: []schemamodel.Function{{Name: "add_one", Parameters: "i integer", Returns: "integer", Language: "sql", Body: "SELECT i + 1"}},
		Roles:     []schemamodel.Role{{Name: "app_reader"}},
	}
	schemamodel.Finalize(db)
	return db
}

// inventoryWithoutEnums is what a document holds of every kind on a target
// that lowers an enum into the column that names it: there the enum type is
// not an object of its own, and no comparison pairs it.
var inventoryWithoutEnums = []safety.InventoryEntry{
	{Kind: "columns", Desired: 5, Observed: 5},
	{Kind: "constraints", Desired: 2, Observed: 2},
	{Kind: "functions", Desired: 1, Observed: 1},
	{Kind: "indexes", Desired: 1, Observed: 1},
	{Kind: "roles", Desired: 1, Observed: 1},
	{Kind: "tables", Desired: 2, Observed: 2},
}

// inventoryWithEnums adds the enum type and its values to inventoryWithoutEnums.
var inventoryWithEnums = []safety.InventoryEntry{
	{Kind: "columns", Desired: 5, Observed: 5},
	{Kind: "constraints", Desired: 2, Observed: 2},
	{Kind: "enum_values", Desired: 2, Observed: 2},
	{Kind: "enums", Desired: 1, Observed: 1},
	{Kind: "functions", Desired: 1, Observed: 1},
	{Kind: "indexes", Desired: 1, Observed: 1},
	{Kind: "roles", Desired: 1, Observed: 1},
	{Kind: "tables", Desired: 2, Observed: 2},
}

// TestCompareSchemas_ADocumentAgainstItselfCountsEveryKindAlike holds the
// inventory's invariant on every target: a comparison that finds no difference
// counts each kind the same on both sides, and the count is what the document
// declares. The current side is the CREATE result each target's runtime
// predicts for the document, so each row compares under that target's rules.
func TestCompareSchemas_ADocumentAgainstItselfCountsEveryKindAlike(t *testing.T) {
	tests := []struct {
		dialect string
		want    []safety.InventoryEntry
	}{
		{dialect: platform.Postgres, want: inventoryWithEnums},
		{dialect: platform.CockroachDB, want: inventoryWithEnums},
		{dialect: platform.YugabyteDB, want: inventoryWithEnums},
		{dialect: platform.YDB, want: inventoryWithEnums},
		{dialect: platform.MySQL, want: inventoryWithoutEnums},
		{dialect: platform.MariaDB, want: inventoryWithoutEnums},
		{dialect: platform.SQLite, want: inventoryWithoutEnums},
		{dialect: platform.SQLServer, want: inventoryWithoutEnums},
		{dialect: platform.ClickHouse, want: inventoryWithoutEnums},
		{dialect: platform.Oracle, want: inventoryWithoutEnums},
		{dialect: platform.Spanner, want: inventoryWithoutEnums},
	}
	runtime := must.Must(builtin.New())

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareSchemas(t.Context(), inventoryDocument(), inventoryDocument(), test.dialect, runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsFalse)
			c.Assert(safety.Inventory(diff), qt.DeepEquals, test.want)
		})
	}
}
