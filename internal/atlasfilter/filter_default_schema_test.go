package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// defaultSchemaFixture mirrors the PostgreSQL fixture the issue reproduces
// against: objects in the connection's own schema come back from every reader
// with an empty Schema field, objects in a second schema carry theirs.
func defaultSchemaFixture() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "users", Columns: []dbschematypes.DBColumn{{Name: "id"}, {Name: "name"}}},
			{Schema: "app", Name: "orders", Columns: []dbschematypes.DBColumn{{Name: "id"}}},
		},
		Indexes: []dbschematypes.DBIndex{
			{TableName: "users", Name: "users_name_idx", Columns: []string{"name"}},
			{Schema: "app", TableName: "orders", Name: "orders_id_idx", Columns: []string{"id"}},
		},
		Views: []dbschematypes.DBView{
			{Name: "v_users"},
			{Schema: "app", Name: "v_orders"},
		},
		MatViews: []dbschematypes.DBMatView{
			{Name: "mv_users"},
			{Schema: "app", Name: "mv_orders"},
		},
		Extensions: []dbschematypes.DBExtension{
			{Schema: "public", Name: "pgcrypto", Version: "1.3"},
			{Schema: "public", Name: "plpgsql", Version: "1.0"},
		},
	}
}

func defaultSchemaMatViewNames(views []dbschematypes.DBMatView) []string {
	names := make([]string, 0, len(views))
	for _, view := range views {
		names = append(names, view.QualifiedName())
	}
	return names
}

func defaultSchemaExtensionNames(extensions []dbschematypes.DBExtension) []string {
	names := make([]string, 0, len(extensions))
	for _, extension := range extensions {
		names = append(names, extension.Name)
	}
	return names
}

// TestExcludeDatabaseWithDefaultSchema_QualifiedPatternsReachDefaultSchema is
// the object-list form of the issue's completion criteria 1-4. Each row asserts
// what survives the filter rather than an exit code: a silent no-op and a
// correct filter both return a nil error here, so only the surviving objects
// tell them apart.
//
// Reverting the default-schema candidate makes the "qualified, default schema"
// rows fail with the excluded object still listed — exactly the reported bug —
// while the bare-name and non-default-schema rows stay green, which is what
// makes them controls rather than duplicates.
//
// The qualified spelling reaches objects, not their children: a pattern deep
// enough to name a child of a qualified object is refused by
// [atlasfilter.ExcludeDatabaseWithDefaultSchema] rather than matched, so the
// column row here uses the schema-relative spelling. See
// TestExcludeDatabaseWithDefaultSchema_RefusesPatternsDeeperThanTheScope.
func TestExcludeDatabaseWithDefaultSchema_QualifiedPatternsReachDefaultSchema(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		assert  func(*qt.C, *dbschematypes.DBSchema)
	}{
		{
			name:    "qualified default-schema table",
			pattern: "public.users",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"app.orders"})
				c.Assert(indexNames(got.Indexes), qt.DeepEquals, []string{"orders_id_idx"})
			},
		},
		{
			name:    "bare default-schema table stays supported",
			pattern: "users",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"app.orders"})
			},
		},
		{
			name:    "qualified non-default-schema table stays supported",
			pattern: "app.orders",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"users"})
			},
		},
		{
			name:    "qualified default-schema view",
			pattern: "public.v_users",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(viewNames(got.Views), qt.DeepEquals, []string{"app.v_orders"})
			},
		},
		{
			name:    "qualified default-schema materialized view",
			pattern: "public.mv_users",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(defaultSchemaMatViewNames(got.MatViews), qt.DeepEquals, []string{"app.mv_orders"})
			},
		},
		{
			// The column the schema-relative spelling names. The qualified
			// spelling of the same column is a depth error, not a deeper
			// selector; see TestExcludeDatabaseWithDefaultSchema_RefusesPatternsDeeperThanTheScope.
			name:    "schema-relative column",
			pattern: "users.name",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"users", "app.orders"})
				c.Assert(columnNames(got.Tables[0].Columns), qt.DeepEquals, []string{"id"})
				c.Assert(indexNames(got.Indexes), qt.DeepEquals, []string{"orders_id_idx"})
			},
		},
		{
			name:    "qualified extension stays supported",
			pattern: "public.pgcrypto",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(defaultSchemaExtensionNames(got.Extensions), qt.DeepEquals, []string{"plpgsql"})
			},
		},
		{
			name:    "a different schema still matches nothing",
			pattern: "nosuch.users",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"users", "app.orders"})
				c.Assert(viewNames(got.Views), qt.DeepEquals, []string{"v_users", "app.v_orders"})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
				defaultSchemaFixture(), []string{test.pattern}, "public")

			c.Assert(err, qt.IsNil)
			test.assert(c, got)
		})
	}
}

// TestExcludeDatabase_WithoutDefaultSchemaKeepsBareOnlyCandidates is the
// inverse mutant of the test above: with no default schema the qualified
// spelling must go back to matching nothing, and the bare one must still work.
// Without this row, a candidate layer that qualified every object with a
// hard-coded "public" would pass the table above and silently mis-filter every
// non-PostgreSQL target.
//
// Reverting the fix leaves this test green; breaking the "empty default means
// no default" rule turns the first assertion red with "users" already gone.
func TestExcludeDatabase_WithoutDefaultSchemaKeepsBareOnlyCandidates(t *testing.T) {
	c := qt.New(t)

	qualified, err := atlasfilter.ExcludeDatabase(defaultSchemaFixture(), []string{"public.users"})
	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(qualified.Tables), qt.DeepEquals, []string{"users", "app.orders"})

	bare, err := atlasfilter.ExcludeDatabase(defaultSchemaFixture(), []string{"users"})
	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(bare.Tables), qt.DeepEquals, []string{"app.orders"})
}

// TestExcludeGeneratedWithDefaultSchema_MatchesDatabaseSide pins the symmetry
// the comparison depends on. A pattern that removed an object from the
// introspected side but not from the desired side would turn the protected
// object into a CREATE, which is the opposite failure of the one being fixed.
//
// Reverting the fix fails this with the table and the view still present in the
// desired state, while the introspected side of the same pattern is empty.
func TestExcludeGeneratedWithDefaultSchema_MatchesDatabaseSide(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		assert  func(*qt.C, *goschema.Database)
	}{
		{
			name:    "qualified default-schema table",
			pattern: "public.users",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"app.orders"})
				c.Assert(generatedFieldNames(got.Fields), qt.DeepEquals, []string{"Order.id"})
			},
		},
		{
			name:    "qualified default-schema view",
			pattern: "public.v_users",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedViewNames(got.Views), qt.DeepEquals, []string{"app.v_orders"})
			},
		},
		{
			name:    "a different schema still matches nothing",
			pattern: "nosuch.users",
			assert: func(c *qt.C, got *goschema.Database) {
				// goschema.Finalize sorts tables by name; views keep input order.
				c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"app.orders", "users"})
				c.Assert(generatedViewNames(got.Views), qt.DeepEquals, []string{"v_users", "app.v_orders"})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := &goschema.Database{
				Tables: []goschema.Table{
					{StructName: "User", Name: "users"},
					{StructName: "Order", Schema: "app", Name: "orders"},
				},
				Fields: []goschema.Field{
					{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
					{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
				},
				Views: []goschema.View{
					{StructName: "VUsers", Name: "v_users", Body: "SELECT id FROM users"},
					{StructName: "VOrders", Name: "app.v_orders", Body: "SELECT id FROM app.orders"},
				},
			}

			got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(schema, []string{test.pattern}, "public")

			c.Assert(err, qt.IsNil)
			test.assert(c, got)
		})
	}
}

func generatedViewNames(views []goschema.View) []string {
	names := make([]string, 0, len(views))
	for _, view := range views {
		names = append(names, view.Name)
	}
	return names
}
