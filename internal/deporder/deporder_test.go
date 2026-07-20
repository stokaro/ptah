package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/deporder"
)

func TestStableTopologicalSort_OrdersDependenciesBeforeDependents(t *testing.T) {
	c := qt.New(t)

	ordered := deporder.StableTopologicalSort(
		[]string{"tasks", "memberships", "projects", "accounts"},
		map[string][]string{
			"tasks":       {"projects"},
			"projects":    {"accounts"},
			"memberships": {"accounts"},
		},
	)

	c.Assert(ordered, qt.DeepEquals, []string{"accounts", "memberships", "projects", "tasks"})
}

func TestStableTopologicalSort_CycleFallsBackToCallerOrder(t *testing.T) {
	c := qt.New(t)

	ordered := deporder.StableTopologicalSort(
		[]string{"a", "b", "c"},
		map[string][]string{
			"a": {"b"},
			"b": {"a"},
		},
	)

	c.Assert(ordered, qt.DeepEquals, []string{"c", "a", "b"})
}

func TestStableReverseDependencySort_OrdersDependentsBeforeParents(t *testing.T) {
	c := qt.New(t)

	ordered := deporder.StableReverseDependencySort(
		[]string{"accounts", "projects", "memberships", "tasks"},
		map[string][]string{
			"tasks":       {"projects"},
			"projects":    {"accounts"},
			"memberships": {"accounts"},
		},
	)

	c.Assert(ordered, qt.DeepEquals, []string{"tasks", "projects", "memberships", "accounts"})
}

func TestTablesForCreate_DerivesForeignKeyDependencies(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Task", Name: "tasks"},
			{StructName: "Project", Name: "projects"},
			{StructName: "Account", Name: "accounts"},
		},
		Fields: []goschema.Field{
			{StructName: "Task", Name: "project_id", Foreign: "projects(id)"},
			{StructName: "Project", Name: "account_id", Foreign: "accounts(id)"},
		},
	}

	tables := deporder.TablesForCreate(schema, []string{"tasks", "projects", "accounts"})

	c.Assert(tableNames(tables), qt.DeepEquals, []string{"accounts", "projects", "tasks"})
}

func TestTableDropOrder_DerivesForeignKeyDependencies(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Account", Name: "accounts"},
			{StructName: "Project", Name: "projects"},
			{StructName: "Task", Name: "tasks"},
		},
		Fields: []goschema.Field{
			{StructName: "Task", Name: "project_id", Foreign: "projects(id)"},
			{StructName: "Project", Name: "account_id", Foreign: "accounts(id)"},
		},
	}

	ordered := deporder.TableDropOrder([]string{"accounts", "projects", "tasks"}, schema)

	c.Assert(ordered, qt.DeepEquals, []string{"tasks", "projects", "accounts"})
}

func TestTablesForCreate_ResolvesUnqualifiedForeignKeyWithinCurrentSchema(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "AuditAccount", Schema: "audit", Name: "accounts"},
			{StructName: "AppProject", Schema: "app", Name: "projects"},
			{StructName: "AppAccount", Schema: "app", Name: "accounts"},
		},
		Fields: []goschema.Field{
			{StructName: "AppProject", Name: "account_id", Foreign: "accounts(id)"},
		},
	}

	tables := deporder.TablesForCreate(schema, []string{"app.projects", "app.accounts", "audit.accounts"})

	c.Assert(qualifiedTableNames(tables), qt.DeepEquals, []string{"app.accounts", "app.projects", "audit.accounts"})
}

func TestFunctionsForCreate_UsesFunctionDependencyMap(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Functions: []goschema.Function{
			{Name: "a_child"},
			{Name: "z_parent"},
		},
		FunctionDependencies: map[string][]string{
			"a_child": {"z_parent"},
		},
	}

	functions := deporder.FunctionsForCreate(schema, []string{"a_child", "z_parent"})

	c.Assert(functionNames(functions), qt.DeepEquals, []string{"z_parent", "a_child"})
}

func TestFunctionsForCreate_FallsBackToGeneratedFunctionOrder(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Functions: []goschema.Function{
			{Name: "z_parent"},
			{Name: "a_child"},
		},
	}

	functions := deporder.FunctionsForCreate(schema, []string{"a_child", "z_parent"})

	c.Assert(functionNames(functions), qt.DeepEquals, []string{"z_parent", "a_child"})
}

func TestViewLikesForCreate_OrdersMaterializedViewBeforeDependentView(t *testing.T) {
	c := qt.New(t)

	objects := deporder.ViewLikesForCreate([]deporder.ViewLike{
		{Name: "a_report", Body: "SELECT id FROM z_base"},
		{Name: "z_base", Body: "SELECT id FROM users", Materialized: true},
	})

	c.Assert(viewLikeNames(objects), qt.DeepEquals, []string{"z_base", "a_report"})
	c.Assert(objects[0].Materialized, qt.IsTrue)
}

func TestViewLikesForCreate_DoesNotMatchIdentifierSubstrings(t *testing.T) {
	c := qt.New(t)

	objects := deporder.ViewLikesForCreate([]deporder.ViewLike{
		{Name: "a_report", Body: "SELECT id FROM z_baseline"},
		{Name: "z_base", Body: "SELECT id FROM users", Materialized: true},
	})

	c.Assert(viewLikeNames(objects), qt.DeepEquals, []string{"a_report", "z_base"})
}

func TestViewLikesForCreate_MatchesSchemaQualifiedReferences(t *testing.T) {
	c := qt.New(t)

	for _, body := range []string{
		"SELECT id FROM public.z_base",
		`SELECT id FROM "public"."z_base"`,
	} {
		objects := deporder.ViewLikesForCreate([]deporder.ViewLike{
			{Name: "a_report", Body: body},
			{Name: "z_base", Body: "SELECT id FROM users", Materialized: true},
		})

		c.Assert(viewLikeNames(objects), qt.DeepEquals, []string{"z_base", "a_report"}, qt.Commentf("body: %s", body))
		c.Assert(objects[0].Materialized, qt.IsTrue)
	}
}

func tableNames(tables []goschema.Table) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}

func qualifiedTableNames(tables []goschema.Table) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.QualifiedName())
	}
	return names
}

func functionNames(functions []goschema.Function) []string {
	names := make([]string, 0, len(functions))
	for _, fn := range functions {
		names = append(names, fn.Name)
	}
	return names
}

func viewLikeNames(objects []deporder.ViewLike) []string {
	names := make([]string, 0, len(objects))
	for _, object := range objects {
		names = append(names, object.Name)
	}
	return names
}
