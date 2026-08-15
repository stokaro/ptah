package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/deporder"
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

func TestTablesForCreate_KeepsLiteralDotAndQualifiedTablesDistinct(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{Tables: []goschema.Table{
		{StructName: "Literal", Name: "tenant.data"},
		{StructName: "Qualified", Schema: "tenant", Name: "data"},
	}}

	tables := deporder.TablesForCreate(schema, []string{`"tenant.data"`, "tenant.data"})

	c.Assert(tables, qt.DeepEquals, schema.Tables)
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

func TestViewLikesForCreateForDialect_ClickHouseCanonicalReferences(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "backtick qualified", body: "SELECT id FROM `analytics`.`z_base`"},
		{name: "unqualified", body: "SELECT id FROM z_base"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects := deporder.ViewLikesForCreateForDialect([]deporder.ViewLike{
				{Name: "analytics.a_report", Body: test.body},
				{Name: "analytics.z_base", Body: "SELECT id FROM users"},
			}, platform.ClickHouse)

			c.Assert(viewLikeNames(objects), qt.DeepEquals, []string{"analytics.z_base", "analytics.a_report"})
		})
	}
}

func TestViewLikesForCreateForDialect_ClickHouseIgnoresMaskedCanonicalReferences(t *testing.T) {
	c := qt.New(t)

	objects := deporder.ViewLikesForCreateForDialect([]deporder.ViewLike{
		{
			Name: "analytics.a_report",
			Body: "SELECT '`analytics`.`z_base`' AS label FROM users " +
				"/* `analytics`.`z_base` */ -- z_base\n",
		},
		{Name: "analytics.z_base", Body: "SELECT id FROM users"},
	}, platform.ClickHouse)

	c.Assert(viewLikeNames(objects), qt.DeepEquals, []string{"analytics.a_report", "analytics.z_base"})
}

func TestViewLikesForCreateForDialect_ClickHouseLineCommentsDoNotCreateFalseCycles(t *testing.T) {
	tests := []struct {
		name    string
		comment string
	}{
		{name: "hash", comment: "# `analytics`.`a_report`"},
		{name: "hash bang", comment: "#! `analytics`.`a_report`"},
		{name: "slash slash", comment: "// `analytics`.`a_report`"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects := deporder.ViewLikesForCreateForDialect([]deporder.ViewLike{
				{
					Name: "analytics.a_report",
					Body: "SELECT id FROM `analytics`.`z_base`",
				},
				{
					Name: "analytics.z_base",
					Body: "SELECT 1 AS id " + test.comment + "\n",
				},
			}, platform.ClickHouse)

			c.Assert(viewLikeNames(objects), qt.DeepEquals, []string{"analytics.z_base", "analytics.a_report"})
		})
	}
}

func TestViewLikesForCreateForDialect_ClickHouseEscapesDoNotCreateFalseCycles(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "ordinary string", body: `SELECT 'it\'s a_report' AS label`},
		{name: "double quoted identifier", body: `SELECT 1 AS "odd\" a_report"`},
		{name: "backtick quoted identifier", body: "SELECT 1 AS `odd\\` a_report`"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects := deporder.ViewLikesForCreateForDialect([]deporder.ViewLike{
				{
					Name: "analytics.a_report",
					Body: "SELECT id FROM `analytics`.`z_base`",
				},
				{Name: "analytics.z_base", Body: test.body},
			}, platform.ClickHouse)

			c.Assert(viewLikeNames(objects), qt.DeepEquals, []string{"analytics.z_base", "analytics.a_report"})
		})
	}
}

// TestReferencesIdentifier_ReadsCodeOnly pins what counts as a reference.
//
// The PostgreSQL planner asks this question to work out what
// DROP VIEW ... CASCADE takes with it, and every "yes" puts a
// DROP MATERIALIZED VIEW ... CASCADE or a CREATE OR REPLACE VIEW into the plan
// for the named object. A name spelled inside a string literal or a comment
// refers to nothing, so answering "yes" there is a destructive statement issued
// against an object that had no part in the change.
func TestReferencesIdentifier_ReadsCodeOnly(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "plain reference",
			body: "SELECT id FROM base_view",
			want: true,
		},
		{
			name: "quoted reference",
			body: `SELECT id FROM "base_view"`,
			want: true,
		},
		{
			name: "string literal only",
			body: "SELECT 'base_view' AS label, count(*) AS total FROM accounts",
			want: false,
		},
		{
			name: "string literal with a doubled quote before it",
			body: "SELECT 'it''s base_view' AS label FROM accounts",
			want: false,
		},
		{
			name: "escape string literal with a backslash-escaped quote",
			body: `SELECT E'\'base_view' AS label FROM accounts`,
			want: false,
		},
		{
			name: "line comment only",
			body: "SELECT id FROM accounts -- used to read base_view\n",
			want: false,
		},
		{
			name: "block comment only",
			body: "SELECT id /* base_view was here */ FROM accounts",
			want: false,
		},
		{
			name: "nested block comment only",
			body: "SELECT id /* outer /* base_view */ still comment */ FROM accounts",
			want: false,
		},
		{
			name: "dollar quoted body only",
			body: "SELECT fn($tag$ SELECT id FROM base_view $tag$) FROM accounts",
			want: false,
		},
		{
			name: "code after a closed literal",
			body: "SELECT 'base_view' AS label FROM base_view",
			want: true,
		},
		{
			name: "code after a closed line comment",
			body: "SELECT id -- base_view\nFROM base_view",
			want: true,
		},
		{
			name: "positional parameter is not a dollar quote",
			body: "SELECT id FROM base_view WHERE id = $1",
			want: true,
		},
		{
			name: "comment marker inside a quoted identifier does not start a comment",
			body: `SELECT id FROM "od--d", base_view`,
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(deporder.ReferencesIdentifier(tc.body, "base_view"), qt.Equals, tc.want,
				qt.Commentf("body: %s", tc.body))
		})
	}
}

// TestViewLikesForCreate_IgnoresNamesInsideLiteralsAndComments is the ordering
// half of the same rule: a literal that happens to spell another object's name
// must not create a dependency edge between them.
func TestViewLikesForCreate_IgnoresNamesInsideLiteralsAndComments(t *testing.T) {
	c := qt.New(t)

	objects := deporder.ViewLikesForCreate([]deporder.ViewLike{
		{Name: "a_report", Body: "SELECT 'z_base' AS label FROM users /* z_base */"},
		{Name: "z_base", Body: "SELECT id FROM users", Materialized: true},
	})

	c.Assert(viewLikeNames(objects), qt.DeepEquals, []string{"a_report", "z_base"})
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
