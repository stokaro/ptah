package schemaclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemaclean"
)

// schemaWithEveryReportableKind holds one object of every kind that any
// dialect's reader can surface into a cleanup plan. Each dialect case below
// asserts the exact subset its writer destroys, so a dialect that reported a
// kind it does not drop shows up as an extra row rather than as a silent pass.
func schemaWithEveryReportableKind() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "users", Schema: "public"},
			{Name: "posts", Schema: "public"},
		},
		Constraints: []dbschematypes.DBConstraint{
			{Name: "fk_posts_users", Schema: "public", TableName: "posts", Type: "FOREIGN KEY"},
			{Name: "pk_posts", Schema: "public", TableName: "posts", Type: "PRIMARY KEY"},
		},
		Enums:      []dbschematypes.DBEnum{{Name: "mood"}},
		Views:      []dbschematypes.DBView{{Name: "v_users", Schema: "public"}},
		MatViews:   []dbschematypes.DBMatView{{Name: "mv_users", Schema: "public"}},
		Functions:  []dbschematypes.DBFunction{{Name: "f_touch"}},
		Domains:    []dbschematypes.DBDomain{{Name: "d_email", Schema: "public"}},
		Composites: []dbschematypes.DBComposite{{Name: "c_addr", Schema: "public"}},
		Ranges:     []dbschematypes.DBRange{{Name: "r_int", Schema: "public"}},
	}
}

func TestSnapshotWithinWriterScopeKeepsOnlyPostgresSchemaOwnedExtensions(t *testing.T) {
	c := qt.New(t)
	schema := &dbschematypes.DBSchema{Extensions: []dbschematypes.DBExtension{
		{Name: "plpgsql", Schema: "pg_catalog"},
		{Name: "pgcrypto", Schema: "app"},
	}}

	got := schemaclean.SnapshotWithinWriterScope(schema, "postgres", "app")

	c.Assert(got.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{
		{Name: "pgcrypto", Schema: "app"},
	})
	c.Assert(schema.Extensions, qt.HasLen, 2)
}

// TestPlanFromSchemaNamesEveryKindTheDialectWriterDestroys pins each dialect's
// plan coverage to what that dialect's SchemaWriter.DropAllTables really drops.
//
// The sqlserver row is the control: its reader does surface views, and its
// writer does not drop them, so a change that simply listed everything the
// reader returns would fail that row. The clickhouse row is the opposite
// control: its writer drops views and materialized views but no types,
// routines or foreign keys, so a row copied from postgres would fail too.
func TestPlanFromSchemaNamesEveryKindTheDialectWriterDestroys(t *testing.T) {

	tests := []struct {
		name    string
		dialect string
		want    []schemaclean.Object
	}{
		{
			name:    "postgres drops relations, routines, types and foreign keys",
			dialect: "postgres",
			want: []schemaclean.Object{
				{Type: "composite", Schema: "public", Name: "c_addr"},
				{Type: "domain", Schema: "public", Name: "d_email"},
				{Type: "enum", Name: "mood"},
				{Type: "foreign_key", Schema: "public", Table: "posts", Name: "fk_posts_users"},
				{
					Type:    "function",
					Name:    "f_touch",
					Command: `DROP FUNCTION IF EXISTS "f_touch"() RESTRICT`,
				},
				{Type: "materialized_view", Schema: "public", Name: "mv_users"},
				{Type: "range", Schema: "public", Name: "r_int"},
				{Type: "table", Schema: "public", Name: "posts"},
				{Type: "table", Schema: "public", Name: "users"},
				{Type: "view", Schema: "public", Name: "v_users"},
			},
		},
		{
			name:    "cockroachdb shares the postgres writer",
			dialect: "cockroachdb",
			want: []schemaclean.Object{
				{Type: "composite", Schema: "public", Name: "c_addr"},
				{Type: "domain", Schema: "public", Name: "d_email"},
				{Type: "enum", Name: "mood"},
				{Type: "foreign_key", Schema: "public", Table: "posts", Name: "fk_posts_users"},
				{
					Type:    "function",
					Name:    "f_touch",
					Command: `DROP FUNCTION IF EXISTS "f_touch"() RESTRICT`,
				},
				{Type: "materialized_view", Schema: "public", Name: "mv_users"},
				{Type: "range", Schema: "public", Name: "r_int"},
				{Type: "table", Schema: "public", Name: "posts"},
				{Type: "table", Schema: "public", Name: "users"},
				{Type: "view", Schema: "public", Name: "v_users"},
			},
		},
		{
			name:    "mysql drops foreign keys, tables and views but has no standalone types",
			dialect: "mysql",
			want: []schemaclean.Object{
				{Type: "foreign_key", Schema: "public", Table: "posts", Name: "fk_posts_users"},
				{Type: "table", Schema: "public", Name: "posts"},
				{Type: "table", Schema: "public", Name: "users"},
				{Type: "view", Schema: "public", Name: "v_users"},
			},
		},
		{
			name:    "mariadb shares the mysql writer",
			dialect: "mariadb",
			want: []schemaclean.Object{
				{Type: "foreign_key", Schema: "public", Table: "posts", Name: "fk_posts_users"},
				{Type: "table", Schema: "public", Name: "posts"},
				{Type: "table", Schema: "public", Name: "users"},
				{Type: "view", Schema: "public", Name: "v_users"},
			},
		},
		{
			name:    "sqlite drops tables and views only",
			dialect: "sqlite",
			want: []schemaclean.Object{
				{Type: "table", Schema: "public", Name: "posts"},
				{Type: "table", Schema: "public", Name: "users"},
				{Type: "view", Schema: "public", Name: "v_users"},
			},
		},
		{
			name:    "sqlserver drops foreign keys and tables but never views",
			dialect: "sqlserver",
			want: []schemaclean.Object{
				{Type: "foreign_key", Schema: "public", Table: "posts", Name: "fk_posts_users"},
				{Type: "table", Schema: "public", Name: "posts"},
				{Type: "table", Schema: "public", Name: "users"},
			},
		},
		{
			name:    "clickhouse drops tables, views and materialized views",
			dialect: "clickhouse",
			want: []schemaclean.Object{
				{Type: "materialized_view", Schema: "public", Name: "mv_users"},
				{Type: "table", Schema: "public", Name: "posts"},
				{Type: "table", Schema: "public", Name: "users"},
				{Type: "view", Schema: "public", Name: "v_users"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan := schemaclean.PlanFromSchema(schemaWithEveryReportableKind(), test.dialect)

			c.Assert(plan.Objects, qt.DeepEquals, test.want)
		})
	}
}

// TestPlanFromObjectsRendersDialectSpecificDropCommands pins the rendered
// report statement for every object kind a plan can name.
func TestPlanFromObjectsRendersDialectSpecificDropCommands(t *testing.T) {

	tests := []struct {
		name    string
		dialect string
		object  schemaclean.Object
		want    string
	}{
		{
			name:    "postgres view",
			dialect: "postgres",
			object:  schemaclean.Object{Type: "view", Schema: "public", Name: "v_users"},
			want:    `DROP VIEW IF EXISTS "public"."v_users" RESTRICT`,
		},
		{
			name:    "postgres materialized view",
			dialect: "postgres",
			object:  schemaclean.Object{Type: "materialized_view", Schema: "public", Name: "mv_users"},
			want:    `DROP MATERIALIZED VIEW IF EXISTS "public"."mv_users" RESTRICT`,
		},
		{
			name:    "postgres function",
			dialect: "postgres",
			object:  schemaclean.Object{Type: "function", Name: "f_touch"},
			want:    `DROP FUNCTION IF EXISTS "f_touch"() RESTRICT`,
		},
		{
			name:    "postgres domain",
			dialect: "postgres",
			object:  schemaclean.Object{Type: "domain", Schema: "public", Name: "d_email"},
			want:    `DROP DOMAIN IF EXISTS "public"."d_email" RESTRICT`,
		},
		{
			name:    "postgres composite type",
			dialect: "postgres",
			object:  schemaclean.Object{Type: "composite", Schema: "public", Name: "c_addr"},
			want:    `DROP TYPE IF EXISTS "public"."c_addr" RESTRICT`,
		},
		{
			name:    "postgres range type",
			dialect: "postgres",
			object:  schemaclean.Object{Type: "range", Schema: "public", Name: "r_int"},
			want:    `DROP TYPE IF EXISTS "public"."r_int" RESTRICT`,
		},
		{
			name:    "postgres foreign key",
			dialect: "postgres",
			object: schemaclean.Object{
				Type:   "foreign_key",
				Schema: "public",
				Table:  "posts",
				Name:   "fk_posts_users",
			},
			want: `ALTER TABLE "public"."posts" DROP CONSTRAINT "fk_posts_users"`,
		},
		{
			name:    "mysql view",
			dialect: "mysql",
			object:  schemaclean.Object{Type: "view", Name: "v_users"},
			want:    "DROP VIEW IF EXISTS `v_users`",
		},
		{
			name:    "mysql function",
			dialect: "mysql",
			object:  schemaclean.Object{Type: "function", Name: "f_one"},
			want:    "DROP FUNCTION IF EXISTS `f_one`",
		},
		{
			name:    "mysql procedure",
			dialect: "mysql",
			object:  schemaclean.Object{Type: "procedure", Name: "p_noop"},
			want:    "DROP PROCEDURE IF EXISTS `p_noop`",
		},
		{
			name:    "mysql event",
			dialect: "mysql",
			object:  schemaclean.Object{Type: "event", Name: "e_noop"},
			want:    "DROP EVENT IF EXISTS `e_noop`",
		},
		{
			name:    "mysql foreign key uses DROP FOREIGN KEY",
			dialect: "mysql",
			object:  schemaclean.Object{Type: "foreign_key", Table: "posts", Name: "fk_posts_users"},
			want:    "ALTER TABLE `posts` DROP FOREIGN KEY `fk_posts_users`",
		},
		{
			name:    "mariadb sequence",
			dialect: "mariadb",
			object:  schemaclean.Object{Type: "sequence", Name: "s_counter"},
			want:    "DROP SEQUENCE IF EXISTS `s_counter`",
		},
		{
			name:    "postgres sequence uses RESTRICT",
			dialect: "postgres",
			object:  schemaclean.Object{Type: "sequence", Schema: "public", Name: "users_id_seq"},
			want:    `DROP SEQUENCE IF EXISTS "public"."users_id_seq" RESTRICT`,
		},
		{
			name:    "catalog command is preserved exactly",
			dialect: "postgres",
			object: schemaclean.Object{
				Type:    "procedure",
				Schema:  "public",
				Name:    "refresh(integer)",
				Command: `DROP PROCEDURE IF EXISTS "public"."refresh"(integer) RESTRICT`,
			},
			want: `DROP PROCEDURE IF EXISTS "public"."refresh"(integer) RESTRICT`,
		},
		{
			name:    "sqlite view",
			dialect: "sqlite",
			object:  schemaclean.Object{Type: "view", Name: "v_const"},
			want:    `DROP VIEW IF EXISTS "v_const"`,
		},
		{
			name:    "clickhouse view drops synchronously",
			dialect: "clickhouse",
			object:  schemaclean.Object{Type: "view", Name: "v_users"},
			want:    "DROP VIEW IF EXISTS `v_users` SYNC",
		},
		{
			// ClickHouse has no DROP MATERIALIZED VIEW statement at all: that
			// spelling is a syntax error on the server, and DROP VIEW is what
			// removes the view together with the storage table it owns.
			name:    "clickhouse materialized view uses the DROP VIEW spelling",
			dialect: "clickhouse",
			object:  schemaclean.Object{Type: "materialized_view", Name: "mv_users"},
			want:    "DROP VIEW IF EXISTS `mv_users` SYNC",
		},
		{
			name:    "sqlserver foreign key keeps DROP CONSTRAINT",
			dialect: "sqlserver",
			object: schemaclean.Object{
				Type:   "foreign_key",
				Schema: "dbo",
				Table:  "posts",
				Name:   "fk_posts_users",
			},
			want: "ALTER TABLE [dbo].[posts] DROP CONSTRAINT [fk_posts_users]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan := schemaclean.PlanFromObjects([]schemaclean.Object{test.object}, test.dialect)

			c.Assert(plan.Changes, qt.HasLen, 1)
			c.Assert(plan.Changes[0].Cmd, qt.Equals, test.want)
		})
	}
}

func TestPlanFromSchemaPreservesOverloadedFunctionIdentity(t *testing.T) {
	c := qt.New(t)
	integerIdentity := "integer"
	textIdentity := "text"
	emptyIdentity := ""
	schema := &dbschematypes.DBSchema{Functions: []dbschematypes.DBFunction{
		{
			Name:              "normalize",
			Schema:            "app",
			Parameters:        "value integer DEFAULT 1",
			IdentityArguments: &integerIdentity,
		},
		{
			Name:              "normalize",
			Schema:            "app",
			Parameters:        "value text",
			IdentityArguments: &textIdentity,
		},
		{
			Name:              "out_only",
			Schema:            "app",
			Parameters:        "OUT value integer",
			IdentityArguments: &emptyIdentity,
		},
	}}

	plan := schemaclean.PlanFromSchema(schema, "postgres")

	c.Assert(plan.Objects, qt.DeepEquals, []schemaclean.Object{
		{
			Type:       "function",
			Schema:     "app",
			Name:       "normalize",
			Parameters: "value integer DEFAULT 1",
			Command:    `DROP FUNCTION IF EXISTS "app"."normalize"(integer) RESTRICT`,
		},
		{
			Type:       "function",
			Schema:     "app",
			Name:       "normalize",
			Parameters: "value text",
			Command:    `DROP FUNCTION IF EXISTS "app"."normalize"(text) RESTRICT`,
		},
		{
			Type:       "function",
			Schema:     "app",
			Name:       "out_only",
			Parameters: "OUT value integer",
			Command:    `DROP FUNCTION IF EXISTS "app"."out_only"() RESTRICT`,
		},
	})
	c.Assert(plan.Changes, qt.DeepEquals, []schemaclean.Change{
		{
			Type:       "function",
			Schema:     "app",
			Name:       "normalize",
			Parameters: "value integer DEFAULT 1",
			Cmd:        `DROP FUNCTION IF EXISTS "app"."normalize"(integer) RESTRICT`,
		},
		{
			Type:       "function",
			Schema:     "app",
			Name:       "normalize",
			Parameters: "value text",
			Cmd:        `DROP FUNCTION IF EXISTS "app"."normalize"(text) RESTRICT`,
		},
		{
			Type:       "function",
			Schema:     "app",
			Name:       "out_only",
			Parameters: "OUT value integer",
			Cmd:        `DROP FUNCTION IF EXISTS "app"."out_only"() RESTRICT`,
		},
	})
}

// TestPlanFromObjectsOrdersReportByKindThenLocation pins the documented report
// order: kind alphabetically, then schema, table, and name. It is deliberately
// not the order Apply executes in — "table" sorting before "view" puts a view's
// backing table above the view even though the writer drops the view first.
func TestPlanFromObjectsOrdersReportByKindThenLocation(t *testing.T) {
	c := qt.New(t)

	plan := schemaclean.PlanFromObjects([]schemaclean.Object{
		{Type: "view", Schema: "public", Name: "v_users"},
		{Type: "table", Schema: "tenant", Name: "accounts"},
		{Type: "table", Schema: "public", Name: "users"},
		{Type: "materialized_view", Schema: "public", Name: "mv_users"},
		{Type: "foreign_key", Schema: "public", Table: "posts", Name: "fk_posts_users"},
		{Type: "composite", Schema: "public", Name: "c_addr"},
	}, "postgres")

	c.Assert(plan.Objects, qt.DeepEquals, []schemaclean.Object{
		{Type: "composite", Schema: "public", Name: "c_addr"},
		{Type: "foreign_key", Schema: "public", Table: "posts", Name: "fk_posts_users"},
		{Type: "materialized_view", Schema: "public", Name: "mv_users"},
		{Type: "table", Schema: "public", Name: "users"},
		{Type: "table", Schema: "tenant", Name: "accounts"},
		{Type: "view", Schema: "public", Name: "v_users"},
	})
}

func TestPlanFromSchemaIgnoresMySQLColumnEnums(t *testing.T) {
	c := qt.New(t)
	schema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "users"},
		},
		Enums: []dbschematypes.DBEnum{
			{Name: "users_status"},
		},
	}

	plan := schemaclean.PlanFromSchema(schema, "mysql")

	c.Assert(plan.Objects, qt.DeepEquals, []schemaclean.Object{
		{Type: "table", Name: "users"},
	})
	c.Assert(plan.Changes, qt.DeepEquals, []schemaclean.Change{
		{Type: "table", Name: "users", Cmd: "DROP TABLE IF EXISTS `users`"},
	})
}

// TestPlanFromSchemaReportsViewsOnceForReadersThatAlsoListThemAsTables guards
// the one double-count risk of reporting views: a reader that puts a view into
// both DBSchema.Tables (with Type "VIEW") and DBSchema.Views must still yield a
// single view row.
func TestPlanFromSchemaReportsViewsOnceForReadersThatAlsoListThemAsTables(t *testing.T) {
	c := qt.New(t)
	schema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "users", Type: "BASE TABLE"},
			{Name: "v_users", Type: "VIEW"},
		},
		Views: []dbschematypes.DBView{
			{Name: "v_users"},
		},
	}

	plan := schemaclean.PlanFromSchema(schema, "postgres")

	c.Assert(plan.Objects, qt.DeepEquals, []schemaclean.Object{
		{Type: "table", Name: "users"},
		{Type: "view", Name: "v_users"},
	})
}

func TestPlanFromSchemaUsesDialectSpecificTableCommands(t *testing.T) {
	c := qt.New(t)
	schema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "events"},
		},
	}

	postgresPlan := schemaclean.PlanFromSchema(schema, "postgres")
	mysqlPlan := schemaclean.PlanFromSchema(schema, "mysql")
	clickhousePlan := schemaclean.PlanFromSchema(schema, "clickhouse")

	c.Assert(postgresPlan.Changes[0].Cmd, qt.Equals, `DROP TABLE IF EXISTS "events" RESTRICT`)
	c.Assert(mysqlPlan.Changes[0].Cmd, qt.Equals, "DROP TABLE IF EXISTS `events`")
	c.Assert(clickhousePlan.Changes[0].Cmd, qt.Equals, "DROP TABLE IF EXISTS `events` SYNC")
}

func TestPlanFromSchemaAcceptsNilSchema(t *testing.T) {
	c := qt.New(t)

	plan := schemaclean.PlanFromSchema(nil, "sqlite")

	c.Assert(plan.Objects, qt.IsNil)
	c.Assert(plan.Changes, qt.IsNil)
}
