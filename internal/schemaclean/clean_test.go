package schemaclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/schemaclean"
)

func TestPlanFromSchemaBuildsDeterministicSupportedObjectChanges(t *testing.T) {
	c := qt.New(t)
	schema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "users", Schema: "public"},
			{Name: "accounts", Schema: "tenant"},
		},
		Enums: []dbschematypes.DBEnum{
			{Name: "status"},
		},
		Views: []dbschematypes.DBView{
			{Name: "active_users", Schema: "public"},
		},
		Functions: []dbschematypes.DBFunction{
			{Name: "set_context"},
		},
	}

	plan := schemaclean.PlanFromSchema(schema, "postgres")

	c.Assert(plan.Objects, qt.DeepEquals, []schemaclean.Object{
		{Type: "enum", Name: "status"},
		{Type: "table", Schema: "public", Name: "users"},
		{Type: "table", Schema: "tenant", Name: "accounts"},
	})
	c.Assert(plan.Changes, qt.DeepEquals, []schemaclean.Change{
		{Type: "enum", Name: "status", Cmd: `DROP TYPE IF EXISTS "status" CASCADE`},
		{Type: "table", Schema: "public", Name: "users", Cmd: `DROP TABLE IF EXISTS "public"."users" CASCADE`},
		{Type: "table", Schema: "tenant", Name: "accounts", Cmd: `DROP TABLE IF EXISTS "tenant"."accounts" CASCADE`},
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

func TestPlanFromObjectsSupportsPostgreSQLSequences(t *testing.T) {
	c := qt.New(t)

	plan := schemaclean.PlanFromObjects([]schemaclean.Object{
		{Type: "sequence", Schema: "public", Name: "users_id_seq"},
	}, "postgres")

	c.Assert(plan.Objects, qt.DeepEquals, []schemaclean.Object{
		{Type: "sequence", Schema: "public", Name: "users_id_seq"},
	})
	c.Assert(plan.Changes, qt.DeepEquals, []schemaclean.Change{
		{
			Type:   "sequence",
			Schema: "public",
			Name:   "users_id_seq",
			Cmd:    `DROP SEQUENCE IF EXISTS "public"."users_id_seq" CASCADE`,
		},
	})
}

func TestPlanFromSchemaReportsSQLServerForeignKeyCleanup(t *testing.T) {
	c := qt.New(t)
	schema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "users", Schema: "dbo"},
			{Name: "posts", Schema: "dbo"},
		},
		Constraints: []dbschematypes.DBConstraint{
			{Name: "fk_posts_users", Schema: "dbo", TableName: "posts", Type: "FOREIGN KEY"},
			{Name: "pk_posts", Schema: "dbo", TableName: "posts", Type: "PRIMARY KEY"},
		},
	}

	plan := schemaclean.PlanFromSchema(schema, "sqlserver")

	c.Assert(plan.Objects, qt.DeepEquals, []schemaclean.Object{
		{Type: "foreign_key", Schema: "dbo", Table: "posts", Name: "fk_posts_users"},
		{Type: "table", Schema: "dbo", Name: "posts"},
		{Type: "table", Schema: "dbo", Name: "users"},
	})
	c.Assert(plan.Changes, qt.DeepEquals, []schemaclean.Change{
		{
			Type:   "foreign_key",
			Schema: "dbo",
			Table:  "posts",
			Name:   "fk_posts_users",
			Cmd:    "ALTER TABLE [dbo].[posts] DROP CONSTRAINT [fk_posts_users]",
		},
		{Type: "table", Schema: "dbo", Name: "posts", Cmd: "DROP TABLE IF EXISTS [dbo].[posts]"},
		{Type: "table", Schema: "dbo", Name: "users", Cmd: "DROP TABLE IF EXISTS [dbo].[users]"},
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

	c.Assert(postgresPlan.Changes[0].Cmd, qt.Equals, `DROP TABLE IF EXISTS "events" CASCADE`)
	c.Assert(mysqlPlan.Changes[0].Cmd, qt.Equals, "DROP TABLE IF EXISTS `events`")
	c.Assert(clickhousePlan.Changes[0].Cmd, qt.Equals, "DROP TABLE IF EXISTS `events` SYNC")
}

func TestPlanFromSchemaAcceptsNilSchema(t *testing.T) {
	c := qt.New(t)

	plan := schemaclean.PlanFromSchema(nil, "sqlite")

	c.Assert(plan, qt.DeepEquals, schemaclean.Plan{})
}
