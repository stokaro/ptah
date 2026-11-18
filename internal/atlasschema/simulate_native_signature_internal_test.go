package atlasschema

// White-box testing required: the rehearsal holds the plan it makes for the
// dev database to the target's in sameOperations, and the exported path
// reaches a pair of plans that differ only where the dev database's rebuilt
// state differs from the target's, which the rehearsal exists to prevent.
// These rows hand it the two plans directly.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
)

// signedOperation is an owner operation the assessment names by the object
// its schema change reports.
type signedOperation struct{ name string }

func (signedOperation) Kind() schemaext.Kind                   { return "example.org/signed-operation" }
func (o signedOperation) CloneExtension() ast.ExtensionPayload { return o }
func (o signedOperation) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: o.name}
}

// mixedPlan is a plan of a native operation on the object named, then the SQL
// unit statement, made on the database info describes.
func mixedPlan(info catalog.ServerInfo, named, statement string) plannedOn {
	planned := sqlPlan(info, statement)
	planned.plan.units = append([]renderer.Fragment{{Native: signedOperation{name: named}}}, planned.plan.units...)
	planned.plan.owned = append(planned.plan.owned, safety.StatementAssessment{})
	return planned
}

// sqlPlan is a plan of one SQL unit per statement, each reading the approved
// input PTAH_REHEARSAL_INPUT, made on the database info describes.
func sqlPlan(info catalog.ServerInfo, statements ...string) plannedOn {
	plan := ApplyPlan{target: info.Dialect, owned: make([]safety.StatementAssessment, len(statements))}
	for _, statement := range statements {
		plan.units = append(plan.units, renderer.Fragment{
			SQL:    statement,
			Inputs: []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "PTAH_REHEARSAL_INPUT"}},
		})
	}
	return plannedOn{plan: plan, info: info}
}

func TestSameOperations_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		target plannedOn
		dev    plannedOn
	}{
		{
			name:   "one PostgreSQL statement on both",
			target: sqlPlan(catalog.ServerInfo{Dialect: "postgres", Schema: "public"}, `ALTER TABLE "public"."notes" ADD COLUMN "body" text`),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "postgres", Schema: "public"}, `ALTER TABLE "public"."notes" ADD COLUMN "body" text`),
		},
		{
			name:   "a MySQL table named by the database each plan runs in",
			target: sqlPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app"}, "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app_dev"}, "ALTER TABLE `app_dev`.`notes` ADD COLUMN `body` text"),
		},
		{
			name:   "a MariaDB table named by the database each plan runs in",
			target: sqlPlan(catalog.ServerInfo{Dialect: "mariadb", Schema: "app"}, "ALTER TABLE `app`.`notes` DROP COLUMN `body`"),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "mariadb", Schema: "app_dev"}, "ALTER TABLE `app_dev`.`notes` DROP COLUMN `body`"),
		},
		{
			name:   "a MySQL table of a whole server, named by its database on both",
			target: sqlPlan(catalog.ServerInfo{Dialect: "mysql"}, "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "mysql"}, "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
		},
		{
			name:   "one SQLite statement on both",
			target: sqlPlan(catalog.ServerInfo{Dialect: "sqlite"}, "CREATE TABLE notes (id INTEGER)"),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "sqlite"}, "CREATE TABLE notes (id INTEGER)"),
		},
		{
			name:   "a native operation beside an SQL unit on both",
			target: mixedPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app"}, "app.thing", "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
			dev:    mixedPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app_dev"}, "app.thing", "ALTER TABLE `app_dev`.`notes` ADD COLUMN `body` text"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sameOperations(c.Context(), must.Must(builtin.New()), test.target, test.dev), qt.IsNil)
		})
	}
}

func TestSameOperations_FailurePath(t *testing.T) {
	postgres := catalog.ServerInfo{Dialect: "postgres", Schema: "public"}
	for _, test := range []struct {
		name   string
		target plannedOn
		dev    plannedOn
		want   string
	}{
		{
			name:   "a PostgreSQL table altered on the target and created on the dev database",
			target: sqlPlan(postgres, `ALTER TABLE "public"."notes" ADD COLUMN "body" text`),
			dev:    sqlPlan(postgres, `CREATE TABLE "public"."notes" ("id" bigint, "body" text)`),
			want: `the plan made for the dev database is not the plan for the target: ` +
				`unit 1 is ALTER TABLE ADD COLUMN body TEXT of public.notes on the target and CREATE TABLE of public.notes on the dev database`,
		},
		{
			name:   "a PostgreSQL column added beside another one",
			target: sqlPlan(postgres, `ALTER TABLE "public"."notes" ADD COLUMN "body" text`),
			dev:    sqlPlan(postgres, `ALTER TABLE "public"."notes" ADD COLUMN "title" text`),
			want: `the plan made for the dev database is not the plan for the target: ` +
				`unit 1 is ALTER TABLE ADD COLUMN body TEXT of public.notes on the target and ` +
				`ALTER TABLE ADD COLUMN title TEXT of public.notes on the dev database`,
		},
		{
			name:   "a PostgreSQL column added to another table",
			target: sqlPlan(postgres, `ALTER TABLE "public"."notes" ADD COLUMN "body" text`),
			dev:    sqlPlan(postgres, `ALTER TABLE "public"."tags" ADD COLUMN "body" text`),
			want: `the plan made for the dev database is not the plan for the target: ` +
				`unit 1 is ALTER TABLE ADD COLUMN body TEXT of public.notes on the target and ` +
				`ALTER TABLE ADD COLUMN body TEXT of public.tags on the dev database`,
		},
		{
			name:   "a MySQL table of another database",
			target: sqlPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app"}, "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app_dev"}, "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
			want: `the plan made for the dev database is not the plan for the target: ` +
				`unit 1 is ALTER TABLE ADD COLUMN body TEXT of notes on the target and ` +
				`ALTER TABLE ADD COLUMN body TEXT of app.notes on the dev database`,
		},
		{
			name:   "a MySQL table whose one name holds the database's name and a dot",
			target: sqlPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app"}, "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app"}, "ALTER TABLE `app.notes` ADD COLUMN `body` text"),
			want: `the plan made for the dev database is not the plan for the target: ` +
				`unit 1 is ALTER TABLE ADD COLUMN body TEXT of notes on the target and ` +
				`ALTER TABLE ADD COLUMN body TEXT of "app.notes" on the dev database`,
		},
		{
			name:   "a MySQL table of a whole server, named by another database on the dev server",
			target: sqlPlan(catalog.ServerInfo{Dialect: "mysql"}, "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "mysql"}, "ALTER TABLE `app_dev`.`notes` ADD COLUMN `body` text"),
			want: `the plan made for the dev database is not the plan for the target: ` +
				`unit 1 is ALTER TABLE ADD COLUMN body TEXT of app.notes on the target and ` +
				`ALTER TABLE ADD COLUMN body TEXT of app_dev.notes on the dev database`,
		},
		{
			name:   "a native operation on another object beside the same SQL unit",
			target: mixedPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app"}, "app.thing", "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
			dev:    mixedPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app_dev"}, "app.other", "ALTER TABLE `app_dev`.`notes` ADD COLUMN `body` text"),
			want: `the plan made for the dev database is not the plan for the target: ` +
				`unit 1 is example.org/signed-operation of app.thing on the target and ` +
				`example.org/signed-operation of app.other on the dev database`,
		},
		{
			name:   "a SQLite index where the target creates a table",
			target: sqlPlan(catalog.ServerInfo{Dialect: "sqlite"}, "CREATE TABLE notes (id INTEGER)"),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "sqlite"}, "CREATE INDEX notes ON tags (id)"),
			want: `the plan made for the dev database is not the plan for the target: ` +
				`unit 1 is CREATE TABLE of notes on the target and CREATE INDEX of notes on the dev database`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sameOperations(c.Context(), must.Must(builtin.New()), test.target, test.dev), qt.ErrorMatches, test.want)
		})
	}
}
