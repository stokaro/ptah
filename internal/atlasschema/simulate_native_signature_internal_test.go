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
	"ptah.run/core/renderer"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
)

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
			name:   "one SQLite statement on both",
			target: sqlPlan(catalog.ServerInfo{Dialect: "sqlite"}, "CREATE TABLE notes (id INTEGER)"),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "sqlite"}, "CREATE TABLE notes (id INTEGER)"),
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
				`unit 1 is ALTER TABLE of public.notes on the target and CREATE TABLE of public.notes on the dev database`,
		},
		{
			name:   "a PostgreSQL column added to another table",
			target: sqlPlan(postgres, `ALTER TABLE "public"."notes" ADD COLUMN "body" text`),
			dev:    sqlPlan(postgres, `ALTER TABLE "public"."tags" ADD COLUMN "body" text`),
			want: `the plan made for the dev database is not the plan for the target: ` +
				`unit 1 is ALTER TABLE of public.notes on the target and ALTER TABLE of public.tags on the dev database`,
		},
		{
			name:   "a MySQL table of another database",
			target: sqlPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app"}, "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
			dev:    sqlPlan(catalog.ServerInfo{Dialect: "mysql", Schema: "app_dev"}, "ALTER TABLE `app`.`notes` ADD COLUMN `body` text"),
			want: `the plan made for the dev database is not the plan for the target: ` +
				`unit 1 is ALTER TABLE of notes on the target and ALTER TABLE of app.notes on the dev database`,
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
