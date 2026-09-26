package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// rewrittenObjects declares an enum, a materialized view, a function, a
// trigger and a policy in the schema app, each with comment, and each changed
// from what rewrittenInDatabase reports so that the plan writes it again: the
// enum loses a value, the view and the policy change their query, the
// function its body and the trigger its timing.
func rewrittenObjects(comment string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", Schema: "app"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INTEGER", Primary: true}},
		Enums:  []schemamodel.Enum{{Name: "e", Schema: "app", Values: []string{"a"}, Comment: comment}},
		MaterializedViews: []schemamodel.MaterializedView{{
			Name: "app.m", Body: "SELECT 2 AS n", Comment: comment,
		}},
		Functions: []schemamodel.Function{{
			Name: "app.f", Parameters: "a integer", Returns: "integer", Language: "sql",
			Security: "INVOKER", Volatility: "VOLATILE", Body: "SELECT 2", Comment: comment,
		}},
		Triggers: []schemamodel.Trigger{{
			Name: "tg", Table: "app.t", Timing: "AFTER", Event: "INSERT", ForEach: "ROW",
			ExecuteFunction: "app.touch", Comment: comment,
		}},
		RLSPolicies: []schemamodel.RLSPolicy{{
			Name: "pol", Table: "app.t", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "(id > 0)", Comment: comment,
		}},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "app.t"}},
	}
}

// rewrittenInDatabase is the database rewrittenObjects changes, with comment
// on each object.
func rewrittenInDatabase(comment string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "t", Schema: "app", Type: "TABLE", RLSEnabled: true,
			Columns: []catalog.Column{{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1}},
		}},
		Enums:    []catalog.Enum{{Name: "e", Schema: "app", Values: []string{"a", "b"}, Comment: comment}},
		MatViews: []catalog.MaterializedView{{Name: "m", Schema: "app", Body: "SELECT 1 AS n", Comment: comment}},
		Functions: []catalog.Function{{
			Name: "f", Schema: "app", Parameters: "a integer", IdentityArguments: new("a integer"),
			Returns: "integer", Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: "SELECT 1", Comment: comment,
		}},
		Triggers: []catalog.Trigger{{
			Name: "tg", Schema: "app", Table: "t", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW",
			ExecuteFunction: "app.touch", Comment: comment,
		}},
		RLSPolicies: []catalog.RLSPolicy{{
			Name: "pol", Table: "app.t", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "true", Comment: comment,
		}},
	}
}

// commentLines lists every COMMENT ON line of a rendered plan.
func commentLines(sql string) []string {
	var lines []string
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.HasPrefix(line, "COMMENT ON") {
			lines = append(lines, line)
		}
	}
	return lines
}

// An object the plan writes again ends with the comment the declaration
// states, and the plan says so once (stokaro/ptah#3646).
//
// The routine and the trigger are replaced in place and keep the comment they
// had, so a kept or a changed comment is written after the replacement and a
// removed one is cleared. The enum, the materialized view and the policy are
// new objects after the plan, so a kept or a changed comment is written with
// them and a removed one needs nothing. The enum is the one no create node
// describes: it is rebuilt with a rename, a CREATE TYPE and a drop, and
// without its own COMMENT ON the new type has none while the comparison,
// which found the two comments equal, asks for nothing.
func TestGenerateSchemaDiffSQL_ARewrittenObjectEndsWithTheDeclaredComment(t *testing.T) {
	tests := []struct {
		name     string
		current  string
		declared string
		want     []string
	}{
		{
			name: "the comment kept", current: "same", declared: "same",
			want: []string{
				`COMMENT ON FUNCTION "app"."f"(a integer) IS 'same';`,
				`COMMENT ON TYPE "app"."e" IS 'same';`,
				`COMMENT ON MATERIALIZED VIEW "app"."m" IS 'same';`,
				`COMMENT ON TRIGGER "tg" ON "app"."t" IS 'same';`,
				`COMMENT ON POLICY "pol" ON "app"."t" IS 'same';`,
			},
		},
		{
			name: "the comment changed", current: "old", declared: "new",
			want: []string{
				`COMMENT ON FUNCTION "app"."f"(a integer) IS 'new';`,
				`COMMENT ON TYPE "app"."e" IS 'new';`,
				`COMMENT ON MATERIALIZED VIEW "app"."m" IS 'new';`,
				`COMMENT ON TRIGGER "tg" ON "app"."t" IS 'new';`,
				`COMMENT ON POLICY "pol" ON "app"."t" IS 'new';`,
			},
		},
		{
			name: "the comment removed", current: "old", declared: "",
			want: []string{
				`COMMENT ON FUNCTION "app"."f"(a integer) IS NULL;`,
				`COMMENT ON TRIGGER "tg" ON "app"."t" IS NULL;`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(rewrittenObjects(test.declared), rewrittenInDatabase(test.current), platform.Postgres)

			sql, err := planner.GenerateSchemaDiffSQL(diff, platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(commentLines(sql), qt.DeepEquals, test.want, qt.Commentf("plan:\n%s", sql))
		})
	}
}

// A comment follows the statement that writes its object: a COMMENT ON
// before a DROP of the same object is lost with it. The policy is the last
// object a plan writes again, after the step that sets the comments of the
// objects it keeps.
func TestGenerateSchemaDiffSQL_ARecreatedObjectsCommentFollowsItsCreate(t *testing.T) {
	tests := []struct {
		name    string
		create  string
		comment string
	}{
		{name: "the enum", create: `CREATE TYPE "app"."e" AS ENUM`, comment: `COMMENT ON TYPE "app"."e"`},
		{name: "the materialized view", create: `CREATE MATERIALIZED VIEW "app"."m"`, comment: `COMMENT ON MATERIALIZED VIEW "app"."m"`},
		{name: "the policy", create: `CREATE POLICY "pol" ON "app"."t"`, comment: `COMMENT ON POLICY "pol" ON "app"."t"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(rewrittenObjects("new"), rewrittenInDatabase("old"), platform.Postgres)

			sql, err := planner.GenerateSchemaDiffSQL(diff, platform.Postgres)

			c.Assert(err, qt.IsNil)
			create := strings.Index(sql, test.create)
			c.Assert(create, qt.Not(qt.Equals), -1, qt.Commentf("plan:\n%s", sql))
			c.Assert(strings.Index(sql, test.comment) > create, qt.IsTrue, qt.Commentf("plan:\n%s", sql))
		})
	}
}

// An object the plan creates is written with its comment, once, whatever
// path builds its create node: the enum through the enum step, the routine,
// the view, the trigger and the policy through their own.
func TestGenerateSchemaDiffSQL_ACreatedObjectIsWrittenWithItsComment(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{Tables: rewrittenInDatabase("").Tables}
	diff := schemadiff.CompareWithDialect(rewrittenObjects("note"), database, platform.Postgres)

	sql, err := planner.GenerateSchemaDiffSQL(diff, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(commentLines(sql), qt.DeepEquals, []string{
		`COMMENT ON FUNCTION "app"."f"(a integer) IS 'note';`,
		`COMMENT ON TYPE "app"."e" IS 'note';`,
		`COMMENT ON MATERIALIZED VIEW "app"."m" IS 'note';`,
		`COMMENT ON TRIGGER "tg" ON "app"."t" IS 'note';`,
		`COMMENT ON POLICY "pol" ON "app"."t" IS 'note';`,
	}, qt.Commentf("plan:\n%s", sql))
}
