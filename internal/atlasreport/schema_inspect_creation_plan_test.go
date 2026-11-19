package atlasreport_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/atlasreport"
)

// creationPlanReport is the report of publicSchemaReport's database with a
// creation plan that counts its calls and answers with changes and err.
func creationPlanReport(c *qt.C, calls *int, changes []atlasreport.SchemaChange, err error) *atlasreport.SchemaInspectReport {
	c.Helper()
	db := excludeTable()
	db.Constraints = nil
	return newInspectReport(c,
		db, &catalog.Database{Schemas: []catalog.Schema{{Name: "public"}}},
		catalog.ServerInfo{Dialect: "postgres", Schema: "public"}, nil,
		atlasreport.SchemaInspectReportOptions{
			CreationPlan: func(context.Context) ([]atlasreport.SchemaChange, error) {
				*calls++
				return changes, err
			},
		},
	)
}

// creationPlanChanges is a plan of two changes, each laid out and named as the
// planner writes it.
var creationPlanChanges = []atlasreport.SchemaChange{
	{Cmd: "CREATE TABLE \"t\" (\n  \"id\" integer NOT NULL,\n  PRIMARY KEY (\"id\")\n)", Comment: `create "t" table`},
	{Cmd: `CREATE INDEX "t_id" ON "t" ("id")`, Comment: `create index "t_id" to table: "t"`},
}

// TestSchemaInspectReport_CreationPlan_HappyPath writes the creation plan in
// place of the rendered schema, the way the pinned community binary
// answers `--format '{{ sql . }}'`: each statement under the comment naming its
// change, on one line unless the helper is given an indent.
func TestSchemaInspectReport_CreationPlan_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		format    string
		changes   []atlasreport.SchemaChange
		want      string
		wantCalls int
	}{
		{
			name:    "one statement a line",
			format:  `{{ sql . }}`,
			changes: creationPlanChanges,
			want: "-- Create \"t\" table\n" +
				"CREATE TABLE \"t\" (\"id\" integer NOT NULL, PRIMARY KEY (\"id\"));\n" +
				"-- Create index \"t_id\" to table: \"t\"\n" +
				"CREATE INDEX \"t_id\" ON \"t\" (\"id\");\n",
			wantCalls: 1,
		},
		{
			name:    "an indent lays a statement out",
			format:  `{{ sql . "  " }}`,
			changes: creationPlanChanges[:1],
			want: "-- Create \"t\" table\n" +
				"CREATE TABLE \"t\" (\n  \"id\" integer NOT NULL,\n  PRIMARY KEY (\"id\")\n);\n",
			wantCalls: 1,
		},
		{
			name:      "MarshalSQL writes the plan",
			format:    `{{ printf "%.6s" .MarshalSQL }}`,
			changes:   creationPlanChanges,
			want:      "-- Cre",
			wantCalls: 1,
		},
		{
			name:      "a plan with no change writes nothing",
			format:    `{{ sql . }}`,
			want:      "",
			wantCalls: 1,
		},
		{
			name:      "a template that does not ask for SQL plans nothing",
			format:    `{{ len .Realm.Schemas }}`,
			changes:   creationPlanChanges,
			want:      "1",
			wantCalls: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var calls int
			report := creationPlanReport(c, &calls, test.changes, nil)

			got, err := atlasreport.RenderSchemaInspect(test.format, report)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Text, qt.Equals, test.want)
			c.Assert(calls, qt.Equals, test.wantCalls)
		})
	}
}

// TestSchemaInspectReport_CreationPlan_FailurePath reports a plan that could
// not be made as the template's error, and writes nothing.
func TestSchemaInspectReport_CreationPlan_FailurePath(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("the planner refused")
	var calls int
	report := creationPlanReport(c, &calls, nil, refused)

	got, err := atlasreport.RenderSchemaInspect(`{{ sql . }}`, report)

	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(err, qt.ErrorMatches, `.*plan SQL: the planner refused`)
	c.Assert(got.Text, qt.Equals, "")
	c.Assert(calls, qt.Equals, 1)
}
