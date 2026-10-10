package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/migration/planner"
)

// A rendered plan is split one fragment at a time, so each statement keeps the
// node that rendered it. A fragment's statements never run into the next
// fragment's, even when the fragment leaves its last one unterminated or ends a
// SQL Server routine whose body would otherwise reach the end of the batch.
func TestRenderedPlanPlannedStatements(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		fragments []string
		want      []planner.PlannedStatement
	}{
		{
			name:      "each statement keeps its node",
			dialect:   "postgres",
			fragments: []string{"CREATE TABLE a (id int);\nCREATE INDEX i ON a (id);\n", "DROP TABLE b;\n"},
			want: []planner.PlannedStatement{
				{SQL: "CREATE TABLE a (id int)", Node: 0}, {SQL: "CREATE INDEX i ON a (id)", Node: 0}, {SQL: "DROP TABLE b", Node: 1},
			},
		},
		{
			name:      "a comment-only piece joins the next statement and takes its node",
			dialect:   "postgres",
			fragments: []string{"-- note\n", "CREATE TABLE a (id int);\n"},
			want:      []planner.PlannedStatement{{SQL: "-- note\nCREATE TABLE a (id int)", Node: 1}},
		},
		{
			name:      "a comment no statement follows belongs to no node",
			dialect:   "postgres",
			fragments: []string{"CREATE TABLE a (id int);\n", "-- tail\n"},
			want:      []planner.PlannedStatement{{SQL: "CREATE TABLE a (id int)", Node: 0}, {SQL: "-- tail", Node: -1}},
		},
		{
			name:      "an unterminated fragment stays apart",
			dialect:   "postgres",
			fragments: []string{"CREATE TABLE a (id int)", "DROP TABLE b"},
			want:      []planner.PlannedStatement{{SQL: "CREATE TABLE a (id int)", Node: 0}, {SQL: "DROP TABLE b", Node: 1}},
		},
		{
			name:      "an empty fragment renders nothing",
			dialect:   "postgres",
			fragments: []string{"", "DROP TABLE b;"},
			want:      []planner.PlannedStatement{{SQL: "DROP TABLE b", Node: 1}},
		},
		{
			name:      "a SQL Server procedure ends with its fragment",
			dialect:   "sqlserver",
			fragments: []string{"CREATE OR ALTER PROCEDURE [do_it] AS SELECT 1;;\n", "CREATE TABLE [t] ([id] INT);\n"},
			want: []planner.PlannedStatement{
				{SQL: "CREATE OR ALTER PROCEDURE [do_it] AS SELECT 1;;", Node: 0}, {SQL: "CREATE TABLE [t] ([id] INT)", Node: 1},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			plan := planner.RenderedPlan{Result: renderer.Result{Complete: true, Fragments: tc.fragments}, Dialect: tc.dialect}

			c.Assert(plan.PlannedStatements(), qt.DeepEquals, tc.want)
			want := make([]string, 0, len(tc.want))
			for _, statement := range tc.want {
				want = append(want, statement.SQL)
			}
			c.Assert(plan.Statements(), qt.DeepEquals, want)
		})
	}
}
