package atlasschema_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/risk"
	"ptah.run/migration/safety"
)

// verdictsOf is what a plan records for its statements above safe, in the shape a
// rating reports them.
func verdictsOf(plan atlasschema.PlanFile) []atlasschema.RatedStatement {
	var verdicts []atlasschema.RatedStatement
	for i, statement := range plan.Statements {
		if risk.Rank(statement.Severity) > risk.Rank(safety.Safe) {
			verdicts = append(verdicts, atlasschema.RatedStatement{
				Position: i + 1,
				Text:     strings.TrimSuffix(strings.Join(strings.Fields(statement.SQL), " "), ";"),
				Severity: statement.Severity,
				Reason:   statement.Reason,
			})
		}
	}
	return verdicts
}

func prepareApply(c *qt.C, conn *dbschema.DatabaseConnection, desired *schemamodel.Database) atlasschema.ApplyRuntimePlan {
	c.Helper()
	plan, err := atlasschema.PrepareApply(context.Background(), conn, atlasschema.ApplyRuntimeOptions{
		Desired: desired,
		Runtime: must.Must(builtin.New()),
		TxMode:  migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	return plan
}

// TestApplyRuntimePlanRating_ReadsWhatAPlanRecords holds a prompt's rating to the
// verdicts `schema plan` records for the same plan. The declared row is the case the
// statement's words cannot rate: a DELETE reads as safe to the analyzer, and its
// severity comes from the data stage.
func TestApplyRuntimePlanRating_ReadsWhatAPlanRecords(t *testing.T) {
	norway, sweden := regionRow("NO", "Norway", 1), regionRow("SE", "Sweden", 2)
	tests := []struct {
		name         string
		extra        string
		declared     *schemamodel.Database
		wantSeverity safety.Severity
		wantTotal    int
		wantRated    int
	}{
		{
			name:         "a declared row the desired schema no longer holds",
			declared:     regionsSchema(norway),
			wantSeverity: safety.Destructive,
			wantTotal:    1,
			wantRated:    1,
		},
		{
			name:         "a dropped table, with the row after it",
			extra:        "CREATE TABLE legacy (id INTEGER PRIMARY KEY)",
			declared:     regionsSchema(norway),
			wantSeverity: safety.Destructive,
			wantTotal:    2,
			wantRated:    2,
		},
		{
			name:         "a declared row added",
			declared:     regionsSchema(norway, sweden, regionRow("DK", "Denmark", 3)),
			wantSeverity: safety.Safe,
			wantTotal:    1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			conn := managedDataConnection(c, "rating.db")
			applyPlan(c, conn, regionsSchema(norway, sweden))
			_, err := conn.ExecContext(ctx, test.extra)
			c.Assert(err, qt.IsNil)
			recorded, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{
				Desired: test.declared,
				Runtime: must.Must(builtin.New()),
			})
			c.Assert(err, qt.IsNil)

			rating, err := prepareApply(c, conn, test.declared).Rating(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(rating.Severity, qt.Equals, test.wantSeverity)
			c.Assert(rating.Total, qt.Equals, test.wantTotal)
			c.Assert(rating.Statements, qt.HasLen, test.wantRated)
			c.Assert(rating.Statements, qt.DeepEquals, verdictsOf(recorded))
		})
	}
}

// TestApplyRuntimePlanRatingOfSQL rates the text a person approves. An edit keeps a
// verdict the words cannot say, and adds what its own words say.
func TestApplyRuntimePlanRatingOfSQL(t *testing.T) {
	const removeRow = `DELETE FROM "regions" WHERE "code" = 'SE'`
	tests := []struct {
		name         string
		edit         func(sqlText string) string
		wantSeverity safety.Severity
		wantTotal    int
		wantTexts    []string
	}{
		{
			name:         "the plan as prepared",
			edit:         func(sqlText string) string { return sqlText },
			wantSeverity: safety.Destructive,
			wantTotal:    1,
			wantTexts:    []string{removeRow},
		},
		{
			name:         "an edit that keeps the statement and adds a DROP",
			edit:         func(sqlText string) string { return sqlText + "DROP TABLE regions;\n" },
			wantSeverity: safety.Destructive,
			wantTotal:    2,
			wantTexts:    []string{removeRow, `DROP TABLE regions`},
		},
		{
			name:         "an edit that removes the statement",
			edit:         func(string) string { return "CREATE TABLE fresh (id INTEGER);\n" },
			wantSeverity: safety.Safe,
			wantTotal:    1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := managedDataConnection(c, "rating-edit.db")
			applyPlan(c, conn, regionsSchema(regionRow("NO", "Norway", 1), regionRow("SE", "Sweden", 2)))
			plan := prepareApply(c, conn, regionsSchema(regionRow("NO", "Norway", 1)))
			prepared, err := plan.SQL()
			c.Assert(err, qt.IsNil)
			c.Assert(prepared, qt.Contains, removeRow)

			rating, err := plan.RatingOfSQL(context.Background(), test.edit(prepared))

			c.Assert(err, qt.IsNil)
			c.Assert(rating.Severity, qt.Equals, test.wantSeverity)
			c.Assert(rating.Total, qt.Equals, test.wantTotal)
			c.Assert(ratedTexts(rating), qt.DeepEquals, test.wantTexts)
		})
	}
}

func ratedTexts(rating atlasschema.PlanRating) []string {
	var texts []string
	for _, statement := range rating.Statements {
		texts = append(texts, statement.Text)
	}
	return texts
}

// TestPlanFileRating rates a saved plan: what it recorded, raised by what its SQL
// says. A verdict in the file cannot lower one, since nothing proves the file is
// what `schema plan` wrote.
func TestPlanFileRating(t *testing.T) {
	const narrows = `ALTER TABLE "t" ALTER COLUMN "c" TYPE varchar(5)`
	long := "CREATE TABLE t (" + strings.Repeat("c int, ", 40) + "d int)"
	tests := []struct {
		name       string
		statements []atlasschema.PlanStatement
		want       atlasschema.PlanRating
	}{
		{
			name:       "a safe plan",
			statements: []atlasschema.PlanStatement{{SQL: `CREATE TABLE t (id int)`, Severity: safety.Safe}},
			want:       atlasschema.PlanRating{Severity: safety.Safe, Total: 1},
		},
		{
			name: "a verdict the words cannot say stays",
			statements: []atlasschema.PlanStatement{
				{SQL: narrows, Severity: safety.Destructive, Reason: "column type narrows"},
			},
			want: atlasschema.PlanRating{Severity: safety.Destructive, Total: 1, Statements: []atlasschema.RatedStatement{
				{Position: 1, Text: narrows, Severity: safety.Destructive, Reason: "column type narrows"},
			}},
		},
		{
			name: "a recorded verdict below what the words say rises",
			statements: []atlasschema.PlanStatement{
				{SQL: `DROP TABLE victim`, Severity: safety.Safe, Reason: "edited down"},
			},
			want: atlasschema.PlanRating{Severity: safety.Destructive, Total: 1, Statements: []atlasschema.RatedStatement{
				{Position: 1, Text: `DROP TABLE victim`, Severity: safety.Destructive, Reason: "DROP TABLE removes the table and all rows"},
			}},
		},
		{
			name: "a warning under a destructive statement",
			statements: []atlasschema.PlanStatement{
				{SQL: `CREATE TABLE fresh (id int)`, Severity: safety.Warning, Reason: "recorded"},
				{SQL: `DROP TABLE victim`, Severity: safety.Destructive, Reason: "recorded"},
			},
			want: atlasschema.PlanRating{Severity: safety.Destructive, Total: 2, Statements: []atlasschema.RatedStatement{
				{Position: 1, Text: `CREATE TABLE fresh (id int)`, Severity: safety.Warning, Reason: "recorded"},
				{Position: 2, Text: `DROP TABLE victim`, Severity: safety.Destructive, Reason: "recorded"},
			}},
		},
		{
			name: "a destructive statement ahead of a warning",
			statements: []atlasschema.PlanStatement{
				{SQL: `DROP TABLE victim`, Severity: safety.Destructive, Reason: "recorded"},
				{SQL: `CREATE TABLE fresh (id int)`, Severity: safety.Warning, Reason: "recorded"},
			},
			want: atlasschema.PlanRating{Severity: safety.Destructive, Total: 2, Statements: []atlasschema.RatedStatement{
				{Position: 1, Text: `DROP TABLE victim`, Severity: safety.Destructive, Reason: "recorded"},
				{Position: 2, Text: `CREATE TABLE fresh (id int)`, Severity: safety.Warning, Reason: "recorded"},
			}},
		},
		{
			name: "a warning alone",
			statements: []atlasschema.PlanStatement{
				{SQL: `CREATE TABLE fresh (id int)`, Severity: safety.Warning, Reason: "recorded"},
			},
			want: atlasschema.PlanRating{Severity: safety.Warning, Total: 1, Statements: []atlasschema.RatedStatement{
				{Position: 1, Text: `CREATE TABLE fresh (id int)`, Severity: safety.Warning, Reason: "recorded"},
			}},
		},
		{
			name: "a statement of comments only is not counted",
			statements: []atlasschema.PlanStatement{
				{SQL: `-- SKIP: nothing runs for this`, Severity: safety.Safe},
				{SQL: "-- the reason\nDROP TABLE victim", Severity: safety.Destructive, Reason: "recorded"},
			},
			want: atlasschema.PlanRating{Severity: safety.Destructive, Total: 1, Statements: []atlasschema.RatedStatement{
				{Position: 1, Text: `DROP TABLE victim`, Severity: safety.Destructive, Reason: "recorded"},
			}},
		},
		{
			name: "a native operation keeps its owner's verdict",
			statements: []atlasschema.PlanStatement{
				{Native: &atlasschema.PlanNative{Kind: schemaext.Kind("example.topic"), Subject: "events"}, Severity: safety.Destructive, Reason: "owner"},
			},
			want: atlasschema.PlanRating{Severity: safety.Destructive, Total: 1, Statements: []atlasschema.RatedStatement{
				{Position: 1, Text: "example.topic events", Severity: safety.Destructive, Reason: "owner"},
			}},
		},
		{
			name: "a long statement is shortened to one line",
			statements: []atlasschema.PlanStatement{
				{SQL: long, Severity: safety.Destructive, Reason: "recorded"},
			},
			want: atlasschema.PlanRating{Severity: safety.Destructive, Total: 1, Statements: []atlasschema.RatedStatement{
				{Position: 1, Text: long[:100] + "...", Severity: safety.Destructive, Reason: "recorded"},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasschema.PlanFile{Statements: test.statements}.Rating(context.Background(), must.Must(builtin.New()), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestWritePlanRating(t *testing.T) {
	tests := []struct {
		name   string
		rating atlasschema.PlanRating
		err    error
		want   string
	}{
		{
			name:   "a safe plan writes nothing",
			rating: atlasschema.PlanRating{Severity: safety.Safe, Total: 3},
		},
		{
			name: "each statement above safe, under the plan's rating",
			rating: atlasschema.PlanRating{Severity: safety.Destructive, Total: 3, Statements: []atlasschema.RatedStatement{
				{Position: 1, Text: "ALTER one", Severity: safety.Warning, Reason: "may fail"},
				{Position: 3, Text: "DROP two", Severity: safety.Destructive, Reason: "loses data"},
			}},
			want: "Plan rating: destructive\n" +
				"Statement 1 of 3, warning: may fail\n  ALTER one\n" +
				"Statement 3 of 3, destructive: loses data\n  DROP two\n",
		},
		{
			name: "a failed rating says so",
			err:  errors.New("the provider did not answer"),
			want: "warning: this plan has no rating: the provider did not answer\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			atlasschema.WritePlanRating(&out, func() (atlasschema.PlanRating, error) { return test.rating, test.err })

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}
