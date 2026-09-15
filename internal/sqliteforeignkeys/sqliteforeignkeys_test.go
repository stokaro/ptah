package sqliteforeignkeys_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqliteforeignkeys"
)

func TestBracketsRecognizesAWrappedPlan(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		want       bool
	}{
		{
			name:       "the spelling this repository emits",
			statements: []string{sqliteforeignkeys.DisableStatement, `DROP TABLE "t"`, sqliteforeignkeys.EnableStatement},
			want:       true,
		},
		{
			name:       "the spelling the pinned community binary emits",
			statements: []string{"PRAGMA foreign_keys = off;", `DROP TABLE "t"`, "PRAGMA foreign_keys = on;"},
			want:       true,
		},
		{
			name:       "numeric values",
			statements: []string{"PRAGMA foreign_keys = 0", `DROP TABLE "t"`, "PRAGMA foreign_keys = 1"},
			want:       true,
		},
		{
			name:       "case and spacing are SQLite's, not ours",
			statements: []string{"  pragma   FOREIGN_KEYS=OFF ;", `DROP TABLE "t"`, "PRAGMA foreign_keys=ON"},
			want:       true,
		},
		{
			name: "the comment the planner writes above each pragma",
			statements: []string{
				"-- Disable foreign-key enforcement for the table rebuild below\n" + sqliteforeignkeys.DisableStatement,
				`DROP TABLE "t"`,
				"-- Restore foreign-key enforcement after the table rebuild\n" + sqliteforeignkeys.EnableStatement,
			},
			want: true,
		},
		{
			name:       "a commented-out pragma is not a pragma",
			statements: []string{"-- " + sqliteforeignkeys.DisableStatement, `DROP TABLE "t"`, sqliteforeignkeys.EnableStatement},
			want:       false,
		},
		{
			name:       "an empty plan",
			statements: nil,
			want:       false,
		},
		{
			name:       "a plan that only disables never restores",
			statements: []string{sqliteforeignkeys.DisableStatement, `DROP TABLE "t"`},
			want:       false,
		},
		{
			name:       "a plan that opens by enabling is not a rebuild",
			statements: []string{sqliteforeignkeys.EnableStatement, `DROP TABLE "t"`, sqliteforeignkeys.EnableStatement},
			want:       false,
		},
		{
			name:       "a pragma in the middle keeps its place",
			statements: []string{`DROP TABLE "t"`, sqliteforeignkeys.DisableStatement, `DROP TABLE "u"`},
			want:       false,
		},
		{
			name:       "a different pragma",
			statements: []string{"PRAGMA journal_mode = off", `DROP TABLE "t"`, "PRAGMA journal_mode = on"},
			want:       false,
		},
		{
			name:       "a pragma with no value is a query, not a setting",
			statements: []string{"PRAGMA foreign_keys", `DROP TABLE "t"`, "PRAGMA foreign_keys = on"},
			want:       false,
		},
		{
			name:       "a value SQLite would not accept",
			statements: []string{"PRAGMA foreign_keys = maybe", `DROP TABLE "t"`, "PRAGMA foreign_keys = on"},
			want:       false,
		},
		{
			name:       "an identifier that merely starts with the word",
			statements: []string{"PRAGMA foreign_keys_extra = off", `DROP TABLE "t"`, "PRAGMA foreign_keys = on"},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqliteforeignkeys.Brackets(tt.statements), qt.Equals, tt.want)
		})
	}
}

func TestAppendIndexPointsInsideTheBracket(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		want       int
	}{
		{
			name:       "a bracketed plan takes additions ahead of the enabling pragma",
			statements: []string{sqliteforeignkeys.DisableStatement, `DROP TABLE "t"`, sqliteforeignkeys.EnableStatement},
			want:       2,
		},
		{
			name: "a bracketed plan whose pragmas carry the planner's comments",
			statements: []string{
				"-- Disable foreign-key enforcement for the table rebuild below\n" + sqliteforeignkeys.DisableStatement,
				`DROP TABLE "t"`,
				"-- Restore foreign-key enforcement after the table rebuild\n" + sqliteforeignkeys.EnableStatement,
			},
			want: 2,
		},
		{
			name:       "an unbracketed plan takes additions at its end",
			statements: []string{`CREATE TABLE "t" ("id" INTEGER)`, `CREATE INDEX "i" ON "t" ("id")`},
			want:       2,
		},
		{
			name:       "a plan that only disables is not bracketed",
			statements: []string{sqliteforeignkeys.DisableStatement, `DROP TABLE "t"`},
			want:       2,
		},
		{
			name:       "an empty plan",
			statements: nil,
			want:       0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqliteforeignkeys.AppendIndex(tt.statements), qt.Equals, tt.want)
		})
	}
}

// TestAppendIndexKeepsAPlanBracketed is the property the index exists for: a
// statement added where it points leaves Brackets answering true.
func TestAppendIndexKeepsAPlanBracketed(t *testing.T) {
	c := qt.New(t)
	plan := []string{sqliteforeignkeys.DisableStatement, `DROP TABLE "t"`, sqliteforeignkeys.EnableStatement}

	extended := slices.Insert(slices.Clone(plan), sqliteforeignkeys.AppendIndex(plan), `INSERT INTO "c" ("id") VALUES (1)`)

	c.Assert(sqliteforeignkeys.Brackets(extended), qt.IsTrue)
	c.Assert(extended[1:3], qt.DeepEquals, []string{`DROP TABLE "t"`, `INSERT INTO "c" ("id") VALUES (1)`})
}

func TestBracketsSQLReadsAGeneratedMigrationFile(t *testing.T) {
	tests := []struct {
		name    string
		sqlText string
		want    bool
	}{
		{
			name: "a generated file, header and all",
			sqlText: "-- Migration generated from schema differences\n" +
				"-- Direction: DOWN\n\n" +
				"PRAGMA foreign_keys = off;\n" +
				"-- SQLite table rebuild\n" +
				"DROP TABLE \"users\";\n" +
				"PRAGMA foreign_keys = on;\n",
			want: true,
		},
		{
			name:    "an ordinary migration",
			sqlText: "-- Direction: UP\n\nALTER TABLE \"users\" ADD COLUMN \"name\" TEXT;\n",
			want:    false,
		},
		{
			name:    "nothing but comments",
			sqlText: "-- Direction: UP\n",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqliteforeignkeys.BracketsSQL(tt.sqlText), qt.Equals, tt.want)
		})
	}
}
