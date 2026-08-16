package sqliteforeignkeys_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/sqliteforeignkeys"
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
